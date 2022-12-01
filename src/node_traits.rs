use crate::util::{common_prefix_len, get_key_from_slice, MergeFences, partial_restore, reinterpret, SplitFences};
use crate::{BTreeNode, FatTruncatedKey, PrefixTruncatedKey};

use std::ops::{Deref, Range};

use std::ptr;
use crate::branch_cache::BranchCacheAccessor;
use crate::btree_node::STRIP_PREFIX;


pub trait InnerNode: InnerConversionSource + Node {
    fn merge_children_check(&mut self, child_index: usize) -> Result<(), ()>;

    /// key must be truncated to length returned from request_space
    /// node takes ownership of child on
    /// space must be checked before with `request_space_for_child`
    unsafe fn insert_child(&mut self, index: usize, key: PrefixTruncatedKey, child: *mut BTreeNode) -> Result<(), ()>;

    /// on success returns prefix length of node
    /// insert should be called with a string truncated to that length
    fn request_space_for_child(&mut self, key_length: usize) -> Result<usize, ()>;

    fn find_child_index(&mut self, key: &[u8], branch_cache: &mut BranchCacheAccessor) -> usize;
}

pub trait SeparableInnerConversionSource: InnerConversionSource {
    type Separator<'a>: Deref<Target=[u8]> + 'a
        where
            Self: 'a;

    fn find_separator<'a>(&'a self) -> (usize, Self::Separator<'a>);
}

/// must have tag and pointers must be reinterpretable as btreenode
pub unsafe trait Node: 'static {
    // true if at 1/4 capacity or less
    fn is_underfull(&self) -> bool;
    fn print(&self);
    fn validate_tree(&self, lower: &[u8], upper: &[u8]);
    fn split_node(
        &mut self,
        parent: &mut dyn InnerNode,
        index_in_parent: usize,
        key_in_node: &[u8],
    ) -> Result<(), ()>;
}

pub trait LeafNode: Node {
    fn insert(&mut self, key: &[u8], payload: &[u8]) -> Result<(), ()>;
    fn lookup(&self, key: &[u8]) -> Option<&[u8]>;
    fn remove(&mut self, key: &[u8]) -> Option<()>;

    fn range_lookup(
        &mut self,
        lower_inclusive: Option<&[u8]>,
        upper_inclusive: Option<&[u8]>,
        callback: &mut dyn FnMut(&[u8]),
    );
}

pub trait InnerConversionSource {
    fn fences(&self) -> FenceData;
    fn key_count(&self) -> usize;
    fn get_child(&self, index: usize) -> *mut BTreeNode;

    /// key will be written to end of dst
    /// returns length of stripped key
    fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()>;
    fn get_key_length_sum(&self, range: Range<usize>) -> usize;
    fn get_key_length_max(&self, range: Range<usize>) -> usize;
}

/// lower and upper should have no common prefix when passed around.
/// call restrip before if neccesary.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FenceData<'a> {
    pub prefix_len: usize,
    pub lower_fence: FenceRef<'a>,
    pub upper_fence: FenceRef<'a>,
}

impl FenceData<'_> {
    pub fn validate(&self) {
        if cfg!(debug_assertions) {
            assert!(self.lower_fence.0 < self.upper_fence.0 || self.upper_fence.0.is_empty() && self.prefix_len == 0);
            if STRIP_PREFIX {
                assert_eq!(common_prefix_len(self.lower_fence.0, self.upper_fence.0), 0)
            } else {
                assert_eq!(common_prefix_len(self.lower_fence.0, self.upper_fence.0), self.prefix_len)
            }
        }
    }
}

/// either with or without prefix depending on configuration
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FenceRef<'a> (pub &'a [u8]);

impl<'a> FenceRef<'a> {
    pub fn from_full(full: &'a [u8], prefix_len: usize) -> Self {
        if STRIP_PREFIX {
            Self(&full[prefix_len..])
        } else {
            Self(full)
        }
    }
}

#[cfg(debug_assertions)]
#[no_mangle]
pub unsafe extern "C" fn node_print(node: *const BTreeNode) {
    (&*node).print()
}

impl FenceData<'_> {
    pub fn restrip(self) -> Self {
        if STRIP_PREFIX {
            let common = common_prefix_len(self.lower_fence.0, self.upper_fence.0);
            FenceData {
                prefix_len: self.prefix_len + common,
                lower_fence: FenceRef(&self.lower_fence.0[common..]),
                upper_fence: FenceRef(&self.upper_fence.0[common..]),
            }
        } else {
            FenceData {
                prefix_len: self.prefix_len + common_prefix_len(&self.lower_fence.0[self.prefix_len..], &&self.upper_fence.0[self.prefix_len..]),
                ..self
            }
        }
    }

    pub fn empty() -> Self {
        FenceData {
            prefix_len: 0,
            lower_fence: FenceRef(&[]),
            upper_fence: FenceRef(&[]),
        }
    }

    pub fn debug_assert_contains(&self, key: &[u8]) {
        if cfg!(debug_assertions) {
            if STRIP_PREFIX {
                assert!(self.lower_fence.0 < &key[self.prefix_len..]);
                assert!(&key[self.prefix_len..] <= self.upper_fence.0 || self.upper_fence.0.is_empty() && self.prefix_len == 0);
            } else {
                assert!(self.lower_fence.0 < key);
                assert!(key <= self.upper_fence.0 || self.upper_fence.0.is_empty() && self.prefix_len == 0);
            }
        }
    }
}

pub unsafe trait InnerConversionSink {
    /// on error, state of dst is unspecified
    /// on success, dst must be initialized
    fn create(dst: &mut BTreeNode, src: &(impl InnerConversionSource + ?Sized)) -> Result<(), ()>;
}

pub fn merge<Dst: InnerConversionSink, Left: InnerConversionSource + ?Sized, Right: InnerConversionSource + ?Sized>(
    dst: &mut BTreeNode,
    left: &Left,
    right: &Right,
    separator: FatTruncatedKey,
) -> Result<(), ()> {
    struct MergeView<'a, Left: InnerConversionSource + ?Sized, Right: InnerConversionSource + ?Sized> {
        left: &'a Left,
        left_count: usize,
        right_count: usize,
        left_fences: FenceData<'a>,
        right_fences: FenceData<'a>,
        new_prefix_len: usize,
        right: &'a Right,
        separator: FatTruncatedKey<'a>,
        fences: MergeFences<'a>,
    }

    impl<'a, Left: InnerConversionSource + ?Sized, Right: InnerConversionSource + ?Sized> InnerConversionSource for MergeView<'a, Left, Right> {
        fn fences(&self) -> FenceData {
            self.fences.fences()
        }

        fn key_count(&self) -> usize {
            self.left_count + self.right_count + 1
        }

        fn get_child(&self, index: usize) -> *mut BTreeNode {
            if index <= self.left_count {
                self.left.get_child(index)
            } else {
                self.right.get_child(index - (self.left_count + 1))
            }
        }

        fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()> {
            debug_assert!(strip_prefix == 0);
            let dst_len = dst.len();
            if index < self.left_count {
                let key_src_len = self.left.get_key(index, dst, 0)?;
                let restored_prefix = &self.separator.remainder[self.new_prefix_len
                    - self.separator.prefix_len
                    ..self.left_fences.prefix_len - self.separator.prefix_len];
                let p_len = get_key_from_slice(
                    PrefixTruncatedKey(restored_prefix),
                    &mut dst[..dst_len - key_src_len],
                    0,
                )?;
                Ok(p_len + key_src_len)
            } else if index == self.left_count {
                get_key_from_slice(
                    PrefixTruncatedKey(
                        &self.separator.remainder
                            [self.new_prefix_len - self.separator.prefix_len..],
                    ),
                    dst,
                    0,
                )
            } else {
                let key_src_len = self.right.get_key(index - (self.left_count + 1), dst, 0)?;
                let restored_prefix = &self.separator.remainder[self.new_prefix_len
                    - self.separator.prefix_len
                    ..self.right_fences.prefix_len - self.separator.prefix_len];
                let p_len = get_key_from_slice(
                    PrefixTruncatedKey(restored_prefix),
                    &mut dst[..dst_len - key_src_len],
                    0,
                )?;
                Ok(p_len + key_src_len)
            }
        }

        fn get_key_length_sum(&self, range: Range<usize>) -> usize {
            debug_assert_eq!(range, 0..self.key_count());
            [
                self.left.get_key_length_sum(0..self.left_count) + self.left_count * (self.left_fences.prefix_len - self.new_prefix_len),
                (self.separator.remainder.len() - (self.new_prefix_len - self.separator.prefix_len)),
                self.right.get_key_length_sum(0..self.right_count) + self.right_count * (self.right_fences.prefix_len - self.new_prefix_len),
            ].iter().sum()
        }

        fn get_key_length_max(&self, range: Range<usize>) -> usize {
            debug_assert_eq!(range, 0..self.key_count());
            [
                self.left.get_key_length_sum(0..self.left_count) + (self.left_fences.prefix_len - self.new_prefix_len),
                (self.separator.remainder.len() - (self.new_prefix_len - self.separator.prefix_len)),
                self.right.get_key_length_sum(0..self.right_count) + (self.right_fences.prefix_len - self.new_prefix_len),
            ].into_iter().max().unwrap()
        }
    }

    let left_fences = left.fences();
    let right_fences = right.fences();
    let new_prefix_len = left_fences.prefix_len.min(right_fences.prefix_len);

    let merge_src = MergeView {
        left,
        left_count: left.key_count(),
        right_count: right.key_count(),
        left_fences,
        right_fences,
        new_prefix_len,
        right,
        separator,
        fences: MergeFences::new(left_fences, separator, right_fences),
    };
    Dst::create(dst, &merge_src)
}

pub fn merge_to_right<Dst: InnerConversionSink>
(left: &BTreeNode, right: &mut BTreeNode, separator: FatTruncatedKey) -> Result<(), ()> {
    debug_assert!(left.is_underfull());
    debug_assert!(right.is_underfull());
    unsafe {
        let mut tmp = BTreeNode::new_uninit();
        merge::<Dst, dyn InnerNode, dyn InnerNode>(&mut tmp, left.to_inner(), right.to_inner(), separator)?;
        ptr::write(right, tmp);
    }
    Ok(())
}

pub fn split_at<
    'a,
    Src: InnerConversionSource,
    Left: InnerConversionSink,
    Right: InnerConversionSink,
>(
    src: &'a Src,
    left: &mut BTreeNode,
    right: &mut BTreeNode,
    split_index: usize,
    separator: PrefixTruncatedKey<'a>,
    prefix_src: &'a [u8],
    parent_prefix_len: usize,
) -> Result<SplitFences<'a>, ()> {
    struct SliceView<'a, S> {
        offset: usize,
        len: usize,
        src: &'a S,
        fence_data: FenceData<'a>,
        strip_prefix: usize,
    }

    impl<'a, S: InnerConversionSource> InnerConversionSource for SliceView<'a, S> {
        fn fences(&self) -> FenceData {
            self.fence_data
        }

        fn key_count(&self) -> usize {
            self.len
        }

        fn get_child(&self, index: usize) -> *mut BTreeNode {
            debug_assert!(index < self.len + 1);
            self.src.get_child(self.offset + index)
        }

        fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()> {
            debug_assert!(strip_prefix == 0);
            debug_assert!(index < self.len + 1);
            self.src
                .get_key(self.offset + index, dst, self.strip_prefix)
        }

        fn get_key_length_sum(&self, range: Range<usize>) -> usize {
            debug_assert_eq!(range, 0..self.key_count());
            self.src.get_key_length_sum(self.offset..self.offset + self.len) - self.strip_prefix * self.len
        }

        fn get_key_length_max(&self, range: Range<usize>) -> usize {
            debug_assert_eq!(range, 0..self.key_count());
            self.src.get_key_length_max(self.offset..self.offset + self.len) - self.strip_prefix
        }
    }

    let src_fences = src.fences();
    let mut split_fences = SplitFences::new(src_fences, separator, parent_prefix_len, prefix_src);
    let left_fences = split_fences.lower();
    Left::create(
        left,
        &SliceView {
            offset: 0,
            len: split_index,
            src,
            fence_data: left_fences,
            strip_prefix: left_fences.prefix_len - src_fences.prefix_len,
        },
    )
        .unwrap();
    let right_fences = split_fences.upper();
    Right::create(
        right,
        &SliceView {
            offset: split_index + 1,
            len: src.key_count() - (split_index + 1),
            src,
            fence_data: right_fences,
            strip_prefix: right_fences.prefix_len - src_fences.prefix_len,
        },
    ).unwrap();
    Ok(split_fences)
}

pub fn split_in_place<
    'a,
    Src: SeparableInnerConversionSource,
    Left: InnerConversionSink,
    Right: InnerConversionSink,
>(
    node: &'a mut BTreeNode,
    parent: &mut dyn InnerNode,
    index_in_parent: usize,
    key_in_node: &[u8],
) -> Result<(), ()> {
    unsafe {
        let mut right;
        {
            let src: &Src = reinterpret(node);
            let (split_index, separator) = src.find_separator();
            let separator = &*separator;
            let parent_prefix_len =
                parent.request_space_for_child(separator.len() + src.fences().prefix_len)?;
            let left = BTreeNode::alloc();
            right = BTreeNode::new_uninit();
            split_at::<Src, Left, Right>(
                src,
                &mut *left,
                &mut right,
                split_index,
                PrefixTruncatedKey(separator),
                key_in_node,
                parent_prefix_len,
            )?;
            let restored_separator = partial_restore(
                0,
                &[&key_in_node[..src.fences().prefix_len], separator],
                parent_prefix_len,
            );
            parent.insert_child(
                index_in_parent,
                PrefixTruncatedKey(&restored_separator),
                left,
            )?;
            (&mut *((parent as *mut (dyn InnerNode) as *mut BTreeNode))).adaption_state().set_adapted(false);
        }
        ptr::write(node, right);
        Ok(())
    }
}

pub union FallbackInnerConversionSink<A: InnerConversionSink, B: InnerConversionSink> {
    _a: std::mem::ManuallyDrop<A>,
    _b: std::mem::ManuallyDrop<B>,
}

unsafe impl<A: InnerConversionSink, B: InnerConversionSink> InnerConversionSink for FallbackInnerConversionSink<A, B> {
    fn create(dst: &mut BTreeNode, src: &(impl InnerConversionSource + ?Sized)) -> Result<(), ()> {
        match A::create(dst, src) {
            Ok(()) => Ok(()),
            Err(()) => B::create(dst, src),
        }
    }
}

pub struct InnerInsertSource<'a, T: InnerConversionSource> {
    src: &'a T,
    index: usize,
    key: PrefixTruncatedKey<'a>,
    child: *mut BTreeNode,
}

impl<'a, T: InnerConversionSource> InnerInsertSource<'a, T> {
    pub fn new(src: &'a T, index: usize, key: PrefixTruncatedKey<'a>, child: *mut BTreeNode) -> Self {
        InnerInsertSource { src, index, key, child }
    }
}

impl<'a, T: InnerConversionSource> InnerConversionSource for InnerInsertSource<'a, T> {
    fn fences(&self) -> FenceData {
        self.src.fences()
    }

    fn key_count(&self) -> usize {
        self.src.key_count() + 1
    }

    fn get_child(&self, index: usize) -> *mut BTreeNode {
        if index < self.index {
            self.src.get_child(index)
        } else if index == self.index {
            self.child
        } else {
            self.src.get_child(index - 1)
        }
    }

    fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()> {
        if index < self.index {
            self.src.get_key(index, dst, strip_prefix)
        } else if index == self.index {
            get_key_from_slice(self.key, dst, strip_prefix)
        } else {
            self.src.get_key(index - 1, dst, strip_prefix)
        }
    }

    fn get_key_length_sum(&self, _range: Range<usize>) -> usize {
        unimplemented!()
    }

    fn get_key_length_max(&self, _range: Range<usize>) -> usize {
        unimplemented!()
    }
}