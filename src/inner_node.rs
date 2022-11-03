use crate::basic_node::BasicNode;
use crate::head_node::{U32HeadNode, U64HeadNode};
use crate::util::{common_prefix_len, get_key_from_slice, partial_restore, SmallBuff};
use crate::{BTreeNode, BTreeNodeTag, FatTruncatedKey, PrefixTruncatedKey};
use once_cell::unsync::OnceCell;
use std::mem::MaybeUninit;

use std::ptr;
use std::ptr::DynMetadata;

pub trait InnerConversionSource {
    fn fences(&self) -> FenceData;
    fn key_count(&self) -> usize;
    fn get_child(&self, index: usize) -> *mut BTreeNode;

    // true if at 1/4 capacity or less
    fn is_underfull(&self) -> bool;
    /// key will be written to end of dst
    /// returns length of stripped key
    fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()>;
    #[cfg(debug_assertions)]
    fn print(&self);
}

/// lower and upper should have no common prefix when passed around.
/// call restrip before if neccesary.
#[derive(Debug, Clone, Copy)]
pub struct FenceData<'a> {
    pub prefix_len: usize,
    pub lower_fence: PrefixTruncatedKey<'a>,
    pub upper_fence: PrefixTruncatedKey<'a>,
}

#[cfg(debug_assertions)]
#[no_mangle]
pub unsafe extern "C" fn node_print(node: *const BTreeNode) {
    (&*node).to_inner_conversion_source().print()
}

impl FenceData<'_> {
    pub fn restrip(self) -> Self {
        let common = common_prefix_len(self.lower_fence.0, self.upper_fence.0);
        FenceData {
            prefix_len: self.prefix_len + common,
            lower_fence: PrefixTruncatedKey(&self.lower_fence.0[common..]),
            upper_fence: PrefixTruncatedKey(&self.upper_fence.0[common..]),
        }
    }
}

pub trait InnerConversionSink {
    /// source has same prefix_len as self
    /// on error, state of self is unspecified
    fn create(dst: &mut BTreeNode, src: &impl InnerConversionSource) -> Result<(), ()>;
}

const INNER_COUNT: usize = 3;

static mut INNER_VTABLES: [MaybeUninit<DynMetadata<dyn InnerConversionSource>>; INNER_COUNT] =
    [MaybeUninit::uninit(); INNER_COUNT];

/// must be called before BTreeNode methods are used
pub fn init_vtables() {
    fn make_inner_vtable<N: InnerConversionSource + 'static>(tag: BTreeNodeTag, index: usize) {
        assert_eq!(tag as usize - 128, index);
        let ptr: *mut N = ptr::null_mut();
        let vtable = ptr::metadata(ptr as *mut dyn InnerConversionSource);
        unsafe {
            INNER_VTABLES[index].write(vtable);
        }
    }
    make_inner_vtable::<BasicNode>(BTreeNodeTag::BasicInner, 0);
    make_inner_vtable::<U64HeadNode>(BTreeNodeTag::U64HeadNode, 1);
    make_inner_vtable::<U32HeadNode>(BTreeNodeTag::U32HeadNode, 2);
}

impl BTreeNode {
    pub fn to_inner_conversion_source(&self) -> &dyn InnerConversionSource {
        let tag = self.tag();
        let index = tag as usize - 128;
        unsafe {
            &*ptr::from_raw_parts(
                self as *const Self as *const (),
                INNER_VTABLES[index].assume_init(),
            )
        }
    }
}

pub fn merge_right<Dst: InnerConversionSink>(
    dst: &mut BTreeNode,
    left: &dyn InnerConversionSource,
    right: &dyn InnerConversionSource,
    separator: FatTruncatedKey,
) -> Result<(), ()> {
    debug_assert!(
        left.is_underfull() && right.is_underfull(),
        "check underfull first, merge_right is expensive"
    );

    struct MergeView<'a> {
        left: &'a dyn InnerConversionSource,
        left_count: usize,
        left_fences: FenceData<'a>,
        right_fences: FenceData<'a>,
        new_prefix_len: usize,
        right: &'a dyn InnerConversionSource,
        separator: FatTruncatedKey<'a>,
        fence_buffer: OnceCell<SmallBuff>,
    }

    impl InnerConversionSource for MergeView<'_> {
        fn fences(&self) -> FenceData {
            let left = self.left_fences;
            let right = self.right_fences;
            debug_assert!(left.prefix_len >= self.separator.prefix_len);
            debug_assert!(right.prefix_len >= self.separator.prefix_len);
            if left.prefix_len == right.prefix_len {
                FenceData {
                    lower_fence: left.lower_fence,
                    ..right
                }
            } else if left.prefix_len > right.prefix_len {
                let lower = self.fence_buffer.get_or_init(|| {
                    partial_restore(
                        self.separator.prefix_len,
                        &[
                            &self.separator.remainder
                                [..left.prefix_len - self.separator.prefix_len],
                            self.left_fences.lower_fence.0,
                        ],
                        right.prefix_len,
                    )
                });
                FenceData {
                    lower_fence: PrefixTruncatedKey(lower.as_slice()),
                    ..right
                }
            } else {
                let upper = self.fence_buffer.get_or_init(|| {
                    partial_restore(
                        self.separator.prefix_len,
                        &[
                            &self.separator.remainder
                                [..right.prefix_len - self.separator.prefix_len],
                            self.right_fences.upper_fence.0,
                        ],
                        left.prefix_len,
                    )
                });
                FenceData {
                    upper_fence: PrefixTruncatedKey(upper.as_slice()),
                    ..left
                }
            }
        }

        fn key_count(&self) -> usize {
            self.left_count + self.right.key_count() + 1
        }

        fn get_child(&self, index: usize) -> *mut BTreeNode {
            if index <= self.left_count {
                self.left.get_child(index)
            } else {
                self.right.get_child(index - self.left_count - 1)
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
                let key_src_len = self.right.get_key(index, dst, 0)?;
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

        #[cfg(debug_assertions)]
        fn print(&self) {
            unimplemented!()
        }

        fn is_underfull(&self) -> bool {
            unimplemented!()
        }
    }

    let left_fences = left.fences();
    let right_fences = right.fences();
    let new_prefix_len = left_fences.prefix_len.min(right_fences.prefix_len);

    let merge_src = MergeView {
        left,
        left_count: left.key_count(),
        left_fences,
        right_fences,
        new_prefix_len,
        right,
        fence_buffer: Default::default(),
        separator,
    };
    Dst::create(dst, &merge_src)
}
