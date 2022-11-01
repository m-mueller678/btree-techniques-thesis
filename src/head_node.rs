use crate::find_separator::{find_separator, KeyRef};
use crate::util::{common_prefix_len, partial_restore, SmallBuff};
use crate::{BTreeNode, BTreeNodeTag, FatTruncatedKey, PrefixTruncatedKey, PAGE_SIZE};
use smallvec::{SmallVec, ToSmallVec};
use std::marker::PhantomData;
use std::mem::{align_of, size_of, transmute};
use std::ops::Range;
use std::{mem, ptr};

pub type U64HeadNode = HeadNode<u64>;
pub type U32HeadNode = HeadNode<u32>;

pub trait FullKeyHead: Ord + Sized + Copy + KeyRef<'static> {
    fn make_fence_head(key: PrefixTruncatedKey) -> Option<Self>;
    fn make_needle_head(key: PrefixTruncatedKey) -> Self;
    //TODO if last character is ascii, store in last byte, left shift by one
    // try various key extracion schemes
    // generate sample set of fence keys by inserting random subsets of datasets
    // and extracting fence keys
    // must also be able to handle keys in between fences
    fn restore(self) -> SmallVec<[u8; 16]>;
    fn strip_prefix(self, prefix_len: usize) -> Self {
        let mut v = self.restore();
        v.drain(..prefix_len);
        Self::make_fence_head(PrefixTruncatedKey(&v)).unwrap()
    }
}

impl FullKeyHead for u64 {
    fn make_fence_head(key: PrefixTruncatedKey) -> Option<Self> {
        if key.0.len() < 8 {
            let mut bytes = [0; 8];
            bytes[..key.0.len()].copy_from_slice(key.0);
            bytes[7] = key.0.len() as u8;
            Some(u64::from_be_bytes(bytes))
        } else {
            None
        }
    }

    fn make_needle_head(key: PrefixTruncatedKey) -> Self {
        let mut bytes = [0; 8];
        if key.0.len() < 8 {
            bytes[..key.0.len()].copy_from_slice(key.0);
            bytes[7] = key.0.len() as u8;
        } else {
            bytes[..7].copy_from_slice(&key.0[..7]);
            bytes[7] = 8;
        }
        u64::from_be_bytes(bytes)
    }

    fn restore(self) -> SmallVec<[u8; 16]> {
        let mut v = self.to_be_bytes().to_smallvec();
        let len = v[7] as usize;
        debug_assert!(len < 8, "this was created from a needle key");
        v.truncate(len);
        v
    }
}


impl KeyRef<'static> for u64 {
    fn common_prefix_len(self, b: Self) -> usize {
        common_prefix_len(&self.restore(), &b.restore())
    }

    fn len(self) -> usize {
        self.restore().len()
    }

    fn truncate(self, new_len: usize) -> Self {
        let mut v = self.restore();
        v.truncate(new_len);
        Self::make_fence_head(PrefixTruncatedKey(&v)).unwrap()
    }
}


impl FullKeyHead for u32 {
    fn make_fence_head(key: PrefixTruncatedKey) -> Option<Self> {
        if key.0.len() < 4 {
            let mut bytes = [0; 4];
            bytes[..key.0.len()].copy_from_slice(key.0);
            bytes[3] = key.0.len() as u8;
            Some(u32::from_be_bytes(bytes))
        } else {
            None
        }
    }

    fn make_needle_head(key: PrefixTruncatedKey) -> Self {
        let mut bytes = [0; 4];
        if key.0.len() < 4 {
            bytes[..key.0.len()].copy_from_slice(key.0);
            bytes[3] = key.0.len() as u8;
        } else {
            bytes[..3].copy_from_slice(&key.0[..3]);
            bytes[3] = 4;
        }
        u32::from_be_bytes(bytes)
    }

    fn restore(self) -> SmallVec<[u8; 16]> {
        let mut v = self.to_be_bytes().to_smallvec();
        let len = v[3] as usize;
        debug_assert!(len < 4, "this was created from a needle key");
        v.truncate(len);
        v
    }
}


impl KeyRef<'static> for u32 {
    fn common_prefix_len(self, b: Self) -> usize {
        common_prefix_len(&self.restore(), &b.restore())
    }

    fn len(self) -> usize {
        self.restore().len()
    }

    fn truncate(self, new_len: usize) -> Self {
        let mut v = self.restore();
        v.truncate(new_len);
        Self::make_fence_head(PrefixTruncatedKey(&v)).unwrap()
    }
}

#[repr(C, align(8))]
pub struct HeadNode<Head> {
    head: HeadNodeHead,
    _p: PhantomData<Head>,
    data: [u8; PAGE_SIZE - size_of::<HeadNodeHead>()],
}

#[repr(C)]
#[derive(Debug)]
pub struct HeadNodeHead {
    tag: BTreeNodeTag,
    key_count: u16,
    key_capacity: u16,
    child_offset: u16,
    lower_fence_offset: u16,
    upper_fence_offset: u16,
    prefix_len: u16,
}

impl<Head: FullKeyHead> HeadNode<Head> {
    pub unsafe fn new(
        tag: BTreeNodeTag,
        lower_fence: PrefixTruncatedKey,
        upper_fence: PrefixTruncatedKey,
        extra_prefix: u16,
        upper: *mut BTreeNode,
    ) -> Self {
        let mut this = HeadNode {
            head: HeadNodeHead {
                tag,
                key_count: 0,
                key_capacity: 0,
                child_offset: 0,
                lower_fence_offset: 0,
                upper_fence_offset: 0,
                prefix_len: 0,
            },
            _p: PhantomData,
            data: unsafe { mem::zeroed() },
        };
        this.set_fences(lower_fence, upper_fence, extra_prefix);
        this.as_parts_mut().2[0] = upper;
        this
    }

    fn set_fences(
        &mut self,
        mut lower: PrefixTruncatedKey,
        mut upper: PrefixTruncatedKey,
        extra_prefix: u16,
    ) {
        debug_assert!(lower < upper || upper.0.is_empty() && self.head.prefix_len == 0);
        let new_prefix = common_prefix_len(lower.0, upper.0);
        self.head.prefix_len = new_prefix as u16 + extra_prefix;
        lower = PrefixTruncatedKey(&lower.0[new_prefix..]);
        upper = PrefixTruncatedKey(&upper.0[new_prefix..]);
        let upper_fence_offset = PAGE_SIZE - upper.0.len();
        let lower_fence_offset = upper_fence_offset - lower.0.len();
        unsafe {
            let bytes = self.as_bytes_mut();
            bytes[upper_fence_offset..][..upper.0.len()].copy_from_slice(upper.0);
            bytes[lower_fence_offset..][..lower.0.len()].copy_from_slice(lower.0);
        }
        self.head.upper_fence_offset = upper_fence_offset as u16;
        self.head.lower_fence_offset = lower_fence_offset as u16;

        let child_align = align_of::<*mut BTreeNode>();

        let child_end = lower_fence_offset - lower_fence_offset % child_align;
        let available_size = child_end - Self::KEY_OFFSET;
        let key_capacity = (available_size - size_of::<*mut BTreeNode>())
            / (size_of::<Head>() + size_of::<*mut BTreeNode>());
        let child_offset = child_end - size_of::<*mut BTreeNode>() * (key_capacity + 1);

        debug_assert!(child_offset % child_align == 0);
        debug_assert!(self as *const Self as usize % child_align == 0);
        debug_assert!(Self::KEY_OFFSET + key_capacity * size_of::<Head>() <= child_offset);
        debug_assert!(
            child_offset + (key_capacity + 1) * size_of::<*mut BTreeNode>() <= lower_fence_offset
        );

        self.head.key_capacity = key_capacity as u16;
        self.head.child_offset = child_offset as u16;
    }

    const KEY_OFFSET: usize = {
        let key_align = align_of::<Head>();
        size_of::<HeadNodeHead>().next_multiple_of(key_align)
    };

    pub fn find_child_for_key(&self, key: &[u8]) -> usize {
        match self.as_parts().1[..self.head.key_count as usize]
            .binary_search(&Head::make_needle_head(PrefixTruncatedKey(&key[self.head.prefix_len as usize..])))
        {
            Ok(i) | Err(i) => i,
        }
    }

    pub fn get_child(&self, index: usize) -> *mut BTreeNode {
        debug_assert!(index < self.head.key_count as usize + 1);
        self.as_parts().2[index]
    }

    pub fn request_space_for_child(&mut self, _key_length: usize) -> Result<usize, ()> {
        if self.head.key_count < self.head.key_capacity {
            Ok(self.head.prefix_len as usize)
        } else {
            Err(())
        }
    }

    fn as_parts_mut(&mut self) -> (&mut HeadNodeHead, &mut [Head], &mut [*mut BTreeNode]) {
        unsafe {
            let head = &mut self.head as *mut HeadNodeHead;
            let keys =
                (self as *mut Self as *mut u8).offset(Self::KEY_OFFSET as isize) as *mut Head;
            let children = (self as *mut Self as *mut u8).offset(self.head.child_offset as isize)
                as *mut *mut BTreeNode;
            let capacity = self.head.key_capacity as usize;
            (
                &mut *head,
                std::slice::from_raw_parts_mut(keys, capacity),
                std::slice::from_raw_parts_mut(children, capacity + 1),
            )
        }
    }

    fn as_parts(&self) -> (&HeadNodeHead, &[Head], &[*mut BTreeNode]) {
        unsafe {
            let head = &self.head as *const HeadNodeHead;
            let keys =
                (self as *const Self as *const u8).offset(Self::KEY_OFFSET as isize) as *mut Head;
            let children = (self as *const Self as *const u8)
                .offset(self.head.child_offset as isize)
                as *const *mut BTreeNode;
            let capacity = self.head.key_capacity as usize;
            (
                &*head,
                std::slice::from_raw_parts(keys, capacity),
                std::slice::from_raw_parts(children, capacity + 1),
            )
        }
    }

    pub fn insert_child(&mut self, index: usize, key: PrefixTruncatedKey, child: *mut BTreeNode) {
        debug_assert!(self.head.key_count < self.head.key_capacity);
        let (head, keys, children) = self.as_parts_mut();
        keys[..head.key_count as usize + 1].copy_within(index..head.key_count as usize, index + 1);
        children[..head.key_count as usize + 2]
            .copy_within(index..head.key_count as usize + 1, index + 1);
        //TODO handle this
        keys[index] = Head::make_fence_head(key).unwrap();
        children[index] = child;
        head.key_count += 1;
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        assert_eq!(PAGE_SIZE, size_of::<Self>());
        unsafe { transmute(self as *const Self) }
    }

    unsafe fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        assert_eq!(PAGE_SIZE, size_of::<Self>());
        transmute(self as *mut Self)
    }

    pub fn fence(&self, upper: bool) -> PrefixTruncatedKey {
        PrefixTruncatedKey(if upper {
            &self.as_bytes()[self.head.upper_fence_offset as usize..]
        } else {
            &self.as_bytes()
                [self.head.lower_fence_offset as usize..self.head.upper_fence_offset as usize]
        })
    }

    pub fn split_node(
        &mut self,
        parent: &mut BTreeNode,
        index_in_parent: usize,
        key_in_node: &[u8],
    ) -> Result<(), ()> {
        // split
        let (sep_slot, truncated_sep_key) =
            find_separator(self.head.key_count as usize, false, |i| {
                self.as_parts().1[i]
            });
        let full_sep_key_len = truncated_sep_key.len() + self.head.prefix_len as usize;
        let parent_prefix_len = parent.request_space_for_child(full_sep_key_len)?;
        let node_left_raw = BTreeNode::alloc() as *mut Self;
        let truncated_sep_key = truncated_sep_key.restore();
        let truncated_sep_key = PrefixTruncatedKey(&truncated_sep_key);
        let node_left;
        let mut node_right;
        unsafe {
            ptr::write(
                node_left_raw,
                Self::new(
                    self.head.tag,
                    self.fence(false),
                    truncated_sep_key,
                    self.head.prefix_len,
                    self.get_child(sep_slot as usize),
                ),
            );
            node_left = &mut *node_left_raw;
            node_right = Self::new(
                self.head.tag,
                truncated_sep_key,
                self.fence(true),
                self.head.prefix_len,
                self.get_child(self.head.key_count as usize),
            );
        };
        let sep_buffer = partial_restore(
            0,
            &[self.prefix(key_in_node), truncated_sep_key.0],
            parent_prefix_len,
        );

        let parent_sep = PrefixTruncatedKey(&sep_buffer);
        parent.insert_child(index_in_parent, parent_sep, node_left_raw as *mut BTreeNode);
        self.copy_key_value_range(0..sep_slot, node_left, FatTruncatedKey::full(key_in_node));
        self.copy_key_value_range(
            sep_slot + 1..self.head.key_count as usize,
            &mut node_right,
            FatTruncatedKey::full(key_in_node),
        );
        *self = node_right;
        Ok(())
    }

    fn copy_key_value_range(
        &self,
        src_range: Range<usize>,
        dst: &mut Self,
        _prefix_src: FatTruncatedKey,
    ) {
        // TODO handle reduced prefix
        assert!(dst.head.prefix_len >= self.head.prefix_len);
        let (src_head, src_keys, src_children) = self.as_parts();
        let (dst_head, dst_keys, dst_children) = dst.as_parts_mut();
        let dst_upper = dst_children[dst_head.key_count as usize];
        let prefix_growth = (dst_head.prefix_len - src_head.prefix_len) as usize;
        for i in src_range.clone() {
            dst_keys[dst_head.key_count as usize + i - src_range.start] =
                src_keys[i].strip_prefix(prefix_growth);
        }
        for i in src_range.clone() {
            dst_children[dst_head.key_count as usize + i - src_range.start] = src_children[i];
        }
        dst_head.key_count += src_range.len() as u16;
        debug_assert!(dst_head.key_count < dst_head.key_capacity);
        dst_children[dst_head.key_count as usize] = dst_upper;
    }

    pub fn prefix<'a>(&self, src: &'a [u8]) -> &'a [u8] {
        &src[..self.head.prefix_len as usize]
    }

    fn print(&self) {
        eprintln!("{:?}", self.head);
        let (head, keys, children) = self.as_parts();
        for i in 0..head.key_count as usize {
            unsafe {
                eprintln!("{:3}|{:3?}|{:3?} -> {:?}", i, transmute::<&Head, &[u8; 8]>(&keys[i]), keys[i].restore(), children[i])
            }
        }
        eprintln!("upper: {:?}", children[head.key_count as usize]);
        eprintln!("fences: {:?}{:?}", self.fence(false), self.fence(true));
    }

    pub fn validate_tree(&self, lower: &[u8], upper: &[u8]) {
        debug_assert_eq!(
            self.head.prefix_len as usize,
            common_prefix_len(lower, upper)
        );
        debug_assert_eq!(
            self.fence(false).0,
            &lower[self.head.prefix_len as usize..]
        );
        debug_assert_eq!(
            self.fence(true).0,
            &upper[self.head.prefix_len as usize..]
        );
        let mut current_lower: SmallBuff = lower.into();
        let (head, keys, children) = self.as_parts();
        for i in 0..head.key_count as usize {
            let current_upper = partial_restore(0, &[self.prefix(lower), &keys[i].restore()], 0);
            unsafe { &mut *children[i] }.validate_tree(&current_lower, &current_upper);
            current_lower = current_upper;
        }
        unsafe { &mut *children[head.key_count as usize] }.validate_tree(&current_lower, upper);
    }
}
