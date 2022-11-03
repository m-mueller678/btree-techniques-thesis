use crate::basic_node::BasicNode;
use crate::find_separator::{find_separator, KeyRef};
use crate::util::{common_prefix_len, get_key_from_slice, partial_restore, SmallBuff};
use crate::{BTreeNode, BTreeNodeTag, FatTruncatedKey, PrefixTruncatedKey, PAGE_SIZE};
use smallvec::{SmallVec, ToSmallVec};
use std::marker::PhantomData;
use std::mem::{align_of, size_of, transmute};
use std::ops::Range;
use std::{mem, ptr};
use crate::inner_node::{FenceData, InnerConversionSink, InnerConversionSource};

pub type U64HeadNode = HeadNode<u64>;
pub type U32HeadNode = HeadNode<u32>;

pub trait FullKeyHead: Ord + Sized + Copy + KeyRef<'static> {
    const TAG: BTreeNodeTag;
    const HINT_COUNT: usize;

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
    const HINT_COUNT: usize = 16;
    const TAG: BTreeNodeTag = BTreeNodeTag::U64HeadNode;

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
    const HINT_COUNT: usize = 16;
    const TAG: BTreeNodeTag = BTreeNodeTag::U32HeadNode;

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
    pub fn new(
        fences: FenceData,
        upper: *mut BTreeNode,
    ) -> Self {
        debug_assert_eq!(size_of::<Self>(), PAGE_SIZE);
        let mut this = Self::from_fences(fences);
        this.as_parts_mut().2[0] = upper;
        this
    }

    fn from_fences(f: FenceData) -> Self {
        let mut this = HeadNode {
            head: HeadNodeHead {
                tag: Head::TAG,
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
        this.set_fences(f);
        this
    }

    fn update_hint(&mut self, slot_id: usize) {
        let count = self.head.key_count as usize;
        let dist = count / (Head::HINT_COUNT + 1);
        let begin = if (count > Head::HINT_COUNT * 2 + 1)
            && (((count - 1) / (Head::HINT_COUNT + 1)) == dist)
            && ((slot_id / dist) > 1)
        {
            (slot_id / dist) - 1
        } else {
            0
        };
        let (_, keys, _, hint) = self.as_parts_mut();
        for i in begin..Head::HINT_COUNT {
            hint[i] = keys[dist * (i + 1)];
            debug_assert!(i == 0 || hint[i - 1] <= hint[i]);
        }
    }

    /// returns half open range
    fn search_hint(&self, head_needle: Head) -> (usize, usize) {
        debug_assert!(self.head.key_count > 0);
        let (head, _, _, hint) = self.as_parts();
        if head.key_count as usize > Head::HINT_COUNT * 2 {
            let dist = head.key_count as usize / (Head::HINT_COUNT + 1);
            let pos = (0..Head::HINT_COUNT)
                .find(|&hi| hint[hi] >= head_needle)
                .unwrap_or(Head::HINT_COUNT);
            let pos2 = (pos..Head::HINT_COUNT)
                .find(|&hi| hint[hi] != head_needle)
                .unwrap_or(Head::HINT_COUNT);
            (
                pos * dist,
                if pos2 < Head::HINT_COUNT {
                    (pos2 + 1) * dist
                } else {
                    head.key_count as usize
                },
            )
        } else {
            (0, head.key_count as usize)
        }
    }

    fn set_fences(
        &mut self,
        fences: FenceData,
    ) {
        debug_assert!(fences.lower_fence < fences.upper_fence || fences.upper_fence.0.is_empty() && self.head.prefix_len == 0);
        self.head.prefix_len = fences.prefix_len as u16;
        let upper_fence_offset = PAGE_SIZE - fences.upper_fence.0.len();
        let lower_fence_offset = upper_fence_offset - fences.lower_fence.0.len();
        unsafe {
            let bytes = self.as_bytes_mut();
            bytes[upper_fence_offset..].copy_from_slice(fences.upper_fence.0);
            bytes[lower_fence_offset..upper_fence_offset].copy_from_slice(fences.lower_fence.0);
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
        Self::HINT_OFFSET + Head::HINT_COUNT * size_of::<Head>()
    };

    const HINT_OFFSET: usize = {
        let key_align = align_of::<Head>();
        size_of::<HeadNodeHead>().next_multiple_of(key_align)
    };

    pub fn find_child_for_key(&self, key: &[u8]) -> usize {
        if self.head.key_count == 0 { return 0; }
        let needle_head = Head::make_needle_head(PrefixTruncatedKey(&key[self.head.prefix_len as usize..]));
        let (lower, upper) = self.search_hint(needle_head);
        match self.as_parts().1[lower..upper].binary_search(&needle_head) {
            Ok(i) | Err(i) => lower + i,
        }
    }

    pub fn request_space_for_child(&mut self, _key_length: usize) -> Result<usize, ()> {
        if self.head.key_count < self.head.key_capacity {
            Ok(self.head.prefix_len as usize)
        } else {
            Err(())
        }
    }

    fn as_parts_mut(&mut self) -> (&mut HeadNodeHead, &mut [Head], &mut [*mut BTreeNode], &mut [Head]) {
        unsafe {
            let head = &mut self.head as *mut HeadNodeHead;
            let hints =
                (self as *mut Self as *mut u8).offset(Self::HINT_OFFSET as isize) as *mut Head;
            let keys =
                (self as *mut Self as *mut u8).offset(Self::KEY_OFFSET as isize) as *mut Head;
            let children = (self as *mut Self as *mut u8).offset(self.head.child_offset as isize)
                as *mut *mut BTreeNode;
            let capacity = self.head.key_capacity as usize;
            (
                &mut *head,
                std::slice::from_raw_parts_mut(keys, capacity),
                std::slice::from_raw_parts_mut(children, capacity + 1),
                std::slice::from_raw_parts_mut(hints, Head::HINT_COUNT),
            )
        }
    }

    fn as_parts(&self) -> (&HeadNodeHead, &[Head], &[*mut BTreeNode], &[Head]) {
        unsafe {
            let head = &self.head as *const HeadNodeHead;
            let hints =
                (self as *const Self as *const u8).offset(Self::HINT_OFFSET as isize) as *const Head;
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
                std::slice::from_raw_parts(hints, Head::HINT_COUNT),
            )
        }
    }

    /// may change node type
    /// if Err is returned, node must be split
    pub unsafe fn insert_child(
        &mut self,
        index: usize,
        key: PrefixTruncatedKey,
        child: *mut BTreeNode,
    ) -> Result<(), ()> {
        debug_assert!(key <= self.fences().upper_fence || self.fences().upper_fence.0.is_empty() && self.fences().prefix_len == 0);
        debug_assert!(key > self.fences().lower_fence || self.fences().lower_fence.0.is_empty() && self.fences().prefix_len == 0);
        debug_assert!(self.head.key_count < self.head.key_capacity);
        if let Some(key) = Head::make_fence_head(key) {
            let (head, keys, children, _) = self.as_parts_mut();
            keys[..head.key_count as usize + 1]
                .copy_within(index..head.key_count as usize, index + 1);
            children[..head.key_count as usize + 2]
                .copy_within(index..head.key_count as usize + 1, index + 1);
            keys[index] = key;
            children[index] = child;
            head.key_count += 1;
            self.update_hint(index);
            Ok(())
        } else {
            let (head, keys, children, _) = self.as_parts();
            let mut tmp = BasicNode::new_inner(children[head.key_count as usize]);
            tmp.set_fences(self.fences());
            let prefix_len = head.prefix_len as usize;
            for i in 0..head.key_count as usize {
                Self::try_insert_to_basic(
                    prefix_len,
                    &mut tmp,
                    i,
                    PrefixTruncatedKey(&keys[i].restore()),
                    children[i],
                )?;
            }
            let self_ptr = self as *mut Self as *mut BasicNode;
            unsafe {
                ptr::write(self_ptr, tmp);
                Self::try_insert_to_basic(prefix_len, &mut *self_ptr, index, key, child)
            }
        }
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        assert_eq!(PAGE_SIZE, size_of::<Self>());
        unsafe { transmute(self as *const Self) }
    }

    unsafe fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        assert_eq!(PAGE_SIZE, size_of::<Self>());
        transmute(self as *mut Self)
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
        let node_left_raw = BTreeNode::alloc();
        let truncated_sep_key = truncated_sep_key.restore();
        let truncated_sep_key = PrefixTruncatedKey(&truncated_sep_key);
        let node_left;
        let mut node_right;
        unsafe {
            node_left = (&mut *node_left_raw).write_inner(Self::new(
                FenceData { upper_fence: truncated_sep_key, ..self.fences() },
                self.get_child(sep_slot as usize),
            ));
            node_right = Self::new(
                FenceData { lower_fence: truncated_sep_key, ..self.fences() },
                self.get_child(self.head.key_count as usize),
            );
        };
        let sep_buffer = partial_restore(
            0,
            &[self.prefix(key_in_node), truncated_sep_key.0],
            parent_prefix_len,
        );

        let parent_sep = PrefixTruncatedKey(&sep_buffer);
        if let Err(()) =
        parent.insert_child(index_in_parent, parent_sep, node_left_raw as *mut BTreeNode)
        {
            unsafe {
                BTreeNode::dealloc(node_left_raw as *mut BTreeNode);
                return Err(());
            }
        }
        self.copy_key_value_range(0..sep_slot, node_left, FatTruncatedKey::full(key_in_node));
        self.copy_key_value_range(
            sep_slot + 1..self.head.key_count as usize,
            &mut node_right,
            FatTruncatedKey::full(key_in_node),
        );
        node_left.update_hint(0);
        node_right.update_hint(0);
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
        let (src_head, src_keys, src_children, _) = self.as_parts();
        let (dst_head, dst_keys, dst_children, _) = dst.as_parts_mut();
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
        let (head, keys, children, _) = self.as_parts();
        for i in 0..head.key_count as usize {
            unsafe {
                eprintln!(
                    "{:3}|{:3?}|{:3?} -> {:?}",
                    i,
                    transmute::<&Head, &[u8; 8]>(&keys[i]),
                    keys[i].restore(),
                    children[i]
                )
            }
        }
        eprintln!("upper: {:?}", children[head.key_count as usize]);
        eprintln!("fences: {:?}", self.fences());
    }

    pub fn validate_tree(&self, lower: &[u8], upper: &[u8]) {
        debug_assert_eq!(
            self.head.prefix_len as usize,
            common_prefix_len(lower, upper)
        );
        debug_assert_eq!(self.fences().lower_fence.0, &lower[self.head.prefix_len as usize..]);
        debug_assert_eq!(self.fences().upper_fence.0, &upper[self.head.prefix_len as usize..]);
        let mut current_lower: SmallBuff = lower.into();
        let (head, keys, children, _) = self.as_parts();
        for i in 0..head.key_count as usize {
            let current_upper = partial_restore(0, &[self.prefix(lower), &keys[i].restore()], 0);
            unsafe { &mut *children[i] }.validate_tree(&current_lower, &current_upper);
            current_lower = current_upper;
        }
        unsafe { &mut *children[head.key_count as usize] }.validate_tree(&current_lower, upper);
    }

    unsafe fn try_insert_to_basic(
        prefix_len: usize,
        dst: &mut BasicNode,
        slot: usize,
        key: PrefixTruncatedKey,
        child: *mut BTreeNode,
    ) -> Result<(), ()> {
        let tmp_prefix_len = dst
            .request_space(dst.space_needed(key.len() + prefix_len, size_of::<*mut BTreeNode>()))?;
        debug_assert_eq!(tmp_prefix_len, prefix_len);
        dst.raw_insert(slot, key, &(child as usize).to_ne_bytes());
        Ok(())
    }

    pub fn try_from_basic_node(this: &mut BTreeNode, new_tag: BTreeNodeTag) -> Result<(), ()> {
        debug_assert!(this.tag() == BTreeNodeTag::BasicInner);
        unsafe {
            let src = &this.basic;
            let mut tmp = Self::new(
                src.fences(),
                ptr::null_mut(),
            );
            let (head, keys, children, _) = tmp.as_parts_mut();
            let src_slots = src.slots();
            for (i, s) in src_slots.iter().enumerate() {
                keys[i] = Head::make_fence_head(s.key(src.as_bytes())).ok_or(())?;
            }
            for i in 0..src_slots.len() {
                children[i] = src.get_child(i);
            }
            children[src_slots.len()] = src.upper();
            head.key_count = src_slots.len() as u16;
            tmp.update_hint(0);
            ptr::write(this as *mut BTreeNode as *mut Self, tmp);
            Ok(())
        }
    }

    pub fn is_underfull(&self) -> bool {
        self.head.key_count * 4 <= self.head.key_capacity
    }

    pub fn merge_children_check(&mut self, mut child_index: usize) -> Result<(), ()> {
        if child_index == self.key_count() {
            if child_index == 0 {
                // only one child
                return Err(());
            }
            child_index -= 1;
        }
        unsafe {
            let left = &mut *self.get_child(child_index);
            let right = self.get_child(child_index + 1);
            let sep_key = self.as_parts().1[child_index].restore();
            left.try_merge_right(
                right,
                FatTruncatedKey {
                    remainder: &sep_key,
                    prefix_len: self.head.prefix_len as usize,
                },
            )?;
            BTreeNode::dealloc(self.get_child(child_index));
            self.remove_slot(child_index);
            Ok(())
        }
    }

    pub fn remove_slot(&mut self, index: usize) {
        let (head, keys, children, _) = self.as_parts_mut();
        keys.copy_within(index + 1..head.key_count as usize, index);
        children.copy_within(index + 1..head.key_count as usize + 1, index);
        head.key_count -= 1;
        self.update_hint(0);
    }
}

impl<Head: FullKeyHead> InnerConversionSink for HeadNode<Head> {
    fn create(dst: &mut BTreeNode, src: &impl InnerConversionSource) -> Result<(), ()> {
        let fences = src.fences();
        dst.write_inner(Self::from_fences(fences));
        let len = src.key_count();
        todo!()
    }
}

impl<Head: FullKeyHead> InnerConversionSource for HeadNode<Head> {
    fn fences(&self) -> FenceData {
        FenceData {
            lower_fence: PrefixTruncatedKey(&self.as_bytes()[self.head.lower_fence_offset as usize..self.head.upper_fence_offset as usize]),
            upper_fence: PrefixTruncatedKey(&self.as_bytes()[self.head.upper_fence_offset as usize..]),
            prefix_len: self.head.prefix_len as usize,
        }
    }

    fn key_count(&self) -> usize {
        self.head.key_count as usize
    }

    fn get_child(&self, index: usize) -> *mut BTreeNode {
        debug_assert!(index < self.head.key_count as usize + 1);
        self.as_parts().2[index]
    }

    fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()> {
        debug_assert!(index < self.head.key_count as usize);
        //TODO avoidable copy
        let key = self.as_parts().1[index].restore();
        get_key_from_slice(PrefixTruncatedKey(&key), dst, strip_prefix)
    }
}