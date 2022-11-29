use crate::btree_node::{AdaptionState, BTreeNode, BTreeNodeHead, PAGE_SIZE};
use crate::find_separator::find_separator;

use crate::node_traits::{FenceData, FenceRef, InnerConversionSink, InnerConversionSource, InnerNode, LeafNode, merge, Node, SeparableInnerConversionSource, split_in_place};
use crate::util::{get_key_from_slice, head, MergeFences, partial_restore, reinterpret_mut, short_slice, SmallBuff, SplitFences, trailing_bytes};
use crate::{FatTruncatedKey, PrefixTruncatedKey};
use std::mem::{size_of, transmute};

use std::{mem, ptr};
use std::ops::Range;
use crate::branch_cache::BranchCacheAccessor;
use crate::vtables::BTreeNodeTag;

#[derive(Clone, Copy)]
#[repr(C)]
#[repr(packed)]
pub struct BasicSlot {
    pub offset: u16,
    pub key_len: u16,
    pub val_len: u16,
    pub head: u32,
}

impl BasicSlot {
    pub fn key<'a>(&self, page: &'a [u8; PAGE_SIZE]) -> PrefixTruncatedKey<'a> {
        PrefixTruncatedKey(short_slice(page, self.offset, self.key_len))
    }

    pub fn value<'a>(&self, page: &'a [u8; PAGE_SIZE]) -> &'a [u8] {
        short_slice(page, self.offset + self.key_len, self.val_len)
    }

    pub fn head_len(&self) -> usize {
        self.key_len.min(4) as usize
    }
}

#[derive(Clone, Copy, Debug)]
struct FenceKeySlot {
    offset: u16,
    len: u16,
}

const HINT_COUNT: usize = 16;

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct BasicNodeHead {
    head: BTreeNodeHead,
    /// only used in inner nodes, points to last child
    upper: *mut BTreeNode,
    lower_fence: FenceKeySlot,
    upper_fence: FenceKeySlot,
    count: u16,
    space_used: u16,
    data_offset: u16,
    prefix_len: u16,
    hint: [u32; HINT_COUNT],
}

#[derive(Clone, Copy)]
#[repr(C)]
pub union BasicNodeData {
    bytes: [u8; PAGE_SIZE - size_of::<BasicNodeHead>()],
    slots: [BasicSlot; (PAGE_SIZE - size_of::<BasicNodeHead>()) / size_of::<BasicSlot>()],
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct BasicNode {
    head: BasicNodeHead,
    data: BasicNodeData,
}

impl BasicNode {
    fn new(leaf: bool) -> Self {
        BasicNode {
            head: BasicNodeHead {
                head: BTreeNodeHead {
                    tag: if leaf {
                        BTreeNodeTag::BasicLeaf
                    } else {
                        BTreeNodeTag::BasicInner
                    },
                    adaption_state: AdaptionState::new(),
                },
                upper: ptr::null_mut(),
                lower_fence: FenceKeySlot { offset: 0, len: 0 },
                upper_fence: FenceKeySlot { offset: 0, len: 0 },
                count: 0,
                space_used: 0,
                data_offset: PAGE_SIZE as u16,
                prefix_len: 0,
                hint: [0; HINT_COUNT],
            },
            data: BasicNodeData {
                bytes: unsafe { mem::zeroed() },
            },
        }
    }

    pub fn validate(&self) {
        self.fences().validate();
        if cfg!(debug_assertions) {
            for w in self.slots().windows(2) {
                assert!(w[0].key(self.as_bytes()).0 <= w[1].key(self.as_bytes()).0);
            }
            assert_eq!(
                self.head.space_used,
                self.slots()
                    .iter()
                    .map(|s| s.key_len + s.val_len)
                    .sum::<u16>()
                    + self.head.lower_fence.len
                    + self.head.upper_fence.len
            );
            self.assert_no_collide();
        }
    }

    pub fn upper(&self) -> *mut BTreeNode {
        self.head.upper
    }

    pub fn new_leaf() -> Self {
        Self::new(true)
    }

    pub fn new_inner(upper: *mut BTreeNode) -> Self {
        let mut r = Self::new(false);
        r.head.upper = upper;
        r
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        assert_eq!(PAGE_SIZE, size_of::<Self>());
        unsafe { transmute(self as *const Self) }
    }

    unsafe fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        assert_eq!(PAGE_SIZE, size_of::<Self>());
        transmute(self as *mut Self)
    }

    pub fn prefix<'a>(&self, src: &'a [u8]) -> &'a [u8] {
        &src[..self.head.prefix_len as usize]
    }

    pub fn slots(&self) -> &[BasicSlot] {
        unsafe { &self.data.slots[..self.head.count as usize] }
    }

    pub fn slots_mut(&mut self) -> &mut [BasicSlot] {
        unsafe { &mut self.data.slots[..self.head.count as usize] }
    }

    pub fn lower_bound(&self, key: PrefixTruncatedKey) -> (usize, bool) {
        if self.head.count == 0 {
            return (0, false);
        }
        let (head, _) = head(key);
        let (lower, upper) = self.search_hint(head);
        let search_result = self.slots()[lower..upper].binary_search_by(|s| {
            let slot_head = s.head;
            slot_head
                .cmp(&head)
                .then_with(|| s.key(self.as_bytes()).cmp(&key))
        });
        match search_result {
            Ok(index) | Err(index) => {
                let index = index + lower;
                debug_assert!(
                    index == self.slots().len() || key <= self.slots()[index].key(self.as_bytes())
                );
                debug_assert!(index == 0 || key > self.slots()[index - 1].key(self.as_bytes()));
                (index, search_result.is_ok())
            }
        }
    }

    /// returns half open range
    fn search_hint(&self, head: u32) -> (usize, usize) {
        debug_assert!(self.head.count > 0);
        if self.head.count as usize > HINT_COUNT * 2 {
            let dist = self.head.count as usize / (HINT_COUNT + 1);
            let pos = (0..HINT_COUNT)
                .find(|&hi| self.head.hint[hi] >= head)
                .unwrap_or(HINT_COUNT);
            let pos2 = (pos..HINT_COUNT)
                .find(|&hi| self.head.hint[hi] != head)
                .unwrap_or(HINT_COUNT);
            (
                pos * dist,
                if pos2 < HINT_COUNT {
                    (pos2 + 1) * dist
                } else {
                    self.head.count as usize
                },
            )
        } else {
            (0, self.head.count as usize)
        }
    }

    pub fn raw_insert(&mut self, slot_id: usize, key: PrefixTruncatedKey, payload: &[u8]) {
        debug_assert!(slot_id == 0 || self.slots()[slot_id - 1].key(self.as_bytes()) < key);
        debug_assert!(
            slot_id + 1 >= self.head.count as usize
                || self.slots()[slot_id + 1].key(self.as_bytes()) > key
        );
        self.head.count += 1;
        self.assert_no_collide();
        let count = self.head.count as usize;
        self.slots_mut()
            .copy_within(slot_id..count - 1, slot_id + 1);
        self.store_key_value(slot_id, key, payload);
        self.update_hint(slot_id);
        self.validate();
    }

    fn free_space(&self) -> usize {
        self.head.data_offset as usize
            - size_of::<BasicNodeHead>()
            - self.slots().len() * size_of::<BasicSlot>()
    }

    pub fn free_space_after_compaction(&self) -> usize {
        PAGE_SIZE
            - self.head.space_used as usize
            - size_of::<BasicNodeHead>()
            - self.slots().len() * size_of::<BasicSlot>()
    }

    pub fn request_space(&mut self, space: usize) -> Result<usize, ()> {
        if space <= self.free_space() {
            Ok(self.head.prefix_len as usize)
        } else if space <= self.free_space_after_compaction() {
            self.compactify();
            Ok(self.head.prefix_len as usize)
        } else {
            Err(())
        }
    }

    fn compactify(&mut self) {
        let should = self.free_space_after_compaction();
        let mut tmp = Self::new(self.head.head.tag.is_leaf());
        tmp.set_fences(self.fences());
        self.copy_key_value_range(self.slots(), &mut tmp, FatTruncatedKey::full(&[]));
        tmp.head.upper = self.head.upper;
        *self = tmp;
        self.make_hint();
        debug_assert!(self.free_space() == should);
    }

    fn copy_key_value_range(
        &self,
        src_slots: &[BasicSlot],
        dst: &mut Self,
        prefix_src: FatTruncatedKey,
    ) {
        for s in src_slots {
            self.copy_key_value(s, dst, prefix_src);
        }
    }

    fn push_slot(&mut self, s: BasicSlot) {
        self.head.count += 1;
        self.assert_no_collide();
        *self.slots_mut().last_mut().unwrap() = s;
    }

    fn copy_key_value(
        &self,
        src_slot: &BasicSlot,
        dst: &mut BasicNode,
        prefix_src: FatTruncatedKey,
    ) {
        let new_key_len = src_slot.key_len + self.head.prefix_len - dst.head.prefix_len;
        let previous_offset = dst.head.data_offset;
        let offset = if self.head.prefix_len <= dst.head.prefix_len {
            // string shrinks or stays same length
            dst.write_data(src_slot.value(self.as_bytes()));
            dst.write_data(&trailing_bytes(
                src_slot.key(self.as_bytes()).0,
                new_key_len as usize,
            ))
        } else {
            // string grows
            dst.write_data(src_slot.value(self.as_bytes()));
            dst.write_data(src_slot.key(self.as_bytes()).0);
            dst.write_data(trailing_bytes(
                &prefix_src.remainder[..self.head.prefix_len as usize - prefix_src.prefix_len],
                (self.head.prefix_len - dst.head.prefix_len) as usize,
            ))
        };
        debug_assert_eq!(offset + new_key_len + src_slot.val_len, previous_offset);
        let (head, _) = head(PrefixTruncatedKey(short_slice(
            dst.as_bytes(),
            offset,
            new_key_len,
        )));
        dst.push_slot(BasicSlot {
            offset,
            key_len: new_key_len,
            val_len: src_slot.val_len,
            head,
        });
    }

    pub fn set_fences(
        &mut self,
        fences @ FenceData {
            lower_fence: lower,
            upper_fence: upper,
            prefix_len,
        }: FenceData,
    ) {
        fences.validate();
        self.head.prefix_len = prefix_len as u16;
        self.head.lower_fence = FenceKeySlot {
            offset: self.write_data(lower.0),
            len: (lower.0.len()) as u16,
        };
        self.head.upper_fence = FenceKeySlot {
            offset: self.write_data(upper.0),
            len: (upper.0.len()) as u16,
        };
    }

    fn store_key_value(
        &mut self,
        slot_id: usize,
        prefix_truncated_key: PrefixTruncatedKey,
        payload: &[u8],
    ) {
        self.write_data(payload);
        let key_offset = self.write_data(prefix_truncated_key.0);
        self.slots_mut()[slot_id] = BasicSlot {
            offset: key_offset,
            key_len: prefix_truncated_key.0.len() as u16,
            val_len: payload.len() as u16,
            head: head(prefix_truncated_key).0,
        };
    }

    fn assert_no_collide(&self) {
        let data_start = self.head.data_offset as usize;
        let slot_end =
            size_of::<BasicNodeHead>() + self.head.count as usize * size_of::<BasicSlot>();
        debug_assert!(slot_end <= data_start);
    }

    fn write_data(&mut self, d: &[u8]) -> u16 {
        self.head.data_offset -= d.len() as u16;
        self.head.space_used += d.len() as u16;
        self.assert_no_collide();
        let offset = self.head.data_offset;
        unsafe {
            self.as_bytes_mut()[offset as usize..][..d.len()].copy_from_slice(d);
        }
        offset
    }

    fn update_hint(&mut self, slot_id: usize) {
        let count = self.head.count as usize;
        let dist = count / (HINT_COUNT + 1);
        let begin = if (count > HINT_COUNT * 2 + 1)
            && (((count - 1) / (HINT_COUNT + 1)) == dist)
            && ((slot_id / dist) > 1)
        {
            (slot_id / dist) - 1
        } else {
            0
        };
        for i in begin..HINT_COUNT {
            self.head.hint[i] = self.slots()[dist * (i + 1)].head;
            debug_assert!(i == 0 || self.head.hint[i - 1] <= self.head.hint[i]);
        }
    }

    fn make_hint(&mut self) {
        let count = self.head.count as usize;
        if count == 0 {
            return;
        }
        let dist = count / (HINT_COUNT + 1);
        for i in 0..HINT_COUNT {
            self.head.hint[i] = self.slots()[dist * (i + 1)].head;
            debug_assert!(i == 0 || self.head.hint[i - 1] <= self.head.hint[i]);
        }
    }

    pub fn space_needed(&self, key_length: usize, payload_length: usize) -> usize {
        key_length + payload_length + size_of::<BasicSlot>() - self.head.prefix_len as usize
    }

    pub fn merge_right(
        &self,
        is_inner: bool,
        right_any: &mut BTreeNode,
        separator: FatTruncatedKey,
    ) -> Result<(), ()> {
        if self.head.head.tag.is_leaf() {
            debug_assert!(right_any.tag() == self.head.head.tag);
        } else {
            unsafe {
                let mut dst = BTreeNode::new_uninit();
                let right = right_any.to_inner();
                if !right.is_underfull() {
                    return Err(());
                }
                merge::<Self, dyn InnerNode, dyn InnerNode>(&mut dst, self, right, separator)?;
                ptr::write(right_any, dst);
            }
            return Ok(());
        }
        let right = unsafe { &right_any.basic };
        let new_prefix_len = self.head.prefix_len.min(right.head.prefix_len);
        let left_grow_per_key = self.head.prefix_len - new_prefix_len;
        let left_grow = (left_grow_per_key) * self.head.count;
        let right_grow = (right.head.prefix_len - new_prefix_len) * right.head.count;
        let separator_space = self.space_needed(
            separator.remainder.len() + separator.prefix_len,
            size_of::<*mut BTreeNode>(),
        ) + left_grow_per_key as usize;
        let space_upper_bound = self.head.space_used as usize
            + right.head.space_used as usize
            + size_of::<BasicNodeHead>()
            + size_of::<BasicSlot>() * (self.head.count + right.head.count) as usize
            + left_grow as usize
            + right_grow as usize
            + if is_inner { separator_space } else { 0 };
        if space_upper_bound > PAGE_SIZE {
            return Err(());
        }
        let mut tmp = BasicNode::new(self.head.head.tag.is_leaf());
        tmp.head.upper = right.head.upper;
        let merge_fences = MergeFences::new(self.fences(), separator, right.fences());
        tmp.set_fences(merge_fences.fences());
        debug_assert_eq!(tmp.head.prefix_len, new_prefix_len);
        self.copy_key_value_range(self.slots(), &mut tmp, separator);
        right.copy_key_value_range(right.slots(), &mut tmp, separator);
        tmp.make_hint();
        right_any.basic = tmp;
        Ok(())
    }

    pub fn remove_slot(&mut self, index: usize) {
        self.head.space_used -= self.slots()[index].key_len + self.slots()[index].val_len;
        let back_slots = &mut self.slots_mut()[index..];
        back_slots.copy_within(1.., 0);
        self.head.count -= 1;
        self.make_hint();
        self.validate();
    }

    pub fn truncate<'a>(&self, key: &'a [u8]) -> PrefixTruncatedKey<'a> {
        PrefixTruncatedKey(&key[self.head.prefix_len as usize..])
    }
}

impl InnerConversionSource for BasicNode {
    fn fences(&self) -> FenceData {
        FenceData {
            lower_fence: FenceRef(
                &self.as_bytes()[self.head.lower_fence.offset as usize..]
                    [..self.head.lower_fence.len as usize],
            ),
            upper_fence: FenceRef(
                &self.as_bytes()[self.head.upper_fence.offset as usize..]
                    [..self.head.upper_fence.len as usize],
            ),
            prefix_len: self.head.prefix_len as usize,
        }
    }

    fn key_count(&self) -> usize {
        self.head.count as usize
    }

    fn get_child(&self, index: usize) -> *mut BTreeNode {
        debug_assert!(index <= self.head.count as usize);
        if index == self.head.count as usize {
            self.head.upper
        } else {
            unsafe {
                ptr::read_unaligned(
                    self.slots()[index].value(self.as_bytes()).as_ptr() as *const *mut BTreeNode
                )
            }
        }
    }

    fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()> {
        get_key_from_slice(self.slots()[index].key(self.as_bytes()), dst, strip_prefix)
    }

    fn get_key_length_sum(&self, range: Range<usize>) -> usize {
        self.slots()[range].iter().map(|s| s.key_len as usize).sum()
    }

    fn get_key_length_max(&self, range: Range<usize>) -> usize {
        self.slots()[range].iter().map(|s| s.key_len as usize).max().unwrap_or(0)
    }
}

unsafe impl Node for BasicNode {
    fn split_node(
        &mut self,
        parent: &mut dyn InnerNode,
        index_in_parent: usize,
        key_in_node: &[u8],
    ) -> Result<(), ()> {
        if self.head.head.tag.is_inner() {
            type Dst = crate::btree_node::DefaultInnerNodeConversionSink;
            return split_in_place::<BasicNode, Dst, Dst>(
                unsafe { reinterpret_mut(self) },
                parent,
                index_in_parent,
                key_in_node,
            );
        }

        // split
        let (sep_slot, truncated_sep_key) = self.find_separator();
        let full_sep_key_len = truncated_sep_key.0.len() + self.head.prefix_len as usize;
        let parent_prefix_len = parent.request_space_for_child(full_sep_key_len)?;
        let node_left_raw;
        let node_left = unsafe {
            node_left_raw = BTreeNode::alloc();
            (*node_left_raw).basic = Self::new(self.head.head.tag.is_leaf());
            &mut (*node_left_raw).basic
        };
        let mut node_right = Self::new(self.head.head.tag.is_leaf());

        let mut split_fences = SplitFences::new(self.fences(), truncated_sep_key, parent_prefix_len, self.prefix(key_in_node));
        node_left.set_fences(split_fences.lower());
        node_right.set_fences(split_fences.upper());
        unsafe {
            if let Err(()) = parent.insert_child(index_in_parent, split_fences.separator(), node_left_raw) {
                BTreeNode::dealloc(node_left_raw);
                return Err(());
            }
        }

        self.copy_key_value_range(
            &self.slots()[..=sep_slot],
            node_left,
            FatTruncatedKey::full(key_in_node),
        );
        self.copy_key_value_range(
            &self.slots()[sep_slot + 1..],
            &mut node_right,
            FatTruncatedKey::full(key_in_node),
        );
        node_left.make_hint();
        node_right.make_hint();
        *self = node_right;
        Ok(())
    }

    fn is_underfull(&self) -> bool {
        self.free_space_after_compaction() >= PAGE_SIZE * 3 / 4
    }

    fn print(&self) {
        eprintln!("{:?}", self.head);
        for (i, s) in self.slots().iter().enumerate() {
            eprintln!(
                "{:4}|{:3?}|{:3?}",
                i,
                s.head.to_be_bytes(),
                s.key(self.as_bytes())
            );
        }
    }

    fn validate_tree(&self, lower: &[u8], upper: &[u8]) {
        debug_assert_eq!(self.fences(), FenceData {
            prefix_len: 0,
            lower_fence: FenceRef(lower),
            upper_fence: FenceRef(upper),
        }.restrip());
        if self.head.head.tag.is_inner() {
            let mut current_lower: SmallBuff = lower.into();
            for (i, s) in self.slots().iter().enumerate() {
                let current_upper =
                    partial_restore(0, &[self.prefix(lower), s.key(self.as_bytes()).0], 0);
                unsafe { &mut *self.get_child(i) }.validate_tree(&current_lower, &current_upper);
                current_lower = current_upper;
            }
            unsafe { &mut *self.get_child(self.head.count as usize) }
                .validate_tree(&current_lower, upper);
        }
    }
}

unsafe impl InnerConversionSink for BasicNode {
    fn create(dst: &mut BTreeNode, src: &(impl InnerConversionSource + ?Sized)) -> Result<(), ()> {
        let key_count = src.key_count();
        let this = dst.write_inner(BasicNode::new_inner(src.get_child(key_count)));
        this.set_fences(src.fences());

        if this.free_space() < size_of::<BasicSlot>() * key_count {
            return Err(());
        };
        let old_count = this.head.count as usize;
        this.head.count += key_count as u16;
        let mut offset = this.head.data_offset as usize;
        let min_offset = offset - this.free_space();
        unsafe {
            for i in 0..key_count {
                let bytes = this.as_bytes_mut();
                let child_bytes = (src.get_child(i) as usize).to_ne_bytes();
                let val_len = get_key_from_slice(
                    PrefixTruncatedKey(child_bytes.as_slice()),
                    &mut bytes[min_offset..offset],
                    0,
                )?;
                debug_assert_eq!(val_len, 8);
                offset -= val_len;
                let key_len = src.get_key(i, &mut bytes[min_offset..offset], 0)?;
                offset -= key_len;
                let head = head(PrefixTruncatedKey(&bytes[offset..][..key_len])).0;
                this.slots_mut()[old_count + i] = BasicSlot {
                    offset: offset as u16,
                    key_len: key_len as u16,
                    val_len: val_len as u16,
                    head,
                }
            }
        }
        this.head.space_used += this.head.data_offset - offset as u16;
        this.head.data_offset = offset as u16;
        this.make_hint();
        this.validate();
        Ok(())
    }
}

impl SeparableInnerConversionSource for BasicNode {
    type Separator<'a> = PrefixTruncatedKey<'a>;

    fn find_separator<'a>(&'a self) -> (usize, Self::Separator<'a>) {
        find_separator(
            self.head.count as usize,
            self.head.head.tag.is_leaf(),
            |i: usize| self.slots()[i].key(self.as_bytes()),
        )
    }
}

impl InnerNode for BasicNode {
    fn merge_children_check(&mut self, mut child_index: usize) -> Result<(), ()> {
        unsafe {
            let left;
            let right;
            if child_index == self.key_count() {
                if child_index == 0 {
                    // only one child
                    return Err(());
                }
                child_index -= 1;
                left = &mut *self.get_child(child_index);
                right = &mut *self.get_child(child_index + 1);
                if !left.is_underfull() {
                    return Err(());
                }
            } else {
                left = &mut *self.get_child(child_index);
                right = &mut *self.get_child(child_index + 1);
                if !right.is_underfull() {
                    return Err(());
                }
            }
            left.try_merge_right(
                right,
                FatTruncatedKey {
                    remainder: self.slots()[child_index].key(self.as_bytes()).0,
                    prefix_len: self.head.prefix_len as usize,
                },
            )?;
            BTreeNode::dealloc(self.get_child(child_index));
            self.remove_slot(child_index);
            self.validate();
            Ok(())
        }
    }

    unsafe fn insert_child(&mut self, index: usize, key: PrefixTruncatedKey, child: *mut BTreeNode) -> Result<(), ()> {
        self.raw_insert(index, key, &(child as usize).to_ne_bytes());
        Ok(())
    }

    fn request_space_for_child(&mut self, key_length: usize) -> Result<usize, ()> {
        self.request_space(self.space_needed(key_length, size_of::<*mut BTreeNode>())
        )
    }

    fn find_child_index(&self, key: &[u8], bc: &mut BranchCacheAccessor) -> usize {
        let truncated = self.truncate(key);
        let index = bc.predict().filter(|&i| {
            i <= self.slots().len()
                && (i == 0 || self.slots()[i - 1].key(self.as_bytes()) < truncated)
                && (i >= self.slots().len() || truncated <= self.slots()[i].key(self.as_bytes()))
        })
            .unwrap_or_else(|| self.lower_bound(truncated).0);
        bc.store(index);
        index
    }
}

impl LeafNode for BasicNode {
    fn insert(&mut self, key: &[u8], payload: &[u8]) -> Result<(), ()> {
        if cfg!(feature="strip-prefix_false") {
            assert!(key <= self.fences().upper_fence.0 || self.fences().upper_fence.0.is_empty());
            assert!(key > self.fences().lower_fence.0 || self.fences().lower_fence.0.is_empty());
        }

        self.request_space(self.space_needed(key.len(), payload.len()))?;
        let key = self.truncate(key);
        let (slot_id, found) = self.lower_bound(key);
        if found {
            let s = &self.slots()[slot_id];
            self.head.space_used -= s.key_len + s.val_len;
            self.store_key_value(slot_id, key, payload);
        } else {
            self.raw_insert(slot_id, key, payload);
        }
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Option<&[u8]> {
        let (index, found) = self.lower_bound(self.truncate(key));
        if found {
            Some(self.slots()[index].value(self.as_bytes()))
        } else {
            None
        }
    }

    fn remove(&mut self, key: &[u8]) -> Option<()> {
        let (slot_id, found) = self.lower_bound(self.truncate(key));
        if !found {
            return None;
        }
        self.remove_slot(slot_id);
        Some(())
    }

    fn range_lookup(&self, lower_inclusive: Option<&[u8]>, upper_inclusive: Option<&[u8]>, callback: &mut dyn FnMut(&[u8])) {
        let start_index = lower_inclusive.map(|k| self.lower_bound(self.truncate(k)).0).unwrap_or(0);
        let end_index_exclusive = upper_inclusive.map(|k| {
            let (index, found) = self.lower_bound(self.truncate(k));
            if found {
                index + 1
            } else {
                index
            }
        }).unwrap_or(self.key_count());
        for i in start_index..end_index_exclusive {
            let s = self.slots()[i];
            callback(s.value(self.as_bytes()));
        }
    }
}