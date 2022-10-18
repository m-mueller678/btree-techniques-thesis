use crate::btree_node::{BTreeNode, BTreeNodeTag, PAGE_SIZE};
use crate::util::{common_prefix_len, head, short_slice};
use crate::{HeadTruncatedKey, PrefixTruncatedKey};
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::io::Write;
use std::mem::{size_of, transmute};
use std::{mem, ptr};

#[derive(Clone, Copy)]
#[repr(C)]
#[repr(packed)]
pub struct HeadStrippedSlot {
    pub offset: u16,
    /// includes head, excludes prefix
    pub key_len: u16,
    pub val_len: u16,
    pub head: u32,
}

type SmallBuff = SmallVec<[u8; 32]>;

struct Separator {
    prefix_truncated_key: SmallBuff,
    /// last low slot for leaves, pull up slot for inner
    slot_id: usize,
}

impl HeadStrippedSlot {
    pub fn key<'a>(&self, page: &'a [u8; PAGE_SIZE]) -> HeadTruncatedKey<'a> {
        HeadTruncatedKey(short_slice(
            page,
            self.offset,
            self.key_len - self.head_len() as u16,
        ))
    }

    pub fn value<'a>(&self, page: &'a [u8; PAGE_SIZE]) -> &'a [u8] {
        short_slice(
            page,
            self.offset + self.key_len - self.head_len() as u16,
            self.val_len,
        )
    }

    pub fn head_len(&self) -> usize {
        (self.key_len as usize).min(4)
    }

    pub fn cmp(&self, other: &Self, page: &[u8; PAGE_SIZE]) -> Ordering {
        self.cmp_external(page, other.head, other.key(page), other.key_len)
    }

    pub fn cmp_external(
        &self,
        page: &[u8; PAGE_SIZE],
        other_head: u32,
        other_key: HeadTruncatedKey,
        other_with_head_len: u16,
    ) -> Ordering {
        let head = self.head;
        head.cmp(&other_head).then_with(|| {
            let self_key_len = self.key_len;
            if self_key_len < 4 || other_with_head_len < 4 {
                self_key_len.cmp(&other_with_head_len)
            } else {
                self.key(page).cmp(&other_key)
            }
        })
    }

    pub fn restore_key(&self, prefix: &[u8], page: &[u8; PAGE_SIZE]) -> SmallBuff {
        let mut v = SmallBuff::with_capacity(prefix.len() + self.key_len as usize);
        v.extend_from_slice(prefix);
        v.extend_from_slice(&self.head.to_be_bytes()[..(self.key_len as usize).min(4)]);
        v.extend_from_slice(self.key(page).0);
        v
    }
}

#[derive(Clone, Copy)]
struct FenceKeySlot {
    offset: u16,
    len: u16,
}

const HINT_COUNT: usize = 16;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct HeadStrippedNodeHead {
    tag: BTreeNodeTag,
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
pub union HeadStrippedNodeData {
    bytes: [u8; PAGE_SIZE - size_of::<HeadStrippedNodeHead>()],
    slots: [HeadStrippedSlot;
        (PAGE_SIZE - size_of::<HeadStrippedNodeHead>()) / size_of::<HeadStrippedSlot>()],
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct HeadStrippedNode {
    head: HeadStrippedNodeHead,
    data: HeadStrippedNodeData,
}

impl HeadStrippedNode {
    fn new(leaf: bool) -> Self {
        HeadStrippedNode {
            head: HeadStrippedNodeHead {
                tag: if leaf {
                    BTreeNodeTag::HeadTruncatedLeaf
                } else {
                    BTreeNodeTag::HeadTruncatedInner
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
            data: HeadStrippedNodeData {
                bytes: unsafe { mem::zeroed() },
            },
        }
    }

    pub fn validate(&self) {
        if cfg!(debug_assertions) {
            for w in self.slots().windows(2) {
                assert!(w[0].cmp(&w[1], self.as_bytes()).is_le());
            }
            assert_eq!(
                self.head.space_used,
                self.slots()
                    .iter()
                    .map(|s| s.key_len - s.head_len() as u16 + s.val_len)
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

    pub fn fence(&self, upper: bool) -> PrefixTruncatedKey {
        let f = if upper {
            self.head.upper_fence
        } else {
            self.head.lower_fence
        };
        PrefixTruncatedKey(short_slice(self.as_bytes(), f.offset, f.len))
    }

    pub fn slots(&self) -> &[HeadStrippedSlot] {
        unsafe { &self.data.slots[..self.head.count as usize] }
    }

    pub fn slots_mut(&mut self) -> &mut [HeadStrippedSlot] {
        unsafe { &mut self.data.slots[..self.head.count as usize] }
    }

    pub fn lower_bound(&self, key: PrefixTruncatedKey) -> (usize, bool) {
        debug_assert!(key.0 <= self.fence(true).0 || self.fence(true).0.is_empty());
        debug_assert!(key.0 > self.fence(false).0 || self.fence(false).0.is_empty());
        if self.head.count == 0 {
            return (0, false);
        }
        let with_head_len = key.0.len();
        let (head, key) = head(key);
        let (lower, upper) = self.search_hint(head);
        let search_result = self.slots()[lower..upper]
            .binary_search_by(|s| s.cmp_external(self.as_bytes(), head, key, with_head_len as u16));
        match search_result {
            Ok(index) | Err(index) => {
                let index = index + lower;
                debug_assert!(
                    index == self.slots().len()
                        || self.slots()[index]
                            .cmp_external(self.as_bytes(), head, key, with_head_len as u16)
                            .is_ge()
                );
                debug_assert!(
                    index == 0
                        || self.slots()[index - 1]
                            .cmp_external(self.as_bytes(), head, key, with_head_len as u16)
                            .is_lt()
                );
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

    pub fn get_child(&self, index: usize) -> *mut BTreeNode {
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

    pub fn insert(
        &mut self,
        key: PrefixTruncatedKey,
        prefix_len: usize,
        payload: &[u8],
    ) -> Result<(), ()> {
        self.request_space(self.space_needed(key.0.len() + prefix_len, payload.len()))?;
        debug_assert!(prefix_len <= self.head.prefix_len as usize);
        let key = PrefixTruncatedKey(&key.0[self.head.prefix_len as usize - prefix_len..]);
        let (slot_id, _) = self.lower_bound(key);
        self.head.count += 1;
        self.assert_no_collide();
        let count = self.head.count as usize;
        self.slots_mut()
            .copy_within(slot_id..count - 1, slot_id + 1);
        self.store_key_value(slot_id, key, payload);
        self.update_hint(slot_id);
        self.validate();
        Ok(())
    }

    fn free_space(&self) -> usize {
        self.head.data_offset as usize
            - size_of::<HeadStrippedNodeHead>()
            - self.slots().len() * size_of::<HeadStrippedSlot>()
    }

    pub fn free_space_after_compaction(&self) -> usize {
        PAGE_SIZE
            - self.head.space_used as usize
            - size_of::<HeadStrippedNodeHead>()
            - self.slots().len() * size_of::<HeadStrippedSlot>()
    }

    pub fn request_space(&mut self, space: usize) -> Result<(), ()> {
        if space <= self.free_space() {
            Ok(())
        } else if space <= self.free_space_after_compaction() {
            self.compactify();
            Ok(())
        } else {
            Err(())
        }
    }

    fn compactify(&mut self) {
        let should = self.free_space_after_compaction();
        let mut tmp = Self::new(self.head.tag.is_leaf());
        tmp.set_fences(self.fence(false), self.fence(true), self.head.prefix_len);
        self.copy_key_value_range(self.slots(), &mut tmp, &[]);
        tmp.head.upper = self.head.upper;
        *self = tmp;
        self.make_hint();
        debug_assert!(self.free_space() == should);
    }

    /// prefix_src must end at prefix end
    fn copy_key_value_range(
        &self,
        src_slots: &[HeadStrippedSlot],
        dst: &mut Self,
        prefix_src: &[u8],
    ) {
        for s in src_slots {
            self.copy_key_value(s, dst, prefix_src);
        }
    }

    fn push_slot(&mut self, s: HeadStrippedSlot) {
        self.head.count += 1;
        self.assert_no_collide();
        *self.slots_mut().last_mut().unwrap() = s;
    }

    /// prefix_src must end at prefix end
    fn copy_key_value(
        &self,
        src_slot: &HeadStrippedSlot,
        dst: &mut HeadStrippedNode,
        prefix_src: &[u8],
    ) {
        let mut buffer = Vec::new();
        buffer.resize(self.head.prefix_len as usize, 0u8);
        let unavailable_prefix_len = buffer.len() - prefix_src.len();
        buffer[unavailable_prefix_len..].copy_from_slice(prefix_src);
        let src_head_len = src_slot.key_len.min(4);
        buffer
            .write(&src_slot.head.to_be_bytes()[..src_head_len as usize])
            .unwrap();
        buffer.write(&src_slot.key(self.as_bytes()).0).unwrap();
        debug_assert!(dst.head.prefix_len as usize >= unavailable_prefix_len);
        let prefix_stripped = PrefixTruncatedKey(&buffer[dst.head.prefix_len as usize..]);
        let (new_head, head_stripped) = head(prefix_stripped);
        dst.write_data(src_slot.value(self.as_bytes()));
        let offset = dst.write_data(head_stripped.0);
        let new_key_len = src_slot.key_len + self.head.prefix_len - dst.head.prefix_len;
        dst.push_slot(HeadStrippedSlot {
            offset,
            key_len: new_key_len,
            val_len: src_slot.val_len,
            head: new_head,
        })
    }

    fn set_fences(
        &mut self,
        lower: PrefixTruncatedKey,
        upper: PrefixTruncatedKey,
        additional_prefix: u16,
    ) {
        debug_assert!(lower <= upper || upper.0.is_empty());
        let new_prefix_len = common_prefix_len(lower.0, upper.0);
        self.head.prefix_len = new_prefix_len as u16 + additional_prefix;
        self.head.lower_fence = FenceKeySlot {
            offset: self.write_data(&lower.0[new_prefix_len..]),
            len: (lower.0.len() - new_prefix_len) as u16,
        };
        self.head.upper_fence = FenceKeySlot {
            offset: self.write_data(&upper.0[new_prefix_len..]),
            len: (upper.0.len() - new_prefix_len) as u16,
        };
    }

    fn store_key_value(
        &mut self,
        slot_id: usize,
        prefix_truncated_key: PrefixTruncatedKey,
        payload: &[u8],
    ) {
        self.write_data(payload);
        let (head, head_truncated) = head(prefix_truncated_key);
        let key_offset = self.write_data(head_truncated.0);
        self.slots_mut()[slot_id] = HeadStrippedSlot {
            offset: key_offset,
            key_len: prefix_truncated_key.0.len() as u16,
            val_len: payload.len() as u16,
            head,
        };
    }

    fn assert_no_collide(&self) {
        let data_start = self.head.data_offset as usize;
        let slot_end = size_of::<HeadStrippedNodeHead>()
            + self.head.count as usize * size_of::<HeadStrippedSlot>();
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
        key_length - self.head.prefix_len as usize + payload_length + size_of::<HeadStrippedSlot>()
    }

    pub fn split_node(&mut self, parent: &mut BTreeNode, key_in_self: &[u8]) -> Result<(), ()> {
        // split
        let separator = self.find_separator();
        let mut full_separator = SmallBuff::from(&key_in_self[..self.prefix_len()]);
        full_separator.extend_from_slice(&separator.prefix_truncated_key);
        let prefix_truncated_separator = PrefixTruncatedKey(&separator.prefix_truncated_key);
        let full_sep_key_len = prefix_truncated_separator.0.len() + self.head.prefix_len as usize;
        debug_assert_eq!(full_separator.len(), full_sep_key_len);
        let space_needed_parent =
            parent.space_needed(full_sep_key_len, size_of::<*mut BTreeNode>());
        parent.request_space(space_needed_parent)?;
        let node_left_raw = BTreeNode::alloc();
        let node_left = unsafe {
            (*node_left_raw).head_truncated = Self::new(self.head.tag.is_leaf());
            &mut (*node_left_raw).head_truncated
        };
        node_left.set_fences(
            self.fence(false),
            prefix_truncated_separator,
            self.head.prefix_len,
        );
        let mut node_right = Self::new(self.head.tag.is_leaf());
        node_right.set_fences(
            prefix_truncated_separator,
            self.fence(true),
            self.head.prefix_len,
        );
        let success = parent
            .insert(
                PrefixTruncatedKey(&full_separator),
                0,
                &(node_left_raw as usize).to_ne_bytes(),
            )
            .is_ok();
        debug_assert!(success);
        if self.head.tag.is_leaf() {
            self.copy_key_value_range(&self.slots()[..=separator.slot_id], node_left, &[]);
            self.copy_key_value_range(&self.slots()[separator.slot_id + 1..], &mut node_right, &[]);
        } else {
            // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
            self.copy_key_value_range(&self.slots()[..separator.slot_id], node_left, &[]);
            self.copy_key_value_range(&self.slots()[separator.slot_id + 1..], &mut node_right, &[]);
            node_left.head.upper = self.get_child(node_left.head.count as usize);
            node_right.head.upper = self.head.upper;
        }
        node_left.make_hint();
        node_left.validate();
        node_right.make_hint();
        node_right.validate();
        *self = node_right;
        Ok(())
    }

    /// returns slot_id and prefix truncated separator
    fn find_separator(&self) -> Separator {
        let k = |i: usize| self.slots()[i].restore_key(&[], self.as_bytes());

        debug_assert!(self.head.count > 1);
        if self.head.tag.is_inner() {
            // inner nodes are split in the middle
            // do not truncate separator to retain fence keys in children
            let slot_id = self.head.count as usize / 2;
            return Separator {
                prefix_truncated_key: k(slot_id),
                slot_id: slot_id,
            };
        }

        let best_slot = if self.head.count >= 16 {
            let lower = (self.head.count as usize / 2) - (self.head.count as usize / 16);
            let upper = self.head.count as usize / 2;
            let k_0 = k(0);
            let best_prefix_len = common_prefix_len(&k_0, &k(lower));
            (lower + 1..=upper)
                .rev()
                .find(|&i| common_prefix_len(&k_0, &k(i)) == best_prefix_len)
                .unwrap_or(lower)
        } else {
            (self.head.count as usize - 1) / 2
        };

        // try to truncate separator
        if best_slot + 1 < self.slots().len() {
            let common = common_prefix_len(&k(best_slot), &k(best_slot + 1));
            if k(best_slot).len() > common && k(best_slot + 1).len() > common + 1 {
                let mut prefix_truncated_key = k(best_slot + 1);
                prefix_truncated_key.truncate(common + 1);
                return Separator {
                    prefix_truncated_key,
                    slot_id: best_slot,
                };
            }
        }
        Separator {
            prefix_truncated_key: k(best_slot),
            slot_id: best_slot,
        }
    }

    pub fn print_slots(&self) {
        for (i, s) in self.slots().iter().enumerate() {
            eprintln!(
                "{:4}|{:3?}|{:3?}",
                i,
                s.head.to_be_bytes(),
                s.key(self.as_bytes())
            );
        }
    }

    pub fn merge_children_check(&mut self, mut child_index: usize) -> Result<(), ()> {
        if child_index == self.slots().len() {
            if child_index == 0 {
                // only one child
                return Err(());
            }
            child_index -= 1;
        }
        unsafe {
            let left = &mut *self.get_child(child_index);
            let right = self.get_child(child_index + 1);
            left.try_merge_right(
                right,
                PrefixTruncatedKey(&self.slots()[child_index].restore_key(&[], self.as_bytes())),
                self.head.prefix_len as usize,
            )?;
            BTreeNode::dealloc(self.get_child(child_index));
            self.remove_slot(child_index);
            self.validate();
            Ok(())
        }
    }

    pub fn merge_right_leaf(
        &mut self,
        right: &mut Self,
        separator: PrefixTruncatedKey,
        separator_prefix_len: usize,
    ) -> Result<(), ()> {
        return Err(());
        /*let mut tmp = HeadStrippedNode::new_leaf();
        let new_prefix_len = self.head.prefix_len.min(right.head.prefix_len) as usize;
        let self_prefix_shrinkage = self.head.prefix_len as usize - new_prefix_len;
        let self_prefix_shrinkage = self.head.prefix_len as usize - new_prefix_len;
        tmp.set_fences(self.fence(false), right.fence(true));
        let left_grow = (self.head.prefix_len - tmp.head.prefix_len) * self.head.count;
        let right_grow = (right.head.prefix_len - tmp.head.prefix_len) * right.head.count;
        let space_upper_bound =
            self.head.space_used as usize + right.head.space_used as usize + size_of::<BasicNodeHead>()
                + size_of::<BasicSlot>() * (self.head.count + right.head.count) as usize + left_grow as usize + right_grow as usize;
        if space_upper_bound > PAGE_SIZE {
            return Err(());
        }
        self.copy_key_value_range(self.slots(), &mut tmp);
        right.copy_key_value_range(right.slots(), &mut tmp);
        tmp.make_hint();
        tmp.validate();
        *right = tmp;
        return Ok(());*/
    }

    pub fn merge_right_inner(
        &mut self,
        right: &mut Self,
        separator: PrefixTruncatedKey,
        separator_prefix_len: usize,
    ) -> Result<(), ()> {
        todo!();
    }

    pub fn remove_slot(&mut self, index: usize) {
        todo!()
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<()> {
        todo!()
    }

    pub fn prefix_len(&self) -> usize {
        self.head.prefix_len as usize
    }
}
