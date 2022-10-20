use crate::btree_node::{BTreeNode, BTreeNodeTag, PAGE_SIZE};
use crate::find_separator::find_separator;
use crate::util::{common_prefix_len, head, partial_restore, short_slice, trailing_bytes};
use crate::PrefixTruncatedKey;
use std::mem::{size_of, transmute};
use std::{mem, ptr};

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
}

#[derive(Clone, Copy)]
struct FenceKeySlot {
    offset: u16,
    len: u16,
}

const HINT_COUNT: usize = 16;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct BasicNodeHead {
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
                tag: if leaf {
                    BTreeNodeTag::BasicLeaf
                } else {
                    BTreeNodeTag::BasicInner
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

    pub fn fence(&self, upper: bool) -> &[u8] {
        let f = if upper {
            self.head.upper_fence
        } else {
            self.head.lower_fence
        };
        &self.as_bytes()[f.offset as usize..][..f.len as usize]
    }

    pub fn prefix(&self) -> &[u8] {
        &self.fence(false)[..self.head.prefix_len as usize]
    }

    pub fn slots(&self) -> &[BasicSlot] {
        unsafe { &self.data.slots[..self.head.count as usize] }
    }

    pub fn slots_mut(&mut self) -> &mut [BasicSlot] {
        unsafe { &mut self.data.slots[..self.head.count as usize] }
    }

    pub fn lower_bound(&self, key: &[u8]) -> (usize, bool) {
        debug_assert!(key <= self.fence(true) || self.fence(true).is_empty());
        debug_assert!(key > self.fence(false) || self.fence(false).is_empty());
        debug_assert!(&key[..self.head.prefix_len as usize] == self.prefix());
        if self.head.count == 0 {
            return (0, false);
        }
        let key = PrefixTruncatedKey(&key[self.head.prefix_len as usize..]);
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

    pub fn insert(&mut self, key: &[u8], payload: &[u8]) -> Result<(), ()> {
        self.request_space(Self::space_needed(key.len(), payload.len()))?;
        let (slot_id, _) = self.lower_bound(key);
        let key = PrefixTruncatedKey(&key[self.head.prefix_len as usize..]);
        self.raw_insert(slot_id, key, payload);
        Ok(())
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
        let mut tmp = Self::new(self.head.tag.is_leaf());
        tmp.set_fences(self.fence(false), self.fence(true));
        self.copy_key_value_range(self.slots(), &mut tmp);
        tmp.head.upper = self.head.upper;
        *self = tmp;
        self.make_hint();
        debug_assert!(self.free_space() == should);
    }

    fn copy_key_value_range(&self, src_slots: &[BasicSlot], dst: &mut Self) {
        for s in src_slots {
            self.copy_key_value(s, dst);
        }
    }

    fn push_slot(&mut self, s: BasicSlot) {
        self.head.count += 1;
        self.assert_no_collide();
        *self.slots_mut().last_mut().unwrap() = s;
    }

    fn copy_key_value(&self, src_slot: &BasicSlot, dst: &mut BasicNode) {
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
                self.prefix(),
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

    fn set_fences(&mut self, lower: &[u8], upper: &[u8]) {
        debug_assert!(lower <= upper || upper.is_empty());
        self.head.prefix_len = common_prefix_len(lower, upper) as u16;
        self.head.lower_fence = FenceKeySlot {
            offset: self.write_data(lower),
            len: lower.len() as u16,
        };
        self.head.upper_fence = FenceKeySlot {
            offset: self.write_data(upper),
            len: upper.len() as u16,
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

    pub fn space_needed(key_length: usize, payload_length: usize) -> usize {
        key_length + payload_length + size_of::<BasicSlot>()
    }

    pub fn split_node(&mut self, parent: &mut BTreeNode, index_in_parent: usize) -> Result<(), ()> {
        // split
        let (sep_slot, truncated_sep_key) = self.find_separator();
        let full_sep_key_len = truncated_sep_key.0.len() + self.head.prefix_len as usize;
        let parent_prefix_len = parent.request_space(full_sep_key_len)?;
        let full_sep = partial_restore(0, &[self.prefix(), truncated_sep_key.0], 0);
        let parent_sep = PrefixTruncatedKey(&full_sep[parent_prefix_len..]);
        let node_left_raw = BTreeNode::alloc();
        let node_left = unsafe {
            (*node_left_raw).basic = Self::new(self.head.tag.is_leaf());
            &mut (*node_left_raw).basic
        };
        node_left.set_fences(self.fence(false), &full_sep);
        let mut node_right = Self::new(self.head.tag.is_leaf());
        node_right.set_fences(&full_sep, self.fence(true));
        parent.insert_child(index_in_parent, parent_sep, node_left_raw);
        if self.head.tag.is_leaf() {
            self.copy_key_value_range(&self.slots()[..=sep_slot], node_left);
            self.copy_key_value_range(&self.slots()[sep_slot + 1..], &mut node_right);
        } else {
            // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
            self.copy_key_value_range(&self.slots()[..sep_slot], node_left);
            self.copy_key_value_range(&self.slots()[sep_slot + 1..], &mut node_right);
            node_left.head.upper = self.get_child(node_left.head.count as usize);
            node_right.head.upper = self.head.upper;
        }
        node_left.make_hint();
        node_right.make_hint();
        *self = node_right;
        Ok(())
    }

    /// returns slot_id and prefix truncated separator
    fn find_separator(&self) -> (usize, PrefixTruncatedKey) {
        find_separator(
            self.head.count as usize,
            self.head.tag.is_leaf(),
            |i: usize| self.slots()[i].key(self.as_bytes()),
        )
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
                self.slots()[child_index].key(self.as_bytes()),
                self.prefix().len(),
            )?;
            BTreeNode::dealloc(self.get_child(child_index));
            self.remove_slot(child_index);
            self.validate();
            Ok(())
        }
    }

    pub fn merge_right_leaf(&mut self, right: &mut Self) -> Result<(), ()> {
        let mut tmp = BasicNode::new_leaf();
        tmp.set_fences(self.fence(false), right.fence(true));
        let left_grow = (self.head.prefix_len - tmp.head.prefix_len) * self.head.count;
        let right_grow = (right.head.prefix_len - tmp.head.prefix_len) * right.head.count;
        let space_upper_bound = self.head.space_used as usize
            + right.head.space_used as usize
            + size_of::<BasicNodeHead>()
            + size_of::<BasicSlot>() * (self.head.count + right.head.count) as usize
            + left_grow as usize
            + right_grow as usize;
        if space_upper_bound > PAGE_SIZE {
            return Err(());
        }
        self.copy_key_value_range(self.slots(), &mut tmp);
        right.copy_key_value_range(right.slots(), &mut tmp);
        tmp.make_hint();
        tmp.validate();
        *right = tmp;
        return Ok(());
    }

    pub fn merge_right_inner(
        &mut self,
        right: &mut Self,
        separator: PrefixTruncatedKey,
        separator_prefix_len: usize,
    ) -> Result<(), ()> {
        let mut tmp = BasicNode::new_inner(right.head.upper);
        tmp.set_fences(self.fence(false), right.fence(true));
        let separator_prefix_len_diff = tmp.prefix().len() - separator_prefix_len;
        let left_grow = (self.head.prefix_len - tmp.head.prefix_len) * self.head.count;
        let right_grow = (right.head.prefix_len - tmp.head.prefix_len) * right.head.count;
        let separator_len = separator.0.len() - separator_prefix_len_diff;
        let space_use = self.head.space_used as usize
            + right.head.space_used as usize
            + size_of::<BasicNodeHead>()
            + size_of::<BasicSlot>() * (self.head.count + right.head.count) as usize
            + left_grow as usize
            + right_grow as usize
            + Self::space_needed(separator_len, size_of::<*mut BTreeNode>());
        if space_use > PAGE_SIZE {
            return Err(());
        }
        self.copy_key_value_range(self.slots(), &mut tmp);
        tmp.head.count += 1;
        tmp.store_key_value(
            self.head.count as usize,
            PrefixTruncatedKey(&separator.0[separator_prefix_len_diff..]),
            &(self.head.upper as usize).to_ne_bytes(),
        );
        right.copy_key_value_range(right.slots(), &mut tmp);
        tmp.make_hint();
        *right = tmp;
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

    pub fn remove(&mut self, key: &[u8]) -> Option<()> {
        let (slot_id, found) = self.lower_bound(key);
        if !found {
            return None;
        }
        self.remove_slot(slot_id);
        Some(())
    }
}
