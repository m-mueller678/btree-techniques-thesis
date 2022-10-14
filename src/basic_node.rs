use crate::btree_node::{BTreeNode, BTreeNodeTag, PAGE_SIZE};
use crate::util::{common_prefix_len, head, short_slice, trailing_bytes};
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
    pub fn key<'a>(&self, page: &'a [u8; PAGE_SIZE]) -> &'a [u8] {
        short_slice(page, self.offset, self.key_len)
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

    fn validate(&self) {
        if cfg!(debug_assertions) {
            for w in self.slots().windows(2) {
                assert!(w[0].key(self.as_bytes()) <= w[1].key(self.as_bytes()));
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

    fn fence(&self, upper: bool) -> &[u8] {
        let f = if upper {
            self.head.upper_fence
        } else {
            self.head.lower_fence
        };
        &self.as_bytes()[f.offset as usize..][..f.len as usize]
    }

    fn prefix(&self) -> &[u8] {
        &self.fence(false)[..self.head.prefix_len as usize]
    }

    pub fn slots(&self) -> &[BasicSlot] {
        unsafe { &self.data.slots[..self.head.count as usize] }
    }

    pub fn slots_mut(&mut self) -> &mut [BasicSlot] {
        unsafe { &mut self.data.slots[..self.head.count as usize] }
    }

    pub fn lower_bound(&self, mut key: &[u8]) -> (usize, bool) {
        debug_assert!(key <= self.fence(true) || self.fence(true).is_empty());
        debug_assert!(key > self.fence(false));
        debug_assert!(&key[..self.head.prefix_len as usize] == self.prefix());
        if self.head.count == 0 {
            return (0, false);
        }
        key = &key[self.head.prefix_len as usize..];
        let head = head(key);
        let (lower, upper) = self.search_hint(head);
        let search_result = self.slots()[lower..upper].binary_search_by(|s| {
            let slot_head = s.head;
            slot_head
                .cmp(&head)
                .then_with(|| s.key(self.as_bytes()).cmp(key))
        });
        match search_result {
            Ok(index) | Err(index) => (index + lower, search_result.is_ok()),
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

    pub unsafe fn get_child(&self, index: usize) -> *mut BTreeNode {
        debug_assert!(index <= self.head.count as usize);
        if index == self.head.count as usize {
            self.head.upper
        } else {
            ptr::read_unaligned(
                self.slots()[index].value(self.as_bytes()).as_ptr() as *const *mut BTreeNode
            )
        }
    }

    pub fn insert(&mut self, key: &[u8], payload: &[u8]) -> Result<(), ()> {
        self.request_space(Self::space_needed(key.len(), payload.len()))?;
        let (slot_id, _) = self.lower_bound(key);
        self.head.count += 1;
        self.assert_no_collide();
        let count = self.head.count as usize;
        self.slots_mut()
            .copy_within(slot_id..count - 1, slot_id + 1);
        self.store_key_value(slot_id, &key[self.head.prefix_len as usize..], payload);
        self.update_hint(slot_id);
        self.validate();
        Ok(())
    }

    fn free_space(&self) -> usize {
        self.head.data_offset as usize
            - size_of::<BasicNodeHead>()
            - self.slots().len() * size_of::<BasicSlot>()
    }

    fn free_space_after_compaction(&self) -> usize {
        PAGE_SIZE
            - self.head.space_used as usize
            - size_of::<BasicNodeHead>()
            - self.slots().len() * size_of::<BasicSlot>()
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
        tmp.set_fences(self.fence(false), self.fence(true));
        self.copy_key_value_range(self.slots(), &mut tmp);
        tmp.head.upper = self.head.upper;
        *self = tmp;
        self.make_hint();
        debug_assert!(self.free_space() == should);
    }

    fn copy_key_value_range(&self, src_slots: &[BasicSlot], dst: &mut Self) {
        if dst.head.prefix_len >= self.head.prefix_len {
            let diff = dst.head.prefix_len - self.head.prefix_len;
            for src in src_slots {
                let new_key = &src.key(self.as_bytes())[diff as usize..];
                dst.write_data(src.value(self.as_bytes()));
                let offset = dst.write_data(new_key);
                dst.push_slot(BasicSlot {
                    offset,
                    key_len: new_key.len() as u16,
                    val_len: src.val_len,
                    head: head(new_key),
                })
            }
        } else {
            for s in src_slots {
                self.copy_key_value(s, dst);
            }
        }
    }

    fn push_slot(&mut self, s: BasicSlot) {
        self.head.count += 1;
        self.assert_no_collide();
        *self.slots_mut().last_mut().unwrap() = s;
    }

    fn copy_key_value(&self, src_slot: &BasicSlot, dst: &mut BasicNode) {
        dst.write_data(src_slot.value(self.as_bytes()));
        let offset = if self.head.prefix_len <= dst.head.prefix_len {
            dst.write_data(
                &src_slot.key(self.as_bytes())
                    [(dst.head.prefix_len - self.head.prefix_len) as usize..],
            )
        } else {
            dst.write_data(src_slot.key(self.as_bytes()));
            dst.write_data(trailing_bytes(
                self.prefix(),
                (self.head.prefix_len - dst.head.prefix_len) as usize,
            ))
        };
        dst.push_slot(BasicSlot {
            offset,
            key_len: src_slot.key_len + self.head.prefix_len - dst.head.prefix_len,
            val_len: src_slot.val_len,
            head: 0,
        });
    }

    fn set_fences(&mut self, lower: &[u8], upper: &[u8]) {
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

    fn store_key_value(&mut self, slot_id: usize, prefix_truncated_key: &[u8], payload: &[u8]) {
        self.write_data(payload);
        let key_offset = self.write_data(prefix_truncated_key);
        self.slots_mut()[slot_id] = BasicSlot {
            offset: key_offset,
            key_len: prefix_truncated_key.len() as u16,
            val_len: payload.len() as u16,
            head: head(prefix_truncated_key),
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
        let dist = count / (HINT_COUNT + 1);
        for i in 0..HINT_COUNT {
            self.head.hint[i] = self.slots()[dist * (i + 1)].head;
            debug_assert!(i == 0 || self.head.hint[i - 1] <= self.head.hint[i]);
        }
    }

    pub fn space_needed(key_length: usize, payload_length: usize) -> usize {
        key_length + payload_length + size_of::<BasicSlot>()
    }

    pub fn split_node(&mut self, parent: &mut BTreeNode) -> Result<(), ()> {
        // split
        let (sep_slot, sep_key) = self.find_separator();
        let space_needed_parent = parent.space_needed(sep_key.len(), size_of::<*mut BTreeNode>());
        parent.request_space(space_needed_parent)?;
        let node_left_raw = BTreeNode::alloc();
        let node_left = unsafe {
            (*node_left_raw).basic = Self::new(self.head.tag.is_leaf());
            &mut (*node_left_raw).basic
        };
        node_left.set_fences(self.fence(false), sep_key);
        let mut node_right = Self::new(self.head.tag.is_leaf());
        node_right.set_fences(sep_key, self.fence(true));
        {
            let prefix_len = self.prefix().len();
            let mut buffer = [0u8; PAGE_SIZE / 4];
            buffer[..prefix_len].copy_from_slice(self.prefix());
            buffer[prefix_len..][..sep_key.len()].copy_from_slice(sep_key);
            let full_sep_key = &buffer[..prefix_len + sep_key.len()];
            let success = parent
                .insert(full_sep_key, &(node_left_raw as usize).to_ne_bytes())
                .is_ok();
            debug_assert!(success);
        }
        if self.head.tag.is_leaf() {
            self.copy_key_value_range(&self.slots()[..=sep_slot], node_left);
            self.copy_key_value_range(&self.slots()[sep_slot + 1..], &mut node_right);
        } else {
            // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
            self.copy_key_value_range(&self.slots()[..sep_slot], node_left);
            self.copy_key_value_range(&self.slots()[sep_slot + 1..], &mut node_right);
            node_left.head.upper = unsafe { self.get_child(node_left.head.count as usize) };
            node_right.head.upper = self.head.upper;
        }
        node_left.make_hint();
        node_right.make_hint();
        *self = node_right;
        Ok(())
    }

    /// returns slot_id and prefix truncated separator
    fn find_separator(&self) -> (usize, &[u8]) {
        let k = |i: usize| self.slots()[i].key(self.as_bytes());

        debug_assert!(self.head.count > 1);
        if self.head.tag.is_inner() {
            // inner nodes are split in the middle
            // do not truncate separator to retain fence keys in children
            let slot_id = self.head.count as usize / 2;
            return (slot_id, self.slots()[slot_id].key(self.as_bytes()));
        }

        let best_slot = if self.head.count >= 16 {
            let lower = (self.head.count as usize / 2) - (self.head.count as usize / 16);
            let upper = self.head.count as usize / 2;
            let best_prefix_len = common_prefix_len(k(0), k(lower));
            (lower + 1..=upper)
                .rev()
                .find(|&i| common_prefix_len(k(0), k(i)) == best_prefix_len)
                .unwrap_or(lower)
        } else {
            (self.head.count as usize - 1) / 2
        };

        // try to truncate separator
        if best_slot + 1 < self.slots().len() {
            let common = common_prefix_len(k(best_slot), k(best_slot + 1));
            if k(best_slot).len() > common && k(best_slot + 1).len() > common + 1 {
                return (best_slot, &k(best_slot + 1)[..common + 1]);
            }
        }
        (best_slot, k(best_slot))
    }
}
