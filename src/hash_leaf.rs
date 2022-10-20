use crate::find_separator::find_separator;
use crate::util::{common_prefix_len, partial_restore, short_slice};
use crate::{BTreeNode, BTreeNodeTag, PrefixTruncatedKey, PAGE_SIZE};
use rustc_hash::FxHasher;
use std::hash::Hasher;
use std::io::Write;
use std::mem::{size_of, transmute, ManuallyDrop};

struct HashSlot {
    offset: u16,
    key_len: u16,
    val_len: u16,
}

impl HashSlot {
    pub fn key<'a>(&self, page: &'a [u8; PAGE_SIZE]) -> PrefixTruncatedKey<'a> {
        PrefixTruncatedKey(short_slice(page, self.offset, self.key_len))
    }

    pub fn value<'a>(&self, page: &'a [u8; PAGE_SIZE]) -> &'a [u8] {
        short_slice(page, self.offset + self.key_len, self.val_len)
    }
}

struct FenceKeySlot {
    offset: u16,
    len: u16,
}

#[repr(C)]
struct HashLeafHead {
    tag: BTreeNodeTag,
    count: u16,
    lower_fence: FenceKeySlot,
    upper_fence: FenceKeySlot,
    space_used: u16,
    data_offset: u16,
    prefix_len: u16,
}

#[repr(C)]
pub struct HashLeaf {
    head: HashLeafHead,
    data: [u8; PAGE_SIZE - size_of::<HashLeafHead>()],
}

struct LayoutInfo {
    slots_start: usize,
    hash_start: usize,
    data_start: usize,
}

const SLOTS_FIRST: bool = true;

impl HashLeaf {
    pub fn space_needed(key_length: usize, payload_length: usize) -> usize {
        key_length + payload_length + size_of::<HashSlot>() + 1
    }

    fn layout(count: usize) -> LayoutInfo {
        debug_assert!(SLOTS_FIRST);
        let slots_start = size_of::<HashLeafHead>();
        let hash_start = slots_start + size_of::<HashSlot>() * count;
        let data_start = hash_start + count;
        LayoutInfo {
            slots_start,
            hash_start,
            data_start,
        }
    }

    fn free_space(&self) -> usize {
        self.head.data_offset as usize - Self::layout(self.head.count as usize).data_start
    }

    pub fn free_space_after_compaction(&self) -> usize {
        PAGE_SIZE
            - Self::layout(self.head.count as usize).data_start
            - self.head.space_used as usize
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        assert_eq!(PAGE_SIZE, size_of::<Self>());
        unsafe { transmute(self as *const Self) }
    }

    unsafe fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        assert_eq!(PAGE_SIZE, size_of::<Self>());
        transmute(self as *mut Self)
    }

    fn slots(&self) -> &[HashSlot] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self as *const u8)
                    .offset(Self::layout(self.head.count as usize).slots_start as isize)
                    as *const HashSlot,
                self.head.count as usize,
            )
        }
    }

    fn slots_mut(&mut self) -> &mut [HashSlot] {
        unsafe {
            let count = self.head.count as usize;
            std::slice::from_raw_parts_mut(
                (self as *mut Self as *mut u8).offset(Self::layout(count).slots_start as isize)
                    as *mut HashSlot,
                count,
            )
        }
    }

    pub fn hashes(&self) -> &[u8] {
        let count = self.head.count as usize;
        &self.as_bytes()[Self::layout(count).hash_start..][..count]
    }

    pub fn hashes_mut(&mut self) -> &mut [u8] {
        unsafe {
            let count = self.head.count as usize;
            &mut self.as_bytes_mut()[Self::layout(count).hash_start..][..count]
        }
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
        let mut buffer = [0u8; PAGE_SIZE];
        let fences_len = self.head.lower_fence.len as usize + self.head.upper_fence.len as usize;
        let new_data_offset = PAGE_SIZE - self.head.space_used as usize;
        let new_data_range = new_data_offset..PAGE_SIZE - fences_len;
        let mut write = &mut buffer[new_data_range.clone()];
        for i in 0..self.head.count as usize {
            let s = &mut self.slots_mut()[i];
            s.offset = (PAGE_SIZE - fences_len - write.len()) as u16;
            write
                .write_all(self.slots()[i].key(self.as_bytes()).0)
                .unwrap();
            write
                .write_all(self.slots()[i].value(self.as_bytes()))
                .unwrap();
        }
        debug_assert!(write.len() == 0);
        unsafe {
            self.as_bytes_mut()[new_data_range.clone()].copy_from_slice(&buffer[new_data_range])
        };
        self.head.data_offset = new_data_offset as u16;
        self.validate();
    }

    fn compute_hash(key: PrefixTruncatedKey) -> u8 {
        let mut hasher = FxHasher::default();
        hasher.write(key.0);
        hasher.finish() as u8
    }

    fn store_key_value(
        &mut self,
        slot_id: usize,
        prefix_truncated_key: PrefixTruncatedKey,
        payload: &[u8],
    ) {
        self.write_data(payload);
        let key_offset = self.write_data(prefix_truncated_key.0);
        self.slots_mut()[slot_id] = HashSlot {
            offset: key_offset,
            key_len: prefix_truncated_key.0.len() as u16,
            val_len: payload.len() as u16,
        };
        self.hashes_mut()[slot_id] = Self::compute_hash(prefix_truncated_key);
    }

    pub fn insert(&mut self, key: &[u8], payload: &[u8]) -> Result<(), ()> {
        self.request_space(Self::space_needed(key.len(), payload.len()))?;
        self.increase_size(1);
        self.store_key_value(
            self.head.count as usize - 1,
            PrefixTruncatedKey(&key[self.head.prefix_len as usize..]),
            payload,
        );
        self.validate();
        Ok(())
    }

    fn increase_size(&mut self, delta: usize) {
        assert!(SLOTS_FIRST);
        let count = self.head.count as usize;
        let old_layout = Self::layout(count as usize);
        let new_layout = Self::layout(count + delta);
        unsafe {
            self.as_bytes_mut().copy_within(
                old_layout.hash_start..old_layout.hash_start + count,
                new_layout.hash_start,
            );
        }
        self.head.count += delta as u16;
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

    fn assert_no_collide(&self) {
        debug_assert!(
            Self::layout(self.head.count as usize).data_start <= self.head.data_offset as usize
        );
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

    fn copy_key_value_range(&self, src_slots: &[HashSlot], dst: &mut Self) {
        assert!(dst.head.prefix_len >= self.head.prefix_len);
        let dst_base = dst.head.count as usize;
        dst.increase_size(src_slots.len());
        let prefix_growth = (dst.head.prefix_len - self.head.prefix_len) as usize;
        for (i, s) in src_slots.iter().enumerate() {
            dst.store_key_value(
                dst_base + i,
                PrefixTruncatedKey(&s.key(self.as_bytes()).0[prefix_growth..]),
                s.value(self.as_bytes()),
            );
        }
    }

    fn prefix<'a>(&self, key_in_node: &'a [u8]) -> &'a [u8] {
        &key_in_node[..self.head.prefix_len as usize]
    }

    pub fn split_node(
        &mut self,
        parent: &mut BTreeNode,
        index_in_parent: usize,
        key_in_self: &[u8],
    ) -> Result<(), ()> {
        {
            //sort
            let this = self as *mut Self as *mut u8;
            let count = self.head.count as usize;
            unsafe {
                let slots = std::slice::from_raw_parts_mut(
                    this.offset(Self::layout(count).slots_start as isize) as *mut HashSlot,
                    count,
                );
                slots.sort_by_key(|s| {
                    std::slice::from_raw_parts(this.offset(s.offset as isize), s.key_len as usize)
                });
            };
        }

        // split
        let (sep_slot, truncated_sep_key) =
            find_separator(self.head.count as usize, true, |i: usize| {
                self.slots()[i].key(self.as_bytes())
            });
        let full_sep_key_len = truncated_sep_key.0.len() + self.head.prefix_len as usize;
        let parent_prefix_len = parent.request_space_for_child(full_sep_key_len)?;
        let node_left_raw = BTreeNode::alloc();
        let node_left = unsafe {
            (*node_left_raw).hash_leaf = ManuallyDrop::new(Self::new());
            &mut (*node_left_raw).hash_leaf
        };
        node_left.set_fences(self.fence(false), truncated_sep_key, self.head.prefix_len);
        let mut node_right = Self::new();
        node_right.set_fences(truncated_sep_key, self.fence(true), self.head.prefix_len);
        let parent_sep = partial_restore(
            0,
            &[self.prefix(key_in_self), truncated_sep_key.0],
            parent_prefix_len,
        );
        let parent_sep = PrefixTruncatedKey(&parent_sep);
        parent.insert_child(index_in_parent, parent_sep, node_left_raw);
        if self.head.tag.is_leaf() {
            self.copy_key_value_range(&self.slots()[..=sep_slot], node_left);
            self.copy_key_value_range(&self.slots()[sep_slot + 1..], &mut node_right);
        }
        node_left.validate();
        node_right.validate();
        *self = node_right;
        Ok(())
    }

    pub fn fence(&self, upper: bool) -> PrefixTruncatedKey {
        let f = if upper {
            &self.head.upper_fence
        } else {
            &self.head.lower_fence
        };
        PrefixTruncatedKey(short_slice(self.as_bytes(), f.offset, f.len))
    }

    pub fn new() -> Self {
        HashLeaf {
            head: HashLeafHead {
                tag: BTreeNodeTag::HashLeaf,
                count: 0,
                lower_fence: FenceKeySlot { offset: 0, len: 0 },
                upper_fence: FenceKeySlot { offset: 0, len: 0 },
                space_used: 0,
                data_offset: PAGE_SIZE as u16,
                prefix_len: 0,
            },
            data: [0u8; PAGE_SIZE - size_of::<HashLeafHead>()],
        }
    }

    pub fn lookup(&self, key: &[u8]) -> Option<&[u8]> {
        let key = PrefixTruncatedKey(&key[self.head.prefix_len as usize..]);
        let needle_hash = Self::compute_hash(key);
        for (i, hash) in self.hashes().iter().enumerate() {
            if *hash == needle_hash && self.slots()[i].key(self.as_bytes()) == key {
                return Some(self.slots()[i].value(self.as_bytes()));
            }
        }
        None
    }

    pub fn validate(&self) {
        self.assert_no_collide();
        for s in self.slots() {
            debug_assert!(s.offset >= self.head.data_offset);
        }
        debug_assert_eq!(
            self.head.space_used as usize,
            self.head.lower_fence.len as usize
                + self.head.upper_fence.len as usize
                + self
                .slots()
                .iter()
                .map(|s| (s.val_len + s.key_len) as usize)
                .sum::<usize>()
        );
    }
}
