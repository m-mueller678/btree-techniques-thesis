use crate::find_separator::find_separator;
use crate::util::{common_prefix_len, merge_fences, partial_restore, short_slice};
use crate::{BTreeNode, BTreeNodeTag, FatTruncatedKey, PrefixTruncatedKey, PAGE_SIZE};
use rustc_hash::FxHasher;
use std::hash::Hasher;
use std::io::Write;
use std::mem::{align_of, size_of, transmute, ManuallyDrop};
use std::simd::SimdPartialEq;

#[derive(Clone, Copy)]
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

#[derive(Clone)]
struct FenceKeySlot {
    offset: u16,
    len: u16,
}

#[derive(Clone)]
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

#[derive(Clone)]
#[repr(C)]
#[repr(align(64))]
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
const USE_SIMD: bool = true;
const SIMD_WIDTH: usize = 32;
const SIMD_ALIGN: usize = 64;

impl HashLeaf {
    pub fn space_needed(&self, key_length: usize, payload_length: usize) -> usize {
        assert!(SLOTS_FIRST);
        let head_growth = if USE_SIMD {
            SIMD_ALIGN.max(size_of::<HashSlot>()) + 1
        } else {
            size_of::<HashSlot>() + 1
        };
        key_length - self.head.prefix_len as usize + payload_length + head_growth
    }

    fn layout(count: usize) -> LayoutInfo {
        debug_assert!(SLOTS_FIRST);
        let slots_start = size_of::<HashLeafHead>();
        let hash_start = slots_start + size_of::<HashSlot>() * count;
        let hash_start = if USE_SIMD {
            hash_start.next_multiple_of(SIMD_ALIGN)
        } else {
            hash_start
        };
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
        //eprintln!("{:?} compactify",self as *const Self);
        let mut buffer = [0u8; PAGE_SIZE];
        let fences_len = self.head.lower_fence.len as usize + self.head.upper_fence.len as usize;
        let new_data_offset = PAGE_SIZE - self.head.space_used as usize;
        let new_data_range = new_data_offset..PAGE_SIZE - fences_len;
        let mut write = &mut buffer[new_data_range.clone()];
        for i in 0..self.head.count as usize {
            let new_offset = (PAGE_SIZE - fences_len - write.len()) as u16;
            debug_assert!(new_offset >= new_data_offset as u16);
            write
                .write_all(self.slots()[i].key(self.as_bytes()).0)
                .unwrap();
            write
                .write_all(self.slots()[i].value(self.as_bytes()))
                .unwrap();
            self.slots_mut()[i].offset = new_offset;
        }
        debug_assert!(write.is_empty());
        unsafe {
            self.as_bytes_mut()[new_data_range.clone()].copy_from_slice(&buffer[new_data_range])
        };
        self.head.data_offset = new_data_offset as u16;
        self.validate();
    }

    fn compute_hash(key: PrefixTruncatedKey) -> u8 {
        let mut hasher = FxHasher::default();
        hasher.write(key.0);
        (hasher.finish() >> 56) as u8
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
        // self.print();
        //eprintln!("{:?} insert {:?}",self as *const Self,key);
        let key = self.truncate(key);
        self.insert_truncated(key, payload)
    }

    fn insert_truncated(&mut self, key: PrefixTruncatedKey, payload: &[u8]) -> Result<(), ()> {
        let index = if let Some(found) = self.find_index(key) {
            let s = &mut self.slots_mut()[found];
            let old_use = s.key_len + s.val_len;
            s.key_len = 0;
            s.val_len = 0;
            self.head.space_used -= old_use;
            self.request_space(key.0.len() + payload.len())?;
            found
        } else {
            self.request_space(
                self.space_needed(key.0.len() + self.head.prefix_len as usize, payload.len()),
            )?;
            self.increase_size(1);
            self.head.count as usize - 1
        };
        self.store_key_value(index, key, payload);
        // self.print();
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
        if let Err(()) = parent.insert_child(index_in_parent, parent_sep, node_left_raw) {
            unsafe {
                BTreeNode::dealloc(node_left_raw);
                return Err(());
            }
        }
        if self.head.tag.is_leaf() {
            self.copy_key_value_range(&self.slots()[..=sep_slot], node_left);
            self.copy_key_value_range(&self.slots()[sep_slot + 1..], &mut node_right);
        }
        node_left.validate();
        node_right.validate();
        // node_left.print();
        // node_right.print();
        debug_assert_eq!(
            self.head.count,
            node_left.head.count + node_right.head.count
        );
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
        assert_eq!(align_of::<Self>(), SIMD_ALIGN);
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
        self.find_index(self.truncate(key))
            .map(|i| self.slots()[i].value(self.as_bytes()))
    }

    fn find_index(&self, key: PrefixTruncatedKey) -> Option<usize> {
        let needle_hash = Self::compute_hash(key);
        //eprintln!("find {:?} -> {}",key,needle_hash);
        if USE_SIMD {
            debug_assert_eq!(
                self.find_simd(key, needle_hash),
                self.find_no_simd(key, needle_hash)
            );
            self.find_simd(key, needle_hash)
        } else {
            self.find_no_simd(key, needle_hash)
        }
    }

    fn find_no_simd(&self, key: PrefixTruncatedKey, needle_hash: u8) -> Option<usize> {
        for (i, hash) in self.hashes().iter().enumerate() {
            if *hash == needle_hash && self.slots()[i].key(self.as_bytes()) == key {
                return Some(i);
            }
        }
        None
    }

    fn find_simd(&self, key: PrefixTruncatedKey, needle_hash: u8) -> Option<usize> {
        unsafe {
            use std::simd::ToBitMask;
            type SimdDtype = std::simd::Simd<u8, SIMD_WIDTH>;
            let count = self.head.count as usize;
            let mut hash_ptr = (self as *const Self as *const u8)
                .offset(Self::layout(count).hash_start as isize)
                as *const SimdDtype;
            let needle = SimdDtype::splat(needle_hash);
            debug_assert!(hash_ptr.is_aligned());
            let mut shift = 0;
            while shift < count {
                let candidates = *(hash_ptr);
                let mut matches = candidates.simd_eq(needle).to_bitmask();
                loop {
                    let trailing_zeros = matches.trailing_zeros();
                    if trailing_zeros == SIMD_WIDTH as u32 {
                        shift = shift - shift % SIMD_WIDTH + SIMD_WIDTH;
                        hash_ptr = hash_ptr.offset(1);
                        break;
                    } else {
                        shift += trailing_zeros as usize;
                        matches >>= trailing_zeros;
                        matches = matches & !1;
                        if shift >= count {
                            return None;
                        }
                        if self.slots()[shift].key(self.as_bytes()) == key {
                            return Some(shift);
                        }
                    }
                }
            }
        }
        None
    }

    pub fn validate(&self) {
        const VALIDATE_HASH_QUALITY: bool = false;
        if cfg!(debug_assertions) && VALIDATE_HASH_QUALITY {
            let mut counts = [0; 256];
            let average = self.head.count as f32 / 256.0;
            let mut acc = 0.0;
            for h in self.hashes() {
                counts[*h as usize] += 1;
            }
            for c in counts {
                acc += (c as f32 - average).powi(2);
            }
            assert!(acc < 750.0);
        }
        self.assert_no_collide();
        debug_assert!(
            self.fence(false) < self.fence(true)
                || self.fence(true).0.is_empty() && self.head.prefix_len == 0
        );
        for s in self.slots() {
            debug_assert!(s.offset >= self.head.data_offset);
        }
        for (s, h) in self.slots().iter().zip(self.hashes().iter()) {
            debug_assert_eq!(Self::compute_hash(s.key(self.as_bytes())), *h);
        }
        debug_assert_eq!(
            self.head.space_used as usize,
            self.head.lower_fence.len as usize
                + self.head.upper_fence.len as usize
                + self
                .slots()
                .iter()
                .map(|s| (s.key_len + s.val_len) as usize)
                .sum::<usize>()
        );
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<()> {
        //eprintln!("### {:?} remove {:?}",self as *const Self,key);
        // self.print();

        let index = self.find_index(self.truncate(key))?;
        let new_count = self.head.count as usize - 1;
        let last = new_count;
        let slot = self.slots()[index];
        self.head.space_used -= slot.key_len + slot.val_len;
        if index < last {
            let slots = self.slots_mut();
            slots[index] = slots[last];
            let hashes = self.hashes_mut();
            hashes[index] = hashes[last];
        }
        assert!(SLOTS_FIRST);
        let old_layout = Self::layout(new_count + 1);
        let new_layout = Self::layout(new_count);
        unsafe {
            self.as_bytes_mut().copy_within(
                old_layout.hash_start..old_layout.hash_start + new_count,
                new_layout.hash_start,
            );
        }
        debug_assert_eq!(old_layout.slots_start, new_layout.slots_start);
        self.head.count -= 1;
        self.validate();
        // self.print();
        Some(())
    }

    pub fn try_merge_right(
        &mut self,
        right: &mut Self,
        separator: FatTruncatedKey,
    ) -> Result<(), ()> {
        //eprintln!("### {:?} merge right {:?}",self as *const Self,right as *const Self);
        // self.print();
        // right.print();
        //TODO optimize
        let mut tmp = Self::new();
        merge_fences(
            FatTruncatedKey {
                remainder: self.fence(false).0,
                prefix_len: self.head.prefix_len as usize,
            },
            separator,
            FatTruncatedKey {
                remainder: right.fence(true).0,
                prefix_len: right.head.prefix_len as usize,
            },
            |lo, hi, p| {
                tmp.set_fences(lo, hi, p as u16);
            },
        );
        let left = self.slots().iter().map(|s| (s, &*self));
        let right_iter = right.slots().iter().map(|s| (s, &*right));
        for (s, this) in left.chain(right_iter) {
            let segments = &[
                &separator.remainder[..this.head.prefix_len as usize - separator.prefix_len],
                s.key(this.as_bytes()).0,
            ];
            let reconstructed =
                partial_restore(separator.prefix_len, segments, tmp.head.prefix_len as usize);
            tmp.insert_truncated(PrefixTruncatedKey(&reconstructed), s.value(this.as_bytes()))?;
        }
        tmp.validate();
        // tmp.print();
        debug_assert_eq!(tmp.head.count, self.head.count + right.head.count);
        *right = tmp;
        Ok(())
    }

    pub fn truncate<'a>(&self, key: &'a [u8]) -> PrefixTruncatedKey<'a> {
        PrefixTruncatedKey(&key[self.head.prefix_len as usize..])
    }

    #[allow(dead_code)]
    fn print(&self) {
        eprintln!(
            "HashLeaf {:?} [{:?}..], {:?} - {:?}",
            self as *const Self,
            self.head.prefix_len,
            self.fence(false).0,
            self.fence(true).0
        );
        for (i, s) in self.slots().iter().enumerate() {
            eprintln!(
                "{:?}|{:3?}|{:3?}",
                i,
                self.hashes()[i],
                s.key(self.as_bytes())
            );
        }
    }

    pub fn validate_tree(&self, lower: &[u8], upper: &[u8]) {
        debug_assert_eq!(
            self.head.prefix_len as usize,
            common_prefix_len(lower, upper)
        );
        debug_assert_eq!(self.fence(false), self.truncate(&lower));
        debug_assert_eq!(self.fence(true), self.truncate(&upper));
    }
}
