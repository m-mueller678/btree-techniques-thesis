use crate::find_separator::find_separator;
use crate::util::{common_prefix_len, MergeFences, partial_restore, short_slice, SplitFences};
use crate::{BTreeNode, PrefixTruncatedKey, PAGE_SIZE, FatTruncatedKey};
use rustc_hash::FxHasher;
use std::hash::Hasher;
use std::io::Write;
use std::mem::{size_of, transmute, ManuallyDrop, align_of};
use std::simd::{Simd, SimdPartialEq};
use crate::btree_node::{AdaptionState, BTreeNodeHead};
use crate::node_traits::{FenceData, FenceRef, InnerNode, LeafNode, Node};
use crate::vtables::BTreeNodeTag;

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
    head: BTreeNodeHead,
    count: u16,
    sorted_count: u16,
    lower_fence: FenceKeySlot,
    upper_fence: FenceKeySlot,
    space_used: u16,
    data_offset: u16,
    prefix_len: u16,
    hash_area: FenceKeySlot,
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
    data_start: usize,
}

const USE_SIMD: bool = true;
const SIMD_WIDTH: usize = 64;
const SIMD_ALIGN: usize = align_of::<Simd<u8, SIMD_WIDTH>>();

impl HashLeaf {
    pub fn space_needed_new_slot(&self, key_length: usize, payload_length: usize) -> usize {
        let hash_space = if self.head.count == self.head.hash_area.len { Self::hash_capacity(self.head.count as usize + 1) } else { 0 };
        key_length - self.head.prefix_len as usize + payload_length + hash_space + size_of::<HashSlot>()
    }

    fn layout(count: usize) -> LayoutInfo {
        let slots_start = size_of::<HashLeafHead>();
        let data_start = slots_start + size_of::<HashSlot>() * count;
        LayoutInfo {
            slots_start,
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
        &self.as_bytes()[self.head.hash_area.offset as usize..][..self.head.count as usize]
    }

    pub fn hashes_mut(&mut self) -> &mut [u8] {
        let offset = self.head.hash_area.offset;
        let count = self.head.count;
        unsafe {
            &mut self.as_bytes_mut()[offset as usize..][..count as usize]
        }
    }

    pub fn request_space(&mut self, space: usize) -> Result<(), ()> {
        if space <= self.free_space() {
            Ok(())
        } else {
            // shrink hash area
            let target_hash_capacity = Self::hash_capacity(self.head.count as usize) as u16;
            self.head.space_used -= self.head.hash_area.len - target_hash_capacity;
            self.head.hash_area.len = target_hash_capacity;

            if space <= self.free_space_after_compaction() {
                self.compactify();
                Ok(())
            } else {
                Err(())
            }
        }
    }

    fn compactify(&mut self) {
        //eprintln!("{:?} compactify",self as *const Self);
        let mut buffer = [0u8; PAGE_SIZE];
        let fences_len = self.head.lower_fence.len as usize + self.head.upper_fence.len as usize;
        let new_data_offset = PAGE_SIZE - self.head.space_used as usize;
        let new_data_range = new_data_offset..PAGE_SIZE - fences_len;
        let mut write = &mut buffer[new_data_range.clone()];
        {
            let new_offset = (PAGE_SIZE - fences_len - write.len()) as u16;
            write.write_all(short_slice(self.as_bytes(), self.head.hash_area.offset, self.head.hash_area.len)).unwrap();
            self.head.hash_area.offset = new_offset;
        }
        for i in 0..self.head.count as usize {
            let new_offset = (PAGE_SIZE - fences_len - write.len()) as u16;
            debug_assert!(new_offset >= new_data_offset as u16);
            write.write_all(self.slots()[i].key(self.as_bytes()).0).unwrap();
            write.write_all(self.slots()[i].value(self.as_bytes())).unwrap();
            self.slots_mut()[i].offset = new_offset;
        }
        debug_assert!(write.is_empty());
        unsafe {
            self.as_bytes_mut()[new_data_range.clone()].copy_from_slice(&buffer[new_data_range])
        };
        self.head.data_offset = new_data_offset as u16;
    }

    #[cfg(feature = "hash_fx")]
    fn compute_hash(key: PrefixTruncatedKey) -> u8 {
        use std::hash::Hasher;
        use rustc_hash::FxHasher;
        let mut hasher = FxHasher::default();
        hasher.write(key.0);
        (hasher.finish() >> 56) as u8
    }

    #[cfg(feature = "hash_wyhash")]
    fn compute_hash(key: PrefixTruncatedKey) -> u8 {
        use std::hash::Hasher;
        let mut hasher = wyhash::WyHash::default();
        hasher.write(key.0);
        hasher.finish() as u8
    }

    #[cfg(feature = "hash_crc32")]
    fn compute_hash(key: PrefixTruncatedKey) -> u8 {
        crc32fast::hash(key.0) as u8
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
            self.request_space(self.space_needed_new_slot(key.0.len() + self.head.prefix_len as usize, payload.len()))?;
            self.increase_size(1);
            self.head.count as usize - 1
        };
        self.store_key_value(index, key, payload);
        // self.print();
        self.validate();
        Ok(())
    }

    fn hash_capacity(size: usize) -> usize {
        size.next_power_of_two()
    }

    fn increase_size(&mut self, delta: usize) {
        let count = self.head.count as usize;
        let old_hash_capacity = self.head.hash_area.len as usize;
        let new_size = self.head.count as usize + delta;
        if new_size > old_hash_capacity {
            let new_capacity = Self::hash_capacity(new_size);
            let old_hash_start = self.head.hash_area.offset as usize;
            let new_hash_start = self.head.data_offset as usize - new_capacity;
            self.head.data_offset = new_hash_start as u16;
            self.head.space_used += (new_capacity - old_hash_capacity) as u16;
            self.assert_no_collide();
            let old_count = self.head.count as usize;
            debug_assert!(old_hash_start > new_hash_start);
            unsafe {
                let (low, high) = self.as_bytes_mut().split_at_mut(old_hash_start);
                low[new_hash_start..][..count].copy_from_slice(&high[..old_count]);
            }
            self.head.hash_area = FenceKeySlot {
                offset: new_hash_start as u16,
                len: new_capacity as u16,
            };
        }
        self.head.count = new_size as u16;
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
        fences @ FenceData {
            lower_fence,
            upper_fence,
            prefix_len,
        }: FenceData,
    ) {
        fences.validate();
        self.head.prefix_len = prefix_len as u16;
        self.head.lower_fence = FenceKeySlot {
            offset: self.write_data(lower_fence.0),
            len: (lower_fence.0.len()) as u16,
        };
        self.head.upper_fence = FenceKeySlot {
            offset: self.write_data(upper_fence.0),
            len: (upper_fence.0.len()) as u16,
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

    pub fn fences(&self) -> FenceData {
        FenceData {
            lower_fence: FenceRef(short_slice(
                self.as_bytes(),
                self.head.lower_fence.offset,
                self.head.lower_fence.len,
            )),
            upper_fence: FenceRef(short_slice(
                self.as_bytes(),
                self.head.upper_fence.offset,
                self.head.upper_fence.len,
            )),
            prefix_len: self.head.prefix_len as usize,
        }
    }

    pub fn new() -> Self {
        assert_eq!(align_of::<Self>(), SIMD_ALIGN);
        HashLeaf {
            head: HashLeafHead {
                head: BTreeNodeHead { tag: BTreeNodeTag::HashLeaf, adaption_state: AdaptionState::new() },
                count: 0,
                sorted_count: 0,
                lower_fence: FenceKeySlot { offset: 0, len: 0 },
                upper_fence: FenceKeySlot { offset: 0, len: 0 },
                space_used: 0,
                data_offset: PAGE_SIZE as u16,
                prefix_len: 0,
                hash_area: FenceKeySlot {
                    offset: PAGE_SIZE as u16,
                    len: 0,
                },
            },
            data: [0u8; PAGE_SIZE - size_of::<HashLeafHead>()],
        }
    }

    fn find_index(&self, key: PrefixTruncatedKey) -> Option<usize> {
        let needle_hash = Self::compute_hash(key);
        //eprintln!("find {:?} -> {}",key,needle_hash);
        if USE_SIMD {
            debug_assert_eq!(self.find_simd(key, needle_hash), self.find_no_simd(key, needle_hash));
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
            let hash_offset_mod = self.head.hash_area.offset as usize % SIMD_ALIGN;
            let hash_offset_floor = self.head.hash_area.offset as usize - hash_offset_mod;
            let mut hash_ptr = (self as *const Self as *const u8).offset(hash_offset_floor as isize) as *const SimdDtype;
            let needle = SimdDtype::splat(needle_hash);
            debug_assert!(hash_ptr.is_aligned());
            let mut shift = hash_offset_mod;
            let mut matches = (*hash_ptr).simd_eq(needle).to_bitmask() >> shift;
            let shift_limit = shift + count;
            while shift < shift_limit {
                let trailing_zeros = matches.trailing_zeros();
                if trailing_zeros == SIMD_WIDTH as u32 {
                    shift = shift - shift % SIMD_WIDTH + SIMD_WIDTH;
                    hash_ptr = hash_ptr.offset(1);
                    matches = (*hash_ptr).simd_eq(needle).to_bitmask();
                } else {
                    shift += trailing_zeros as usize;
                    matches >>= trailing_zeros;
                    matches = matches & !1;
                    if shift >= shift_limit {
                        return None;
                    }
                    if self.slots()[shift - hash_offset_mod].key(self.as_bytes()) == key {
                        return Some(shift - hash_offset_mod);
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
            let average = (self.head.count as f32 / 256.0);
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
        self.fences().validate();
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
                + self.head.hash_area.len as usize
                + self
                .slots()
                .iter()
                .map(|s| (s.key_len + s.val_len) as usize)
                .sum::<usize>()
        );
        debug_assert!(self.head.sorted_count <= self.head.count);
        debug_assert!(self.slots()[..self.head.sorted_count as usize].is_sorted_by_key(|s| s.key(self.as_bytes())));
    }

    pub fn try_merge_right(&self, right: &mut Self, separator: FatTruncatedKey) -> Result<(), ()> {
        //eprintln!("### {:?} merge right {:?}",self as *const Self,right as *const Self);
        // self.print();
        // right.print();
        //TODO optimize
        let mut tmp = Self::new();
        tmp.set_fences(MergeFences::new(self.fences(), separator, right.fences()).fences());
        let left = self.slots().iter().map(|s| (s, &*self));
        let right_iter = right.slots().iter().map(|s| (s, &*right));
        for (s, this) in left.chain(right_iter) {
            let segments = &[&separator.remainder[..this.head.prefix_len as usize - separator.prefix_len], s.key(this.as_bytes()).0];
            let reconstructed = partial_restore(separator.prefix_len, segments, tmp.head.prefix_len as usize);
            tmp.insert_truncated(PrefixTruncatedKey(&reconstructed), s.value(this.as_bytes()))?;
        }
        tmp.head.sorted_count = self.head.sorted_count;
        tmp.validate();
        // tmp.print();
        debug_assert_eq!(tmp.head.count, self.head.count + right.head.count);
        *right = tmp;
        Ok(())
    }

    pub fn truncate<'a>(&self, key: &'a [u8]) -> PrefixTruncatedKey<'a> {
        PrefixTruncatedKey(&key[self.head.prefix_len as usize..])
    }

    fn sort(&mut self) {
        use std::mem::MaybeUninit;
        let unsorted_count = (self.head.count - self.head.sorted_count) as usize;
        if unsorted_count == 0 {
            return;
        }
        assert!(self.head.sorted_count <= self.head.count);
        let mut slots_space = MaybeUninit::<(HashSlot, u8)>::uninit_array::<{ PAGE_SIZE / size_of::<(HashSlot, u8)>() }>();
        for i in 0..unsorted_count {
            slots_space[i].write((self.slots()[self.head.sorted_count as usize + i], self.hashes()[self.head.sorted_count as usize + i]));
        }
        let unsorted_slots = unsafe { MaybeUninit::slice_assume_init_mut(&mut slots_space[..unsorted_count]) };
        unsorted_slots.sort_unstable_by_key(|s| s.0.key(self.as_bytes()));

        let mut unmerged_remaining = self.head.count as usize;
        let mut sorted_remaining = self.head.sorted_count as usize;
        let mut unsorted_remaining = unsorted_count;
        while sorted_remaining > 0 && unsorted_remaining > 0 {
            assert_eq!(unmerged_remaining, sorted_remaining + unsorted_remaining);
            if self.slots()[sorted_remaining - 1].key(self.as_bytes()) > unsorted_slots[unsorted_remaining - 1].0.key(self.as_bytes()) {
                self.slots_mut().copy_within(sorted_remaining - 1..=sorted_remaining - 1, unmerged_remaining - 1);
                self.hashes_mut().copy_within(sorted_remaining - 1..=sorted_remaining - 1, unmerged_remaining - 1);
                sorted_remaining -= 1;
                unmerged_remaining -= 1;
            } else {
                self.slots_mut()[unmerged_remaining - 1] = unsorted_slots[unsorted_remaining - 1].0;
                self.hashes_mut()[unmerged_remaining - 1] = unsorted_slots[unsorted_remaining - 1].1;
                unsorted_remaining -= 1;
                unmerged_remaining -= 1;
            }
        }
        while unsorted_remaining > 0 {
            self.slots_mut()[unmerged_remaining - 1] = unsorted_slots[unsorted_remaining - 1].0;
            self.hashes_mut()[unmerged_remaining - 1] = unsorted_slots[unsorted_remaining - 1].1;
            unsorted_remaining -= 1;
            unmerged_remaining -= 1;
        }
        self.head.sorted_count = self.head.count;
        self.validate();
    }

    pub fn lower_bound(&self, key: PrefixTruncatedKey) -> (usize, bool) {
        if self.head.count == 0 {
            return (0, false);
        }
        let search_result = self.slots().binary_search_by(|s| {
            s.key(self.as_bytes()).cmp(&key)
        });
        match search_result {
            Ok(index) | Err(index) => {
                debug_assert!(
                    index == self.slots().len() || key <= self.slots()[index].key(self.as_bytes())
                );
                debug_assert!(index == 0 || key > self.slots()[index - 1].key(self.as_bytes()));
                (index, search_result.is_ok())
            }
        }
    }
}

unsafe impl Node for HashLeaf {
    fn split_node(
        &mut self,
        parent: &mut dyn InnerNode,
        index_in_parent: usize,
        key_in_self: &[u8],
    ) -> Result<(), ()> {
        //TODO if prefix length does not change, hashes can be copied
        self.sort();

        // split
        let (sep_slot, truncated_sep_key) =
            find_separator(self.head.count as usize, true, |i: usize| {
                self.slots()[i].key(self.as_bytes())
            });
        let full_sep_key_len = truncated_sep_key.0.len() + self.head.prefix_len as usize;
        let parent_prefix_len = parent.request_space_for_child(full_sep_key_len)?;
        let node_left_raw;
        let node_left = unsafe {
            node_left_raw = BTreeNode::alloc();
            (*node_left_raw).hash_leaf = ManuallyDrop::new(Self::new());
            &mut (*node_left_raw).hash_leaf
        };

        let mut split_fences = SplitFences::new(self.fences(), truncated_sep_key, parent_prefix_len, self.prefix(key_in_self));
        node_left.set_fences(split_fences.lower());
        let mut node_right = Self::new();
        node_right.set_fences(split_fences.upper());
        unsafe {
            if let Err(()) = parent.insert_child(index_in_parent, split_fences.separator(), node_left_raw) {
                BTreeNode::dealloc(node_left_raw);
                return Err(());
            }
        }
        self.copy_key_value_range(&self.slots()[..=sep_slot], node_left);
        self.copy_key_value_range(&self.slots()[sep_slot + 1..], &mut node_right);
        node_left.head.sorted_count = node_left.head.count;
        node_right.head.sorted_count = node_right.head.count;
        node_left.validate();
        node_right.validate();
        // node_left.print();
        // node_right.print();
        debug_assert_eq!(self.head.count, node_left.head.count + node_right.head.count);
        *self = node_right;
        Ok(())
    }


    fn is_underfull(&self) -> bool {
        self.free_space_after_compaction() >= PAGE_SIZE * 3 / 4
    }

    fn print(&self) {
        eprintln!("HashLeaf {:?}: {:?}", self as *const Self, self.fences());
        for (i, s) in self.slots().iter().enumerate() {
            eprintln!("{:?}|{:3?}|{:3?}", i, self.hashes()[i], s.key(self.as_bytes()));
        }
    }

    fn validate_tree(&self, lower: &[u8], upper: &[u8]) {
        debug_assert_eq!(self.fences(), FenceData {
            prefix_len: 0,
            lower_fence: FenceRef(lower),
            upper_fence: FenceRef(upper),
        }.restrip());
    }
}

unsafe impl LeafNode for HashLeaf {
    fn insert(&mut self, key: &[u8], payload: &[u8]) -> Result<(), ()> {
        // self.print();
        //eprintln!("{:?} insert {:?}",self as *const Self,key);
        let key = self.truncate(key);
        self.insert_truncated(key, payload)
    }


    fn lookup(&mut self, key: &[u8]) -> Option<&mut [u8]> {
        self.validate();
        self.find_index(self.truncate(key)).map(|i| {
            let slot = self.slots()[i];
            unsafe {
                &mut self.as_bytes_mut()[(slot.offset + slot.key_len) as usize..][..slot.val_len as usize]
            }
        })
    }


    fn remove(&mut self, key: &[u8]) -> Option<()> {
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
        if self.head.sorted_count > index as u16 {
            self.head.sorted_count = index as u16;
        }
        self.head.count -= 1;
        self.validate();
        // self.print();
        Some(())
    }


    unsafe fn range_lookup(&mut self, start: &[u8], key_out: *mut u8, callback: &mut dyn FnMut(usize, &[u8]) -> bool) -> bool {
        self.sort();
        debug_assert!(!key_out.is_null());
        key_out.copy_from_nonoverlapping(start.as_ptr(), self.head.prefix_len as usize);
        let start_index = self.lower_bound(self.truncate(start)).0;
        for s in &self.slots()[start_index..] {
            let k = s.key(self.as_bytes());
            key_out.offset(self.head.prefix_len as isize).copy_from_nonoverlapping(k.0.as_ptr(), k.0.len());
            if !callback((s.key_len + self.head.prefix_len) as usize, s.value(self.as_bytes())) {
                return false;
            }
        }
        true
    }

    unsafe fn range_lookup_desc(&mut self, start: &[u8], key_out: *mut u8, callback: &mut dyn FnMut(usize, &[u8]) -> bool) -> bool {
        self.sort();
        debug_assert!(!key_out.is_null());
        key_out.copy_from_nonoverlapping(start.as_ptr(), self.head.prefix_len as usize);
        let start_index = self.lower_bound(self.truncate(start)).0.min(self.head.count as usize - 1);
        for s in self.slots()[..=start_index].iter().rev() {
            let k = s.key(self.as_bytes());
            key_out.offset(self.head.prefix_len as isize).copy_from_nonoverlapping(k.0.as_ptr(), k.0.len());
            if !callback((s.key_len + self.head.prefix_len) as usize, s.value(self.as_bytes())) {
                return false;
            }
        }
        true
    }
}
