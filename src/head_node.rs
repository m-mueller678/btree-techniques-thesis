use crate::basic_node::BasicNode;
use crate::find_separator::{find_separator, KeyRef};
use crate::inner_node::{FenceData, InnerConversionSink, InnerConversionSource, InnerNode, merge, Node, SeparableInnerConversionSource, split_in_place};
use crate::util::{
    common_prefix_len, get_key_from_slice, partial_restore, reinterpret_mut, SmallBuff,
};
use crate::{BTreeNode, FatTruncatedKey, PAGE_SIZE, PrefixTruncatedKey};
use smallvec::{SmallVec, ToSmallVec};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{align_of, size_of, transmute};
use std::{mem, ptr};
use std::ops::Range;
use bytemuck::{bytes_of, bytes_of_mut, Pod};
use crate::vtables::BTreeNodeTag;

pub type U64HeadNode = HeadNode<ExplicitLengthHead<u64>>;
pub type U32HeadNode = HeadNode<ExplicitLengthHead<u32>>;

#[cfg(feature = "head-early-abort-create_true")]
const HEAD_EARLY_ABORT_CREATE: bool = true;
#[cfg(feature = "head-early-abort-create_false")]
const HEAD_EARLY_ABORT_CREATE: bool = false;

pub trait FullKeyHeadNoTag: Ord + Sized + Copy + KeyRef<'static> + Debug + 'static {
    const HINT_COUNT: usize;
    const MAX_LEN: usize;

    fn make_fence_head(key: PrefixTruncatedKey) -> Option<Self>;
    fn make_needle_head(key: PrefixTruncatedKey) -> Self;
    fn restore(self) -> SmallVec<[u8; 16]>;
    fn strip_prefix(self, prefix_len: usize) -> Self {
        let mut v = self.restore();
        v.drain(..prefix_len);
        Self::make_fence_head(PrefixTruncatedKey(&v)).unwrap()
    }
}

pub trait FullKeyHead: FullKeyHeadNoTag {
    const TAG: BTreeNodeTag;
}

/// must be fixed size unsigned integer, like u8-u128
pub unsafe trait UnsignedInt: Debug + Copy + Ord + Pod + 'static {
    const BYTE_LEN: usize;
    #[must_use]
    fn swap_big_native_endian(self) -> Self;
    #[must_use]
    fn inc(self) -> Self;
}

unsafe impl UnsignedInt for u64 {
    const BYTE_LEN: usize = 8;

    fn swap_big_native_endian(self) -> Self {
        self.to_be()
    }

    fn inc(self) -> Self {
        self.saturating_add(1)
    }
}

unsafe impl UnsignedInt for u32 {
    const BYTE_LEN: usize = 4;

    fn swap_big_native_endian(self) -> Self {
        self.to_be()
    }

    fn inc(self) -> Self {
        self.saturating_add(1)
    }
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
pub struct AsciiHead(u64);

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
pub struct ExplicitLengthHead<T: UnsignedInt>(T);

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
pub struct ZeroPaddedHead<T: UnsignedInt>(T);

impl<'a> KeyRef<'a> for AsciiHead {
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

impl FullKeyHeadNoTag for AsciiHead {
    const HINT_COUNT: usize = 16;
    const MAX_LEN: usize = 9;

    fn make_fence_head(key: PrefixTruncatedKey) -> Option<Self> {
        if key.len() > 9 {
            return None;
        }
        let mut out: u64 = 0;
        for i in 0..9 {
            out <<= 7;
            if i < key.0.len() {
                // 0x7f is invalid because we shift one up to allow 0x00
                if key.0[i] >= 0x7f {
                    return None;
                }
                out += key[i] as u64 + 1;
            }
        }
        out <<= 1;
        Some(AsciiHead(out))
    }

    fn make_needle_head(key: PrefixTruncatedKey) -> Self {
        let mut out: u64 = 0;
        let mut ceil = false;
        for i in 0..9 {
            out <<= 7;
            if i < key.0.len() {
                // 0x7f is invalid because we shift one up to allow 0x00
                if key.0[i] >= 0x7f {
                    ceil = true
                }
                if !ceil {
                    debug_assert!(key.0[i] < 127);
                    out += key.0[i] as u64 + 1;
                }
            }
            if ceil {
                out += 127;
            }
        }
        out <<= 1;
        if ceil || key.0.len() > 9 {
            out += 1;
        }
        AsciiHead(out)
    }

    fn restore(self) -> SmallVec<[u8; 16]> {
        let mut v = SmallVec::new();
        let mut x = self.0;
        debug_assert!(x % 2 == 0);
        for _ in 0..9 {
            let byte = (x >> (64 - 7) & 127) as u8;
            if byte == 0 {
                break;
            }
            v.push(byte - 1);
            x <<= 7;
        }
        v
    }
}

impl<T: UnsignedInt> FullKeyHeadNoTag for ExplicitLengthHead<T> {
    const HINT_COUNT: usize = 16;
    const MAX_LEN: usize = T::BYTE_LEN - 1;

    fn make_fence_head(key: PrefixTruncatedKey) -> Option<Self> {
        let mut ret = T::zeroed();
        let bytes = bytes_of_mut(&mut ret);
        debug_assert!(key.0.len() > 0);
        let (len, data_area) = bytes.split_last_mut().unwrap();
        if key.0.len() <= data_area.len() {
            data_area[..key.0.len()].copy_from_slice(key.0);
            *len = key.0.len() as u8;
            Some(ExplicitLengthHead(ret.swap_big_native_endian()))
        } else {
            None
        }
    }

    fn make_needle_head(key: PrefixTruncatedKey) -> Self {
        let mut ret = T::zeroed();
        let bytes = bytes_of_mut(&mut ret);
        let (len, data_area) = bytes.split_last_mut().unwrap();
        if key.0.len() <= data_area.len() {
            data_area[..key.0.len()].copy_from_slice(key.0);
            *len = key.0.len() as u8;
        } else {
            data_area.copy_from_slice(&key.0[..data_area.len()]);
            *len = 8;
        }
        ExplicitLengthHead(ret.swap_big_native_endian())
    }

    fn restore(self) -> SmallVec<[u8; 16]> {
        let mut v = bytes_of(&self.0.swap_big_native_endian()).to_smallvec();
        let len = *v.last().unwrap() as usize;
        debug_assert!(len < size_of::<Self>(), "this was created from a needle key");
        v.truncate(len);
        v
    }
}

impl<T: UnsignedInt> FullKeyHeadNoTag for ZeroPaddedHead<T> {
    const HINT_COUNT: usize = 16;
    const MAX_LEN: usize = T::BYTE_LEN;

    fn make_fence_head(key: PrefixTruncatedKey) -> Option<Self> {
        let mut ret = T::zeroed();
        let bytes = bytes_of_mut(&mut ret);
        debug_assert!(key.0.len() > 0);
        if key.0.len() <= bytes.len() {
            bytes[..key.0.len()].copy_from_slice(key.0);
            if bytes[key.0.len() - 1] == 0 {
                return None; // collides with shorter keys
            }
            if bytes.iter().all(|&x| x == 255) {
                return None;
            }
            Some(ZeroPaddedHead(ret.swap_big_native_endian()))
        } else {
            None
        }
    }

    fn make_needle_head(key: PrefixTruncatedKey) -> Self {
        let mut ret = T::zeroed();
        let bytes = bytes_of_mut(&mut ret);
        if key.0.len() <= bytes.len() {
            bytes[..key.0.len()].copy_from_slice(key.0);
            if key.0.len() > 0 && bytes[key.0.len() - 1] == 0 {
                // u32::from_be_bytes(bytes) represents a prefix of self, this key must come after that prefix
                ZeroPaddedHead(ret.swap_big_native_endian().inc())
            } else {
                ZeroPaddedHead(ret.swap_big_native_endian())
            }
        } else {
            bytes.copy_from_slice(&key.0[..bytes.len()]);
            // u32::from_be_bytes(bytes) represents a prefix of self, this key must come after that prefix
            ZeroPaddedHead(ret.swap_big_native_endian().inc())
        }
    }

    fn restore(self) -> SmallVec<[u8; 16]> {
        let mut v = bytes_of(&self.0.swap_big_native_endian()).to_smallvec();
        while v.last().copied() == Some(0) {
            v.pop();
        }
        v
    }
}

impl<T: UnsignedInt> KeyRef<'static> for ExplicitLengthHead<T> {
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


impl<T: UnsignedInt> KeyRef<'static> for ZeroPaddedHead<T> {
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

impl FullKeyHead for ExplicitLengthHead<u64> {
    const TAG: BTreeNodeTag = BTreeNodeTag::U64HeadNode;
}

impl FullKeyHead for ExplicitLengthHead<u32> {
    const TAG: BTreeNodeTag = BTreeNodeTag::U32HeadNode;
}

impl FullKeyHead for ZeroPaddedHead<u64> {
    const TAG: BTreeNodeTag = unimplemented!();
}

impl FullKeyHead for ZeroPaddedHead<u32> {
    const TAG: BTreeNodeTag = unimplemented!();
}


impl FullKeyHead for AsciiHead {
    const TAG: BTreeNodeTag = unimplemented!();
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
    pub fn new(fences: FenceData, upper: *mut BTreeNode) -> Self {
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

    fn set_fences(&mut self, fences: FenceData) {
        debug_assert!(
            fences.lower_fence < fences.upper_fence
                || fences.upper_fence.0.is_empty() && self.head.prefix_len == 0
        );
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

    const KEY_OFFSET: usize = { Self::HINT_OFFSET + Head::HINT_COUNT * size_of::<Head>() };

    const HINT_OFFSET: usize = {
        let key_align = align_of::<Head>();
        size_of::<HeadNodeHead>().next_multiple_of(key_align)
    };

    pub fn find_child_for_key(&self, key: &[u8]) -> usize {
        if self.head.key_count == 0 {
            return 0;
        }
        let needle_head =
            Head::make_needle_head(PrefixTruncatedKey(&key[self.head.prefix_len as usize..]));
        let (lower, upper) = self.search_hint(needle_head);
        match self.as_parts().1[lower..upper].binary_search(&needle_head) {
            Ok(i) | Err(i) => lower + i,
        }
    }

    fn as_parts_mut(
        &mut self,
    ) -> (
        &mut HeadNodeHead,
        &mut [Head],
        &mut [*mut BTreeNode],
        &mut [Head],
    ) {
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
            let hints = (self as *const Self as *const u8).offset(Self::HINT_OFFSET as isize)
                as *const Head;
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
        parent: &mut dyn InnerNode,
        index_in_parent: usize,
        key_in_node: &[u8],
    ) -> Result<(), ()> {
        split_in_place::<Self, Self, Self>(
            unsafe { reinterpret_mut(self) },
            parent,
            index_in_parent,
            key_in_node,
        )
    }

    pub fn prefix<'a>(&self, src: &'a [u8]) -> &'a [u8] {
        &src[..self.head.prefix_len as usize]
    }

    pub fn validate_tree(&self, lower: &[u8], upper: &[u8]) {
        debug_assert_eq!(
            self.head.prefix_len as usize,
            common_prefix_len(lower, upper)
        );
        debug_assert_eq!(
            self.fences().lower_fence.0,
            &lower[self.head.prefix_len as usize..]
        );
        debug_assert_eq!(
            self.fences().upper_fence.0,
            &upper[self.head.prefix_len as usize..]
        );
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
        dst: &mut BasicNode,
        slot: usize,
        key: PrefixTruncatedKey,
        child: *mut BTreeNode,
    ) -> Result<(), ()> {
        let prefix_len = dst.fences().prefix_len;
        dst.request_space(dst.space_needed(key.len() + prefix_len, size_of::<*mut BTreeNode>()))?;
        dst.raw_insert(slot, key, &(child as usize).to_ne_bytes());
        Ok(())
    }

    pub fn try_from_any(this: &mut BTreeNode) -> Result<(), ()> {
        let mut tmp = unsafe { BTreeNode::new_uninit() };
        Self::create(&mut tmp, this.to_inner())?;
        unsafe {
            ptr::write(this, tmp);
        }
        Ok(())
    }

    pub fn remove_slot(&mut self, index: usize) {
        let (head, keys, children, _) = self.as_parts_mut();
        keys.copy_within(index + 1..head.key_count as usize, index);
        children.copy_within(index + 1..head.key_count as usize + 1, index);
        head.key_count -= 1;
        self.update_hint(0);
    }

    #[tracing::instrument(skip(self, right_any))]
    pub fn merge_right(
        &mut self,
        right_any: &mut BTreeNode,
        separator: FatTruncatedKey,
    ) -> Result<(), ()> {
        unsafe {
            let mut tmp = BTreeNode::new_uninit();
            merge::<Self, dyn InnerNode, dyn InnerNode>(
                &mut tmp,
                self,
                right_any.to_inner(),
                separator,
            )?;
            ptr::write(right_any, tmp);
        }
        return Ok(());
    }
}

unsafe impl<Head: FullKeyHead> InnerConversionSink for HeadNode<Head> {
    fn create(dst: &mut BTreeNode, src: &(impl InnerConversionSource + ?Sized)) -> Result<(), ()> {
        let len = src.key_count();
        if HEAD_EARLY_ABORT_CREATE && src.get_key_length_max(0..len) > Head::MAX_LEN {
            return Err(());
        }
        let fences = src.fences();
        let this = dst.write_inner(Self::from_fences(fences));
        if (this.head.key_capacity as usize) < len {
            return Err(());
        }
        this.head.key_count = len as u16;
        let (_, keys, children, _) = this.as_parts_mut();
        debug_assert!(size_of::<Head>() <= 8);
        let mut buffer = [0u8; 16];
        for i in 0..len {
            let key_len = src.get_key(i, buffer.as_mut_slice(), 0)?;
            keys[i] = Head::make_fence_head(PrefixTruncatedKey(&buffer[buffer.len() - key_len..]))
                .ok_or(())?;
        }
        for i in 0..len + 1 {
            children[i] = src.get_child(i);
        }
        this.update_hint(0);
        Ok(())
    }
}

impl<Head: FullKeyHead> InnerConversionSource for HeadNode<Head> {
    fn fences(&self) -> FenceData {
        FenceData {
            lower_fence: PrefixTruncatedKey(
                &self.as_bytes()
                    [self.head.lower_fence_offset as usize..self.head.upper_fence_offset as usize],
            ),
            upper_fence: PrefixTruncatedKey(
                &self.as_bytes()[self.head.upper_fence_offset as usize..],
            ),
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

    fn get_key_length_sum(&self, range: Range<usize>) -> usize {
        debug_assert!(range.end <= self.key_count());
        self.as_parts().1[range].iter().map(|k| k.len()).sum()
    }

    fn get_key_length_max(&self, range: Range<usize>) -> usize {
        debug_assert!(range.end <= self.key_count());
        self.as_parts().1[range].iter().map(|k| k.len()).max().unwrap_or(0)
    }
}

unsafe impl<Head: FullKeyHead> Node for HeadNode<Head> {
    fn is_underfull(&self) -> bool {
        self.head.key_count * 4 <= self.head.key_capacity
    }

    #[cfg(debug_assertions)]
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
}

impl<Head: FullKeyHead> SeparableInnerConversionSource for HeadNode<Head> {
    type Separator<'a> = SmallVec<[u8; 16]>;

    fn find_separator<'a>(&'a self) -> (usize, Self::Separator<'a>) {
        let (sep_slot, truncated_sep_key) =
            find_separator(self.head.key_count as usize, false, |i| {
                self.as_parts().1[i]
            });
        let truncated_sep_key = truncated_sep_key.restore();
        (sep_slot, truncated_sep_key)
    }
}

impl<Head: FullKeyHead> InnerNode for HeadNode<Head> {
    fn merge_children_check(&mut self, mut child_index: usize) -> Result<(), ()> {
        debug_assert!(child_index < self.head.key_count as usize + 1);
        debug_assert!(unsafe { (&*self.get_child(child_index)).is_underfull() });
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

    /// may change node type
    /// if Err is returned, node must be split
    unsafe fn insert_child(
        &mut self,
        index: usize,
        key: PrefixTruncatedKey,
        child: *mut BTreeNode,
    ) -> Result<(), ()> {
        debug_assert!(
            key <= self.fences().upper_fence
                || self.fences().upper_fence.0.is_empty() && self.fences().prefix_len == 0
        );
        debug_assert!(
            key > self.fences().lower_fence
                || self.fences().lower_fence.0.is_empty() && self.fences().prefix_len == 0
        );
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
            let mut tmp = BTreeNode::new_uninit();
            BasicNode::create(&mut tmp, self)?;
            let self_ptr = self as *mut Self as *mut BTreeNode;
            unsafe {
                ptr::write(self_ptr, tmp);
                Self::try_insert_to_basic(&mut *(self_ptr as *mut BasicNode), index, key, child)
            }
        }
    }

    fn request_space_for_child(&mut self, _key_length: usize) -> Result<usize, ()> {
        if self.head.key_count < self.head.key_capacity {
            Ok(self.head.prefix_len as usize)
        } else {
            Err(())
        }
    }
}