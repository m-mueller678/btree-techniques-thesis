use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::mem::{align_of, size_of};
use std::ops::Range;
use std::ptr;
use smallvec::SmallVec;
use crate::{BTreeNode, PAGE_SIZE, PrefixTruncatedKey};
use crate::branch_cache::BranchCacheAccessor;
use crate::find_separator::find_separator;
use crate::node_traits::{FenceData, FenceRef, InnerConversionSink, InnerConversionSource, InnerInsertSource, InnerNode, Node, SeparableInnerConversionSource, split_in_place};
use crate::util::{common_prefix_len, get_key_from_slice, partial_restore, reinterpret, reinterpret_mut, SmallBuff};
use crate::vtables::BTreeNodeTag;

/// implementation incomplete.
/// work paused to focus on other aspects.
#[repr(C)]
pub struct ArtNode {
    head: ArtNodeHead,
    data: ArtNodeData,
}

union ArtNodeData {
    range_array: [u16; (PAGE_SIZE - size_of::<ArtNodeHead>()) / 2],
    _bytes: [u8; PAGE_SIZE - size_of::<ArtNodeHead>()],
}

#[derive(Debug)]
#[repr(C)]
struct ArtNodeHead {
    tag: BTreeNodeTag,
    key_count: u16,
    data_write: u16,
    range_array_len: u16,
    upper: *mut BTreeNode,
    upper_fence: u16,
    lower_fence: u16,
    prefix_len: u16,
    root_node: u16,
}

struct LayoutInfo {
    range_array: usize,
    page_indirection_vector: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct PageIndirectionVectorEntry {
    key_offset: u16,
    key_len: u16,
}

impl PageIndirectionVectorEntry {
    fn key<'a>(&self, page: &'a ArtNode) -> PrefixTruncatedKey<'a> {
        unsafe {
            PrefixTruncatedKey(&reinterpret::<ArtNode, [u8; PAGE_SIZE]>(page)[self.key_offset as usize..][..self.key_len as usize])
        }
    }
}

const NODE_REF_IS_RANGE: u16 = 1 << 15;
const NODE_TAG_DECISION: u16 = 0xa3cf;
const NODE_TAG_SPAN: u16 = 0x1335;

const MAX_CHILDREN: usize = 4;

impl ArtNode {
    #[inline(always)]
    fn layout(range_array_len: usize) -> LayoutInfo {
        let range_array = size_of::<ArtNodeHead>().next_multiple_of(align_of::<u16>());
        let page_indirection_vector = (range_array + range_array_len * size_of::<u16>()).next_multiple_of(align_of::<PageIndirectionVectorEntry>());
        LayoutInfo { range_array, page_indirection_vector }
    }

    /// returns Ok for decision nodes, Err(successor) for span nodes
    #[inline(always)]
    unsafe fn read_node(&self, offset: u16) -> (&[u8], Result<&[u16], u16>) {
        debug_assert_eq!(offset & NODE_REF_IS_RANGE, 0);
        let bytes = reinterpret::<Self, [u8; PAGE_SIZE]>(self).as_slice();
        let node_ptr = unsafe { bytes.as_ptr().offset(offset as isize) as *const u16 };
        debug_assert!(node_ptr.is_aligned());
        let node_tag = *node_ptr;
        let key_count = *node_ptr.offset(1) as usize;
        let node_bytes = &bytes[offset as usize + 4..][..key_count];
        debug_assert!(node_tag == NODE_TAG_DECISION || node_tag == NODE_TAG_SPAN);
        let extra = if node_tag == NODE_TAG_DECISION {
            Ok(std::slice::from_raw_parts(node_ptr.offset(2 + key_count.next_multiple_of(2) as isize / 2), key_count + 1))
        } else {
            Err(offset + 4 + key_count.next_multiple_of(2) as u16)
        };
        (node_bytes, extra)
    }

    fn construct<F: Fn(&Self, usize) -> PrefixTruncatedKey>(&mut self, keys: &F, key_range: Range<usize>, prefix_len: usize) -> Result<u16, ()> {
        let original_start = key_range.start;
        if key_range.len() < 3 {
            return Ok(self.push_range_array_entry(key_range)? | NODE_REF_IS_RANGE);
        }
        let full_prefix = prefix_len + common_prefix_len(&keys(self, key_range.start).0[prefix_len..], &keys(self, key_range.end - 1).0[prefix_len..]);
        let ret = if full_prefix > prefix_len {
            self.push_range_array_entry(original_start..key_range.start)?;
            self.construct_inner_decision_node::<F>(&keys, key_range.clone(), full_prefix)?;
            self.push_range_array_entry(key_range.end..key_range.end)?;
            let span_len = full_prefix - prefix_len;
            self.set_heap_write_pos_mod_2(span_len as u16)?;
            unsafe {
                // key_ptr may point into self, but will never overlap destination
                let src = &keys(self, key_range.start)[prefix_len..full_prefix];
                let src_ptr = src.as_ptr();
                let src_len = src.len();
                let offset = self.heap_alloc(src_len)?;
                std::ptr::copy_nonoverlapping(src_ptr, (self as *mut Self as *mut u8).offset(offset as isize), src_len);
            }
            self.assert_heap_write_aligned();
            self.heap_write((span_len as u16).to_ne_bytes().as_slice())?;
            Ok(self.heap_write(NODE_TAG_SPAN.to_ne_bytes().as_slice())? as u16)
        } else {
            self.construct_inner_decision_node::<F>(&keys, key_range, full_prefix)
        };
        ret
    }

    unsafe fn node_min(&self, node: u16) -> u16 {
        if (node & NODE_REF_IS_RANGE) != 0 {
            return node & !NODE_REF_IS_RANGE;
        }
        let (_node_bytes, extra) = self.read_node(node);
        match extra {
            Ok(children) => {
                self.node_min(children[0])
            }
            Err(child_offset) => {
                self.node_min(child_offset) - 1
            }
        }
    }

    unsafe fn node_max(&self, node: u16) -> u16 {
        if (node & NODE_REF_IS_RANGE) != 0 {
            return node & !NODE_REF_IS_RANGE;
        }
        let (_node_bytes, extra) = self.read_node(node);
        match extra {
            Ok(children) => {
                self.node_max(*children.last().unwrap())
            }
            Err(child_offset) => {
                self.node_max(child_offset) + 1
            }
        }
    }

    unsafe fn find_key_range_unchecked(&self, key: &[u8], node: u16) -> (u16, usize) {
        if (node & NODE_REF_IS_RANGE) != 0 {
            return (node & !NODE_REF_IS_RANGE, key.len());
        }
        let (node_bytes, extra) = self.read_node(node);
        match extra {
            Ok(children) => {
                let child_index = if key.is_empty() {
                    0
                } else {
                    node_bytes.iter().position(|&k| key[0] < k).unwrap_or(node_bytes.len())
                };
                self.find_key_range_unchecked(key, children[child_index])
            }
            Err(successor) => {
                if node_bytes.len() <= key.len() {
                    match key[..node_bytes.len()].cmp(node_bytes) {
                        Ordering::Equal => self.find_key_range_unchecked(&key[node_bytes.len()..], successor),
                        Ordering::Less => (self.node_min(node), key.len()),
                        Ordering::Greater => (self.node_max(node), key.len()),
                    }
                } else {
                    match key.cmp(node_bytes) {
                        Ordering::Equal => unreachable!(),
                        Ordering::Less => (self.node_min(node), key.len()),
                        Ordering::Greater => (self.node_max(node), key.len()),
                    }
                }
            }
        }
    }

    fn partition<F: Fn(usize) -> Option<u8>>(keys: &F, key_range: Range<usize>) -> SmallVec<[u16; MAX_CHILDREN - 1]> {
        let mut splits = SmallVec::new();
        let mut range_start = key_range.start;
        for i in range_start + 1..key_range.end {
            if (i == range_start + 1 && keys(i - 1).is_none())
                || keys(i - 1).unwrap() != keys(i).unwrap() {
                splits.push(i as u16);
                range_start = i;
            }
        }
        splits
    }

    // TODO: how to handle [[],[0,...],[0,...],...]?
    //       0 key cannot split -> cannot create span node in next step
    //       decision four cases: less, equal, greater, empty?
    //       span four cases: prefix less, prefix greater, same as prefix, prefix is prefix of key
    fn construct_inner_decision_node<F: Fn(&Self, usize) -> PrefixTruncatedKey>(&mut self, keys: &F, key_range: Range<usize>, prefix_len: usize) -> Result<u16, ()> {
        let mut children = SmallVec::<[u16; MAX_CHILDREN]>::new();
        {
            let mut range_start = key_range.start;
            for i in Self::partition(&|i| keys(self, i).get(prefix_len).copied(), key_range.clone()).iter().map(|x| *x as usize) {
                children.push(self.construct::<F>(keys, range_start..i, prefix_len)?);
                range_start = i;
            }
            children.push(self.construct::<F>(keys, range_start..key_range.end, prefix_len)?);
        }
        let key_count = children.len() - 1;
        let key_array_size = key_count.next_multiple_of(2);
        self.set_heap_write_pos_mod_2(0)?;
        let pos = self.heap_alloc(size_of::<u16>() * 2 + key_array_size + size_of::<u16>() * children.len())? as usize;
        unsafe {
            self.write_to(pos, NODE_TAG_DECISION.to_ne_bytes().as_slice());
            self.write_to(pos + 2, (key_count as u16).to_ne_bytes().as_slice());
            {
                let range_start = key_range.start;
                let mut next_write = 0;
                for i in range_start + 1..key_range.end {
                    if
                    (i == range_start + 1 && keys(self, i - 1).len() == prefix_len)
                        || keys(self, i - 1)[prefix_len] != keys(self, i)[prefix_len] {
                        reinterpret_mut::<Self, [u8; PAGE_SIZE]>(self)[pos + 4 + next_write] = keys(self, i)[prefix_len];
                        next_write += 1;
                    }
                }
            }
            self.write_to(pos + 4 + key_array_size, bytemuck::cast_slice::<u16, u8>(children.as_slice()));
        }
        debug_assert!(pos % 2 == 0);
        Ok(pos as u16)
    }

    fn heap_write(&mut self, data: &[u8]) -> Result<usize, ()> {
        unsafe {
            let pos = self.heap_alloc(data.len())?;
            self.write_to(pos, data);
            Ok(pos)
        }
    }

    unsafe fn write_to(&mut self, offset: usize, data: &[u8]) {
        reinterpret_mut::<Self, [u8; PAGE_SIZE]>(self)[offset..][..data.len()].copy_from_slice(data);
    }

    fn assert_heap_write_aligned(&self) {
        debug_assert_eq!(self.head.data_write % 2, 0)
    }

    fn set_heap_write_pos_mod_2(&mut self, m: u16) -> Result<usize, ()> {
        if self.head.data_write % 2 != m % 2 {
            self.heap_alloc(1)
        } else {
            Ok(self.head.data_write as usize)
        }
    }

    fn free_space(&self) -> usize {
        self.head.data_write as usize - (Self::layout(self.head.range_array_len as usize).page_indirection_vector + size_of::<PageIndirectionVectorEntry>() * self.head.key_count as usize)
    }

    fn heap_alloc(&mut self, len: usize) -> Result<usize, ()> {
        if self.free_space() < len {
            Err(())
        } else {
            self.head.data_write -= len as u16;
            Ok(self.head.data_write as usize)
        }
    }

    /// overwrites page indirection vector
    fn push_range_array_entry(&mut self, range: Range<usize>) -> Result<u16, ()> {
        if self.free_space() < 2 {
            return Err(());
        } else {
            let pos = self.head.range_array_len;
            unsafe {
                debug_assert!(pos == 0 && range == (0..0) || range.start == self.data.range_array[pos as usize - 1] as usize);
                self.data.range_array[pos as usize] = range.end as u16;
            }
            self.head.range_array_len += 1;
            Ok(pos)
        }
    }

    fn range_array(&self) -> &[u16] {
        unsafe {
            let offset = Self::layout(self.head.range_array_len as usize).range_array;
            let ptr = reinterpret::<Self, [u8; PAGE_SIZE]>(self).as_ptr().offset(offset as isize) as *const u16;
            std::slice::from_raw_parts(ptr, self.head.range_array_len as usize)
        }
    }

    fn piv_entry(&self, index: usize) -> &PageIndirectionVectorEntry {
        debug_assert!(index < self.head.key_count as usize);
        unsafe {
            &*((self as *const Self as *const u8).offset(Self::layout(self.head.range_array_len as usize).page_indirection_vector as isize) as *const PageIndirectionVectorEntry)
                .offset(index as isize)
        }
    }

    fn page_indirection_vector(&self) -> &[PageIndirectionVectorEntry] {
        unsafe {
            let ptr = (self as *const Self as *const u8).offset(Self::layout(self.head.range_array_len as usize).page_indirection_vector as isize) as *const PageIndirectionVectorEntry;
            std::slice::from_raw_parts(ptr, self.key_count())
        }
    }
}

struct NodeDebugWrapper<'a> {
    page: &'a ArtNode,
    offset: u16,
}

impl Debug for NodeDebugWrapper<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.offset & NODE_REF_IS_RANGE != 0 {
            return write!(f, "RangeIndex({})", (self.offset & !NODE_REF_IS_RANGE));
        }
        match unsafe { self.page.read_node(self.offset) } {
            (keys, Ok(children)) => {
                let mut s = f.debug_struct("Decision");
                s.field("_", &NodeDebugWrapper { offset: children[0], page: self.page });
                for i in 0..keys.len() {
                    s.field(&format!("{:?}", keys[i]), &NodeDebugWrapper { offset: children[i + 1], page: self.page });
                }
                s.finish()
            }
            (span, Err(child)) => {
                let mut s = f.debug_struct("Span");
                s.field("span", &span);
                s.field("child", &NodeDebugWrapper { offset: child, page: self.page });
                s.finish()
            }
        }
    }
}

impl Debug for ArtNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("ArtNode");
        s.field("head", &self.head);
        s.field("range_array", &self.range_array());
        s.field("tree", &NodeDebugWrapper {
            page: self,
            offset: self.head.root_node,
        });
        s.finish()
    }
}

unsafe impl Node for ArtNode {
    fn is_underfull(&self) -> bool {
        self.free_space() > PAGE_SIZE * 3 / 4
    }

    fn print(&self) {
        println!("{:#?}", self)
    }

    fn validate_tree(&self, lower: &[u8], upper: &[u8]) {
        debug_assert_eq!(self.fences(), FenceData {
            prefix_len: 0,
            lower_fence: FenceRef(lower),
            upper_fence: FenceRef(upper),
        }.restrip());
        let mut current_lower: SmallBuff = lower.into();
        for (i, e) in self.page_indirection_vector().iter().enumerate() {
            let current_upper = partial_restore(0, &[&lower[..self.head.prefix_len as usize], e.key(self).0], 0);
            unsafe { &mut *self.get_child(i) }.validate_tree(&current_lower, &current_upper);
            current_lower = current_upper;
        }
        unsafe { &mut *self.get_child(self.head.key_count as usize) }.validate_tree(&current_lower, upper);
    }

    fn split_node(&mut self, parent: &mut dyn InnerNode, index_in_parent: usize, key_in_node: &[u8]) -> Result<(), ()> {
        unsafe {
            split_in_place::<Self, Self, Self>(reinterpret_mut::<Self, BTreeNode>(self), parent, index_in_parent, key_in_node)
        }
    }
}

impl InnerNode for ArtNode {
    fn merge_children_check(&mut self, _child_index: usize) -> Result<(), ()> {
        //TODO
        return Err(());
    }

    unsafe fn insert_child(&mut self, index: usize, key: PrefixTruncatedKey, child: *mut BTreeNode) -> Result<(), ()> {
        let mut tmp = BTreeNode::new_uninit();

        Self::create(&mut tmp, &InnerInsertSource::new(self, index, key, child))?;
        unsafe {
            ptr::copy_nonoverlapping(&tmp as *const BTreeNode as *const Self, self, 1);
        }
        Ok(())
    }

    fn request_space_for_child(&mut self, key_length: usize) -> Result<usize, ()> {
        let size = size_of::<PageIndirectionVectorEntry>() + size_of::<usize>() + key_length;
        if self.free_space() >= size {
            Ok(self.head.prefix_len as usize)
        } else {
            Err(())
        }
    }

    fn find_child_index(&mut self, key: &[u8], bc: &mut BranchCacheAccessor) -> usize {
        let key = PrefixTruncatedKey(&key[self.head.prefix_len as usize..]);
        let index = bc.predict().filter(|&i| {
            i <= self.key_count()
                && (i == 0 || self.piv_entry(i - 1).key(self) < key)
                && (i >= self.key_count() || key <= self.piv_entry(i).key(self))
        })
            .unwrap_or_else(|| unsafe {
                let (range_index, remaining_key_len) = self.find_key_range_unchecked(key.0, self.head.root_node);
                let range_index = range_index as usize;
                let key_skip = key.len() - remaining_key_len;
                let range = self.range_array()[range_index - 1] as usize..self.range_array()[range_index] as usize;
                debug_assert!(range.end == self.head.key_count as usize || key <= self.page_indirection_vector()[range.end].key(self));
                debug_assert!(range.start == 0 || self.page_indirection_vector()[range.start - 1].key(self) < key);
                let index = range.start as usize + match self.page_indirection_vector()[range].binary_search_by_key(&&key.0[key_skip..], |e: &PageIndirectionVectorEntry| {
                    &&e.key(self).0[key_skip..]
                }) {
                    Ok(i) | Err(i) => i
                };
                index
            });
        bc.store(index);
        index
    }
}

unsafe impl InnerConversionSink for ArtNode {
    fn create(dst: &mut BTreeNode, src: &(impl InnerConversionSource + ?Sized)) -> Result<(), ()> {
        let key_count = src.key_count();
        let piv_space = key_count * size_of::<PageIndirectionVectorEntry>();
        let this = dst.write_inner(ArtNode {
            head: ArtNodeHead {
                tag: BTreeNodeTag::ArtInner,
                data_write: PAGE_SIZE as u16,
                range_array_len: 0,
                upper: src.get_child(key_count),
                key_count: 0,
                lower_fence: 0,
                upper_fence: 0,
                prefix_len: 0,
                root_node: 0,
            },
            data: ArtNodeData {
                _bytes: unsafe { std::mem::zeroed() },
            },
        });
        let fences = src.fences();
        this.head.upper_fence = this.heap_write(fences.upper_fence.0)? as u16;
        this.head.lower_fence = this.heap_write(fences.lower_fence.0)? as u16;
        this.head.prefix_len = fences.prefix_len as u16;
        let mut key_entries = SmallVec::<[PageIndirectionVectorEntry; 256]>::new();
        for ki in 0..key_count {
            this.heap_write((src.get_child(ki) as usize).to_ne_bytes().as_slice())?;
            let data_write = this.head.data_write as usize;
            let written = src.get_key(ki, unsafe { &mut reinterpret_mut::<Self, [u8; PAGE_SIZE]>(this)[size_of::<ArtNodeHead>()..data_write] }, 0)?;
            this.head.data_write -= written as u16;
            key_entries.push(PageIndirectionVectorEntry {
                key_len: written as u16,
                key_offset: (data_write - written) as u16,
            });
        }
        this.head.key_count = key_count as u16;
        this.push_range_array_entry(0..0)?;
        this.head.root_node = this.construct(&|node, index| {
            let s = key_entries[index];
            PrefixTruncatedKey(unsafe {
                &reinterpret::<Self, [u8; PAGE_SIZE]>(node)[s.key_offset as usize..][..s.key_len as usize]
            })
        }, 0..key_count, 0)?;
        let indirection_vector_offset = Self::layout(this.head.range_array_len as usize).page_indirection_vector as usize;
        unsafe {
            let piv = (this as *mut Self as *mut u8).offset(indirection_vector_offset as isize) as *mut PageIndirectionVectorEntry;
            if this.free_space() < piv_space {
                return Err(());
            }
            std::slice::from_raw_parts_mut(piv, key_entries.len()).copy_from_slice(&key_entries[..]);
        }
        Ok(())
    }
}

impl InnerConversionSource for ArtNode {
    fn fences(&self) -> FenceData {
        unsafe {
            FenceData {
                prefix_len: self.head.prefix_len as usize,
                lower_fence: FenceRef(&reinterpret::<Self, [u8; PAGE_SIZE]>(self)[self.head.lower_fence as usize..self.head.upper_fence as usize]),
                upper_fence: FenceRef(&reinterpret::<Self, [u8; PAGE_SIZE]>(self)[self.head.upper_fence as usize..]),
            }
        }
    }

    fn key_count(&self) -> usize {
        self.head.key_count as usize
    }

    fn get_child(&self, index: usize) -> *mut BTreeNode {
        if index < self.head.key_count as usize {
            let entry = self.piv_entry(index);
            unsafe {
                ((self as *const Self as *const u8).offset((entry.key_offset + entry.key_len) as isize) as *const *mut BTreeNode).read_unaligned()
            }
        } else {
            debug_assert!(index == self.head.key_count as usize);
            self.head.upper
        }
    }

    fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()> {
        get_key_from_slice(self.piv_entry(index).key(self), dst, strip_prefix)
    }

    fn get_key_length_sum(&self, _range: Range<usize>) -> usize {
        unimplemented!()
    }

    fn get_key_length_max(&self, _range: Range<usize>) -> usize {
        self.page_indirection_vector().iter().map(|e| e.key_len as usize).max().unwrap_or(0)
    }
}

impl SeparableInnerConversionSource for ArtNode {
    type Separator<'a> = PrefixTruncatedKey<'a>;

    fn find_separator<'a>(&'a self) -> (usize, Self::Separator<'a>) {
        find_separator(
            self.head.key_count as usize,
            self.head.tag.is_leaf(),
            |i: usize| {
                let e = self.page_indirection_vector()[i];
                PrefixTruncatedKey(unsafe { &reinterpret::<Self, [u8; PAGE_SIZE]>(self)[e.key_offset as usize..][..e.key_len as usize] })
            },
        )
    }
}