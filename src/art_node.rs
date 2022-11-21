use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::io::{Write};
use std::mem::{align_of, size_of};
use std::ops::Range;
use std::ptr;
use smallvec::SmallVec;
use crate::{BTreeNode, PAGE_SIZE, PrefixTruncatedKey};
use crate::basic_node::BasicNode;
use crate::find_separator::find_separator;
use crate::inner_node::{FenceData, FenceRef, InnerConversionSink, InnerConversionSource, InnerNode, LeafNode, Node, SeparableInnerConversionSource, split_in_place};
use crate::util::{common_prefix_len, get_key_from_slice, reinterpret, reinterpret_mut};
use crate::vtables::BTreeNodeTag;

pub struct ArtNode {
    head: ArtNodeHead,
    data: ArtNodeData,
}

union ArtNodeData {
    range_array: [u16; (PAGE_SIZE - size_of::<ArtNodeHead>()) / 2],
    _bytes: [u8; PAGE_SIZE - size_of::<ArtNodeHead>()],
}

#[derive(Debug)]
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

#[derive(Clone, Copy)]
struct PageIndirectionVectorEntry {
    key_offset: u16,
    key_len: u16,
}

const NODE_REF_IS_RANGE: u16 = 1 << 15;
const NODE_TAG_DECISION: u16 = 0xa3cf;
const NODE_TAG_SPAN: u16 = 0x1335;

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

    fn construct<F: Fn(&Self, usize) -> PrefixTruncatedKey>(&mut self, keys: &F, mut key_range: Range<usize>, prefix_len: usize) -> Result<u16, ()> {
        let mut full_prefix = prefix_len;
        let initial_range_start = key_range.start;
        if key_range.len() < 3 {
            return Ok(self.push_range_array_entry(key_range.end as u16)? | NODE_REF_IS_RANGE);
        }
        full_prefix += common_prefix_len(&keys(self, key_range.start).0[full_prefix..], &keys(self, key_range.end - 1).0[full_prefix..]);
        while keys(self, key_range.start).len() == full_prefix {
            let without_first = common_prefix_len(&keys(self, key_range.start + 1).0[full_prefix..], &keys(self, key_range.end - 1).0[full_prefix..]);
            if without_first > 0 {
                key_range.start += 1;
                full_prefix += without_first;
                if key_range.len() < 3 {
                    return Ok(self.push_range_array_entry(key_range.end as u16)? | NODE_REF_IS_RANGE);
                }
            } else {
                break;
            }
        }
        let ret = if full_prefix > prefix_len {
            self.push_range_array_entry(key_range.start as u16)?;
            let child = self.construct_inner_decision_node::<F>(&keys, key_range.clone(), full_prefix)?;
            self.push_range_array_entry(key_range.end as u16)?;
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

    unsafe fn find_key_range(&self, key: &[u8], node: u16) -> u16 {
        if (node & NODE_REF_IS_RANGE) != 0 {
            return node & !NODE_REF_IS_RANGE;
        }
        let (node_bytes, extra) = self.read_node(node);
        match extra {
            Ok(children) => {
                let child_index = if key.is_empty() {
                    0
                } else {
                    node_bytes.iter().position(|&k| key[0] <= k).unwrap_or(node_bytes.len())
                };
                self.find_key_range(key, children[child_index])
            }
            Err(successor) => {
                if node_bytes.len() <= key.len() {
                    match key[..node_bytes.len()].cmp(node_bytes) {
                        Ordering::Equal => self.find_key_range(&key[node_bytes.len()..], successor),
                        Ordering::Less => self.node_min(node),
                        Ordering::Greater => self.node_max(node),
                    }
                } else {
                    match key.cmp(node_bytes) {
                        Ordering::Equal => unreachable!(),
                        Ordering::Less => self.node_min(node),
                        Ordering::Greater => self.node_max(node),
                    }
                }
            }
        }
    }

    // TODO: how to handle [[],[0,...],[0,...],...]?
    //       0 key cannot split -> cannot create span node in next step
    //       decision four cases: less, equal, greater, empty?
    //       span four cases: prefix less, prefix greater, same as prefix, prefix is prefix of key
    fn construct_inner_decision_node<F: Fn(&Self, usize) -> PrefixTruncatedKey>(&mut self, keys: &F, key_range: Range<usize>, prefix_len: usize) -> Result<u16, ()> {
        let mut children = SmallVec::<[u16; 64]>::new();
        {
            let mut range_start = key_range.start;
            for i in range_start + 1..key_range.end {
                if !(i == range_start + 1 && keys(self, i - 1).len() == prefix_len) && keys(self, i - 1)[prefix_len] != keys(self, i)[prefix_len] {
                    children.push(self.construct::<F>(keys, range_start..i, prefix_len)?);
                    range_start = i;
                }
            }
            children.push(self.construct::<F>(keys, range_start..key_range.end, prefix_len)?);
        }
        let key_count = children.len() - 1;
        let key_array_size = key_count.next_multiple_of(2);
        let pos = self.heap_alloc(size_of::<u16>() * 2 + key_array_size + size_of::<u16>() * children.len())? as usize;
        unsafe {
            self.write_to(pos, NODE_TAG_DECISION.to_ne_bytes().as_slice());
            self.write_to(pos + 2, (key_count as u16).to_ne_bytes().as_slice());
            {
                let range_start = key_range.start;
                let mut next_write = 0;
                for i in range_start + 1..key_range.end {
                    if !(i == range_start + 1 && keys(self, i - 1).len() == prefix_len) && keys(self, i - 1)[prefix_len] != keys(self, i)[prefix_len] {
                        reinterpret_mut::<Self, [u8; PAGE_SIZE]>(self)[pos + 4 + next_write] = keys(self, i - 1)[prefix_len];
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

    fn push_range_array_entry(&mut self, index: u16) -> Result<u16, ()> {
        if self.free_space() < 2 {
            return Err(());
        } else {
            let pos = self.head.range_array_len;
            unsafe {
                self.data.range_array[pos as usize] = index;
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
        s.finish()
    }
}

pub fn test_tree() {
    use rand::*;
    let max_len = 10;
    let insert_count = 100;
    let lookup_count = 200;

    let rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(0x1234567890abcdef);
    for (iteration, mut rng) in std::iter::successors(Some(rng), |rng| {
        let mut r = rng.clone();
        r.jump();
        Some(r)
    }).enumerate() {
        dbg!(iteration);
        let radixes: Vec<u8> = (0..max_len).map(|_| rng.gen_range(0..32)).collect();
        let mut gen_key = || {
            let len = rng.gen_range(0..=max_len);
            (0..len).map(|i| { rng.gen_range(0..=radixes[i]) }).collect()
        };
        let keys: Vec<Vec<u8>> = (0..insert_count).map(|_| {
            gen_key()
        }).collect();
        let mut keys: Vec<PrefixTruncatedKey> = keys.iter().map(|v| PrefixTruncatedKey(&**v)).collect();
        keys.sort();
        keys.dedup();

        let mut node = ArtNode {
            head: ArtNodeHead {
                tag: BTreeNodeTag::BasicLeaf,
                key_count: 0,
                data_write: PAGE_SIZE as u16,
                range_array_len: 0,
                lower_fence: 0,
                upper_fence: 0,
                prefix_len: 0,
                upper: ptr::null_mut(),
                root_node: 0,
            },
            data: ArtNodeData {
                _bytes: unsafe { std::mem::zeroed() },
            },
        };
        dbg!(PAGE_SIZE - node.head.data_write as usize);
        node.push_range_array_entry(0).unwrap();
        let root_node = node.construct(&|_node: &ArtNode, i| unsafe {
            // key must live for _node
            std::mem::transmute::<PrefixTruncatedKey<'_>, PrefixTruncatedKey<'static>>(keys[i])
        }, 0..keys.len(), 0).unwrap();
        let test_key = |k: PrefixTruncatedKey| {
            unsafe {
                let found = node.find_key_range(k.0, root_node);
                let range = node.data.range_array[found as usize - 1] as usize..node.data.range_array[found as usize] as usize;
                assert!(range.start == keys.len() || keys[range.start] <= k || range.start == 0 || keys[range.start - 1] < k);
                assert!(range.end == keys.len() || k < keys[range.end]);
            }
        };

        for &k in keys.iter()
        {
            test_key(k)
        }

        for _ in 0..lookup_count {
            let k = gen_key();
            test_key(PrefixTruncatedKey(&k));
        }
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
        todo!()
    }

    fn split_node(&mut self, parent: &mut dyn InnerNode, index_in_parent: usize, key_in_node: &[u8]) -> Result<(), ()> {
        unsafe {
            split_in_place::<Self, Self, Self>(reinterpret_mut::<Self, BTreeNode>(self), parent, index_in_parent, key_in_node)
        }
    }
}

impl InnerNode for ArtNode {
    fn merge_children_check(&mut self, child_index: usize) -> Result<(), ()> {
        //TODO
        return Err(());
    }

    unsafe fn insert_child(&mut self, index: usize, key: PrefixTruncatedKey, child: *mut BTreeNode) -> Result<(), ()> {
        self.heap_write((child as usize).to_ne_bytes().as_slice())?;
        let key_offfset = self.heap_write(key.0)?;
        if self.free_space() < size_of::<PageIndirectionVectorEntry>() {
            return Err(());
        }
        let piv_ptr = (self as *mut Self as *mut u8).offset(Self::layout(self.head.range_array_len as usize).page_indirection_vector as isize) as *mut PageIndirectionVectorEntry;
        let new_key_count = self.head.key_count as usize + 1;
        let extended_piv = std::slice::from_raw_parts_mut(piv_ptr, new_key_count);
        extended_piv.copy_within(index..new_key_count - 1, index + 1);
        extended_piv[index] = PageIndirectionVectorEntry {
            key_offset: key_offfset as u16,
            key_len: key.0.len() as u16,
        };
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

    fn find_child_index(&self, key: &[u8]) -> usize {
        unsafe {
            let key = &key[self.head.prefix_len as usize..];
            let range_index = self.find_key_range(key, self.head.root_node) as usize;
            let range = self.range_array()[range_index - 1] as usize..self.range_array()[range_index - 1] as usize;
            let index = range.start as usize + match self.page_indirection_vector()[range].binary_search_by_key(&key, |e: &PageIndirectionVectorEntry| {
                &reinterpret::<Self, [u8; PAGE_SIZE]>(self)[e.key_offset as usize..][..e.key_len as usize]
            }) {
                Ok(i) | Err(i) => i
            };
            index
        }
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
        this.push_range_array_entry(0)?;
        this.construct(&|node, index| {
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
        this.head.key_count = key_count as u16;
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
        let entry = self.piv_entry(index);
        unsafe {
            get_key_from_slice(PrefixTruncatedKey(&reinterpret::<Self, [u8; PAGE_SIZE]>(self)[entry.key_offset as usize..][..entry.key_len as usize]), dst, strip_prefix)
        }
    }

    fn get_key_length_sum(&self, _range: Range<usize>) -> usize {
        unimplemented!()
    }

    fn get_key_length_max(&self, _range: Range<usize>) -> usize {
        unimplemented!()
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