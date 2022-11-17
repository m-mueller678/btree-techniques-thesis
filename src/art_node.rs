use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::io::{Write};
use std::mem::{align_of, size_of};
use std::ops::Range;
use smallvec::SmallVec;
use crate::{PAGE_SIZE, PrefixTruncatedKey};
use crate::util::{common_prefix_len, reinterpret, reinterpret_mut};
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
    child_count: u16,
    data_write: u16,
    range_array_len: u16,
    key_count: u16,
}

struct LayoutInfo {
    range_array: usize,
    page_indirection_vector: usize,
}

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

    fn construct(&mut self, keys: &[PrefixTruncatedKey], mut key_range: Range<usize>, prefix_len: usize) -> Result<u16, ()> {
        debug_assert!(key_range.len() > 0);
        let mut full_prefix = prefix_len;
        let initial_range_start = key_range.start;
        loop {
            if key_range.len() < 3 {
                dbg!(&key_range);
                eprintln!("{:?}", &keys[key_range.clone()]);
                return Ok(dbg!(self.push_range_array_entry(key_range.end as u16)?) | NODE_REF_IS_RANGE);
            }
            full_prefix += common_prefix_len(&keys[key_range.start].0[full_prefix..], &keys[key_range.clone()].last().unwrap().0[full_prefix..]);
            if keys[key_range.start].0.len() == full_prefix {
                key_range.start += 1;
            } else {
                break;
            }
        }
        let ret = if full_prefix > prefix_len {
            self.push_range_array_entry(initial_range_start as u16)?;
            let child = self.construct_inner_decision_node(&keys, key_range.clone(), full_prefix)?;
            self.push_range_array_entry(key_range.end as u16)?;
            let span_len = full_prefix - prefix_len;
            self.set_heap_write_pos_mod_2(span_len as u16)?;
            self.heap_write(&keys[key_range.start][prefix_len..full_prefix])?;
            self.assert_heap_write_aligned();
            self.heap_write((span_len as u16).to_ne_bytes().as_slice())?;
            Ok(self.heap_write(NODE_TAG_SPAN.to_ne_bytes().as_slice())? as u16)
        } else {
            self.construct_inner_decision_node(&keys, key_range, full_prefix)
        };
        eprintln!("KK {:#?}", NodeDebugWrapper { offset: ret?, page: &self });
        ret
    }

    unsafe fn node_min(&self, node: u16) -> u16 {
        if (node & NODE_REF_IS_RANGE) != 0 {
            return (node & !NODE_REF_IS_RANGE) - 1;
        }
        let (_node_bytes, extra) = self.read_node(node);
        match extra {
            Ok(children) => {
                self.node_min(children[0])
            }
            Err(child_offset) => {
                self.node_min(child_offset)
            }
        }
    }

    unsafe fn node_max(&self, node: u16) -> u16 {
        if (node & NODE_REF_IS_RANGE) != 0 {
            return (node & !NODE_REF_IS_RANGE) + 1;
        }
        let (_node_bytes, extra) = self.read_node(node);
        match extra {
            Ok(children) => {
                self.node_max(*children.last().unwrap())
            }
            Err(child_offset) => {
                self.node_max(child_offset)
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
    fn construct_inner_decision_node(&mut self, keys: &[PrefixTruncatedKey], key_range: Range<usize>, prefix_len: usize) -> Result<u16, ()> {
        dbg!(&keys[key_range.clone()],prefix_len);
        let mut children = SmallVec::<[u16; 64]>::new();
        {
            let mut range_start = key_range.start;
            for i in range_start + 1..key_range.end {
                if (i == range_start + 1 && keys[i - 1].len() == prefix_len) || keys[i - 1][prefix_len] != keys[i][prefix_len] {
                    children.push(self.construct(keys, range_start..i, prefix_len)?);
                    range_start = i;
                }
            }
            children.push(self.construct(keys, range_start..key_range.end, prefix_len)?);
        }
        let key_count = children.len() - 1;
        let key_array_size = key_count.next_multiple_of(2);
        let pos = self.heap_alloc(size_of::<u16>() * 2 + key_array_size + size_of::<u16>() * children.len())? as usize;
        unsafe {
            self.write_to(pos, NODE_TAG_DECISION.to_ne_bytes().as_slice());
            self.write_to(pos + 2, (key_count as u16).to_ne_bytes().as_slice());
            {
                let mut keys_slice = &mut reinterpret_mut::<Self, [u8; PAGE_SIZE]>(self)[pos + 4..][..key_count];
                let range_start = key_range.start;
                for i in range_start + 1..key_range.end {
                    if (i == range_start + 1 && keys[i - 1].len() == prefix_len) || keys[i - 1][prefix_len] != keys[i][prefix_len] {
                        keys_slice.write_all(&[keys[i - 1][prefix_len]]).unwrap();
                    }
                }
                debug_assert!(keys_slice.is_empty());
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
    let max_len = 5;
    let insert_count = 10;
    let lookup_count = 50;

    let mut rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(0x1234567890abcdef);
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
                child_count: 0,
                data_write: PAGE_SIZE as u16,
                range_array_len: 0,
                key_count: 0,
            },
            data: ArtNodeData {
                _bytes: unsafe { std::mem::zeroed() },
            },
        };
        node.push_range_array_entry(0).unwrap();
        let root_node = node.construct(&keys, 0..keys.len(), 0).unwrap();

        eprintln!("keys:");
        for k in &keys {
            eprintln!("\t{:3?}", k.0);
        }

        eprintln!("{:#?}", NodeDebugWrapper { offset: root_node, page: &node });
        eprintln!("{:#?}", &node);

        let test_key = |k: PrefixTruncatedKey| {
            unsafe {
                let found = node.find_key_range(k.0, root_node);
                let range = node.data.range_array[found as usize - 1] as usize..node.data.range_array[found as usize] as usize;
                eprintln!("\t{:3?} -> {} -> {:?} -> {:?}", k.0, found, range, &keys[range.clone()]);
                assert!(keys[range.start] <= k || range.start == 0 || keys[range.start - 1] < k);
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