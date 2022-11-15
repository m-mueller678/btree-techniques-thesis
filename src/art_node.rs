use std::cmp::Ordering;
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
const NODE_TAG_DECISION: u16 = 1;
const NODE_TAG_SPAN: u16 = 1;

impl ArtNode {
    #[inline(always)]
    fn layout(range_array_len: usize) -> LayoutInfo {
        let range_array = size_of::<ArtNodeHead>().next_multiple_of(align_of::<u16>());
        let page_indirection_vector = (range_array + range_array_len * size_of::<u16>()).next_multiple_of(align_of::<PageIndirectionVectorEntry>());
        LayoutInfo { range_array, page_indirection_vector }
    }

    /// returns Ok for decision nodes, Err(successor) for span nodes
    #[inline(always)]
    unsafe fn read_node(&self, offset: u16) -> (usize, &[u8], Result<&[u16], u16>) {
        debug_assert_eq!(offset & NODE_REF_IS_RANGE, 0);
        let bytes = reinterpret::<Self, [u8; PAGE_SIZE]>(self).as_slice();
        let node_ptr = unsafe { bytes.as_ptr().offset(offset as isize) as *const u16 };
        debug_assert!(node_ptr.is_aligned());
        let node_tag = *node_ptr;
        let key_count = *node_ptr.offset(1) as usize;
        let node_bytes = &bytes[offset as usize + 4..][..key_count];
        let extra = if node_tag == NODE_TAG_DECISION {
            Ok(std::slice::from_raw_parts(node_ptr.offset(2 + key_count.next_multiple_of(2) as isize / 2), key_count + 1))
        } else {
            Err(offset + 4 + key_count.next_multiple_of(2) as u16)
        };
        (key_count, node_bytes, extra)
    }

    fn construct(&mut self, keys: &[PrefixTruncatedKey], key_range: Range<usize>, prefix_len: usize) -> Result<u16, ()> {
        debug_assert!(key_range.len() > 0);
        if key_range.len() < 3 {
            return Ok(self.push_range_array_entry(key_range.end as u16)? | NODE_REF_IS_RANGE);
        }
        let new_prefix_len = common_prefix_len(&keys[key_range.start].0[prefix_len..], &keys[key_range.end - 1].0[prefix_len..]);
        if new_prefix_len > 0 {
            self.push_range_array_entry(key_range.start as u16);
            self.construct_inner_decision_node(&keys, key_range.clone(), prefix_len + new_prefix_len);
            self.push_range_array_entry(key_range.end as u16);
            self.set_heap_write_pos_mod_2(new_prefix_len as u16);
            self.heap_write(&keys[key_range.start][prefix_len..][..new_prefix_len])?;
            self.assert_heap_write_aligned();
            self.heap_write((new_prefix_len as u16).to_ne_bytes().as_slice())?;
            Ok(self.heap_write(NODE_TAG_SPAN.to_ne_bytes().as_slice())? as u16)
        } else {
            self.construct_inner_decision_node(&keys, key_range, prefix_len + new_prefix_len)
        }
    }

    unsafe fn node_min(&self, node: u16) -> u16 {
        if (node & NODE_REF_IS_RANGE) != 0 {
            return (node & !NODE_REF_IS_RANGE) - 1;
        }
        let (_len, _node_bytes, extra) = self.read_node(node);
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
        let (len, _node_bytes, extra) = self.read_node(node);
        match extra {
            Ok(children) => {
                self.node_max(children[len - 1])
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
        let (len, node_bytes, extra) = self.read_node(node);
        match extra {
            Ok(children) => {
                let child_index = if key.is_empty() {
                    0
                } else {
                    node_bytes.iter().rposition(|&k| key[0] <= k).unwrap_or(len)
                };
                self.find_key_range(key, children[child_index])
            }
            Err(successor) => {
                if len <= key.len() {
                    match key[..len].cmp(node_bytes) {
                        Ordering::Equal => self.find_key_range(&key[len..], successor),
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

    fn construct_inner_decision_node(&mut self, keys: &[PrefixTruncatedKey], key_range: Range<usize>, prefix_len: usize) -> Result<u16, ()> {
        dbg!(keys,&key_range,prefix_len);
        debug_assert!(keys[key_range.clone()].iter().all(|k| k.0.len() >= prefix_len));
        debug_assert!(keys[key_range.clone()][1..].iter().all(|k| k.0.len() > prefix_len));
        let mut children = SmallVec::<[u16; 64]>::new();
        {
            let current_byte = keys[key_range.start].get(prefix_len).copied().unwrap_or(0);
            let mut range_start = key_range.start;
            for (i, b) in keys[key_range.clone()].iter().skip(1).map(|k| k.0[prefix_len]).enumerate()
            {
                let i = i + key_range.start + 1;
                if b != current_byte {
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
                let current_byte = keys[key_range.start].get(prefix_len).copied().unwrap_or(0);
                let mut keys_slice = &mut reinterpret_mut::<Self, [u8; PAGE_SIZE]>(self)[pos + 4..][..key_count];
                for b in keys.iter().skip(1).map(|k| k.0[prefix_len]) {
                    if b != current_byte {
                        keys_slice.write_all(&[b]).unwrap();
                    }
                }
                debug_assert!(keys_slice.is_empty());
            }
            self.write_to(pos + 4 + key_array_size, bytemuck::cast_slice::<u16, u8>(children.as_slice()));
        }
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

    fn set_heap_write_pos_mod_2(&mut self, m: u16) {
        if self.head.data_write % 2 != m % 2 {
            self.head.data_write -= 1;
            todo!();
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
            return Err(())
        } else {
            let pos = self.head.range_array_len;
            unsafe {
                self.data.range_array[pos as usize] = index;
            }
            self.head.range_array_len += 1;
            Ok(pos)
        }
    }
}

#[test]
fn test_tree() {
    use rand::*;

    let mut rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(0x1234567890abcdef);
    let max_len = 5;
    let mut radixes: Vec<u8> = (0..max_len).map(|i| rng.gen_range(0..32)).collect();
    let insert_count = 5;
    let keys: Vec<Vec<u8>> = (0..insert_count).map(|_| {
        let len = rng.gen_range(0..=max_len);
        (0..len).map(|i| { rng.gen_range(0..=radixes[i]) }).collect()
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
    let root_node = node.construct(&keys, 0..keys.len(), 0);
}