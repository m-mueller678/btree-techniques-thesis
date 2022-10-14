use std::mem::{size_of, transmute};
use std::{mem, ptr};
use crate::btree_node::{BTreeNode, BTreeNodeTag, PAGE_SIZE};
use crate::util::{head, short_slice};

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
    pub fn init_leaf()->Self {
        BasicNode {
            head: BasicNodeHead {
                tag: BTreeNodeTag::BasicLeaf,
                upper: ptr::null_mut(),
                lower_fence: FenceKeySlot {
                    offset: 0,
                    len: 0,
                },
                upper_fence: FenceKeySlot {
                    offset: 0,
                    len: 0,
                },
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

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        assert_eq!(PAGE_SIZE, size_of::<Self>());
        unsafe {
            transmute(self as *const Self)
        }
    }

    fn fence(&self, upper: bool) -> &[u8] {
        let f = if upper { self.head.upper_fence } else { self.head.lower_fence };
        &self.as_bytes()[f.offset as usize..][..f.len as usize]
    }

    fn prefix(&self) -> &[u8] {
        &self.fence(false)[..self.head.prefix_len as usize]
    }

    pub fn slots(&self) -> &[BasicSlot] {
        unsafe {
            &self.data.slots[..self.head.count as usize]
        }
    }

    pub fn lower_bound(&self, mut key: &[u8]) -> (usize, bool) {
        debug_assert!(key <= self.fence(true));
        debug_assert!(key > self.fence(false));
        debug_assert!(&key[..self.head.prefix_len as usize] == self.prefix());
        key = &key[self.head.prefix_len as usize..];
        let head = head(key);
        let (lower, upper) = self.search_hint(head);
        let search_result = self.slots()[lower..=upper].binary_search_by(|s| {
            let slot_head = s.head;
            slot_head.cmp(&head).then_with(|| {
                s.key(self.as_bytes()).cmp(key)
            })
        });
        match search_result {
            Ok(index) | Err(index) => (index, search_result.is_ok())
        }
    }

    fn search_hint(&self, head: u32) -> (usize, usize) {
        if self.head.count as usize > HINT_COUNT * 2 {
            let dist = self.head.count as usize / (HINT_COUNT + 1);
            let pos = (0..HINT_COUNT).find(|&hi| self.head.hint[hi] >= head).unwrap_or(HINT_COUNT);
            let pos2 = (pos..HINT_COUNT).find(|&hi| self.head.hint[hi] != head).unwrap_or(HINT_COUNT);
            (pos * dist, if pos2 < HINT_COUNT { (pos2 + 1) * dist } else { self.head.count as usize })
        } else {
            (0, self.head.count as usize - 1)
        }
    }

    pub unsafe fn get_child(&self, index: usize) -> *mut BTreeNode {
        debug_assert!(index <= self.head.count as usize);
        if index == self.head.count as usize {
            self.head.upper
        } else {
            ptr::read_unaligned(self.slots()[index].value(self.as_bytes()).as_ptr() as *const *mut BTreeNode)
        }
    }
}