use crate::basic_node::BasicNode;
use crate::hash_leaf::HashLeaf;
use crate::{FatTruncatedKey, PrefixTruncatedKey};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::mem::{size_of, ManuallyDrop};
use std::{mem, ptr};

pub const PAGE_SIZE: usize = 4096;

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum BTreeNodeTag {
    BasicLeaf,
    BasicInner,
    HashLeaf,
}

impl BTreeNodeTag {
    pub fn is_leaf(&self) -> bool {
        match self {
            BTreeNodeTag::BasicLeaf => true,
            BTreeNodeTag::BasicInner => false,
            BTreeNodeTag::HashLeaf => true,
        }
    }

    pub fn is_inner(&self) -> bool {
        !self.is_leaf()
    }
}

#[repr(C)]
pub union BTreeNode {
    pub raw_bytes: [u8; PAGE_SIZE],
    pub basic: BasicNode,
    pub hash_leaf: ManuallyDrop<HashLeaf>,
}

impl BTreeNode {
    pub fn tag(&self) -> BTreeNodeTag {
        BTreeNodeTag::try_from_primitive(unsafe { self.raw_bytes[0] }).unwrap()
    }

    /// descends to target node, returns target node, parent, and index within parent
    pub fn descend(
        mut self: &mut Self,
        key: &[u8],
        mut filter: impl FnMut(*mut BTreeNode) -> bool,
    ) -> (*mut BTreeNode, *mut BTreeNode, usize) {
        let mut parent = ptr::null_mut();
        let mut index = 0;
        while !filter(self) {
            match self.tag() {
                BTreeNodeTag::BasicLeaf => break,
                BTreeNodeTag::BasicInner => unsafe {
                    index = self.basic.lower_bound(self.basic.truncate(key)).0;
                    parent = self;
                    self = &mut *self.basic.get_child(index);
                },
                BTreeNodeTag::HashLeaf => break,
            }
        }
        (self, parent, index)
    }

    pub fn alloc() -> *mut BTreeNode {
        Box::into_raw(Box::new(BTreeNode {
            raw_bytes: unsafe { mem::zeroed() },
        }))
    }

    pub unsafe fn dealloc(node: *mut BTreeNode) {
        drop(Box::from_raw(node));
    }

    pub fn new_leaf() -> *mut BTreeNode {
        unsafe {
            let leaf = Self::alloc();
            (*leaf).hash_leaf = ManuallyDrop::new(HashLeaf::new());
            leaf
        }
    }

    pub fn new_inner(child: *mut BTreeNode) -> *mut BTreeNode {
        unsafe {
            let leaf = Self::alloc();
            (*leaf).basic = BasicNode::new_inner(child);
            leaf
        }
    }

    /// on success returns prefix length of node
    /// insert should be called with a string truncated to that length
    pub fn request_space_for_child(&mut self, key_length: usize) -> Result<usize, ()> {
        match self.tag() {
            BTreeNodeTag::HashLeaf | BTreeNodeTag::BasicLeaf => unreachable!(),
            BTreeNodeTag::BasicInner => unsafe {
                self.basic.request_space(
                    self.basic
                        .space_needed(key_length, size_of::<*mut BTreeNode>()),
                )
            },
        }
    }

    /// key must be truncated to length returned from request_space
    pub fn insert_child(&mut self, index: usize, key: PrefixTruncatedKey, child: *mut BTreeNode) {
        match self.tag() {
            BTreeNodeTag::BasicInner => unsafe {
                self.basic
                    .raw_insert(index, key, &(child as usize).to_ne_bytes())
            },
            BTreeNodeTag::HashLeaf | BTreeNodeTag::BasicLeaf => {
                unreachable!()
            }
        }
    }

    pub fn is_underfull(&self) -> bool {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe {
                self.basic.free_space_after_compaction() >= PAGE_SIZE * 3 / 4
            },
            BTreeNodeTag::HashLeaf => unsafe {
                self.hash_leaf.free_space_after_compaction() >= PAGE_SIZE * 3 / 4
            },
        }
    }

    pub fn try_merge_child(&mut self, child_index: usize) -> Result<(), ()> {
        match self.tag() {
            BTreeNodeTag::BasicInner => unsafe { self.basic.merge_children_check(child_index) },
            BTreeNodeTag::BasicLeaf => panic!(),
            BTreeNodeTag::HashLeaf => unreachable!(),
        }
    }

    pub unsafe fn try_merge_right(
        &mut self,
        right: *mut BTreeNode,
        separator: FatTruncatedKey,
    ) -> Result<(), ()> {
        debug_assert!((*right).tag() == self.tag());
        debug_assert!(right != self);
        match self.tag() {
            BTreeNodeTag::BasicInner => {
                self.basic.merge_right(true, &mut (*right).basic, separator)
            }
            BTreeNodeTag::BasicLeaf => {
                self.basic
                    .merge_right(false, &mut (*right).basic, separator)
            }
            BTreeNodeTag::HashLeaf => todo!(),
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<()> {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe { self.basic.remove(key) },
            BTreeNodeTag::HashLeaf => {
                todo!()
            }
        }
    }
}
