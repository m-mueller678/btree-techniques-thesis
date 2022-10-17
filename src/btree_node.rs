use crate::basic_node::BasicNode;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::{mem, ptr};

pub const PAGE_SIZE: usize = 4096;

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum BTreeNodeTag {
    BasicLeaf,
    BasicInner,
}

impl BTreeNodeTag {
    pub fn is_leaf(&self) -> bool {
        match self {
            BTreeNodeTag::BasicLeaf => true,
            BTreeNodeTag::BasicInner => false,
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
                    index = self.basic.lower_bound(key).0;
                    parent = self;
                    self = &mut *self.basic.get_child(index);
                },
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
            (*leaf).basic = BasicNode::new_leaf();
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

    pub fn space_needed(&self, key_length: usize, payload_length: usize) -> usize {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => {
                BasicNode::space_needed(key_length, payload_length)
            }
        }
    }

    pub fn request_space(&mut self, space: usize) -> Result<(), ()> {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe {
                self.basic.request_space(space)
            },
        }
    }

    pub fn insert(&mut self, key: &[u8], payload: &[u8]) -> Result<(), ()> {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe {
                self.basic.insert(key, payload)
            },
        }
    }

    pub fn is_underfull(&self) -> bool {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe {
                self.basic.free_space_after_compaction() >= PAGE_SIZE * 3 / 4
            },
        }
    }

    pub fn try_merge_child(&mut self, child_index: usize) -> Result<(), ()> {
        match self.tag() {
            BTreeNodeTag::BasicInner => unsafe {
                self.basic.merge_children_check(child_index)
            },
            BTreeNodeTag::BasicLeaf => panic!(),
        }
    }

    pub unsafe fn try_merge_right(&mut self, right: *mut BTreeNode, separator: &[u8]) -> Result<(), ()> {
        debug_assert!((*right).tag() == self.tag());
        debug_assert!(right != self);
        match self.tag() {
            BTreeNodeTag::BasicInner => self.basic.merge_right_inner(&mut (*right).basic, separator),
            BTreeNodeTag::BasicLeaf => self.basic.merge_right_leaf(&mut (*right).basic),
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<()> {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe {
                self.basic.remove(key)
            },
        }
    }
}
