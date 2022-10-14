use crate::basic_node::BasicNode;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::{mem, ptr};

pub const PAGE_SIZE: usize = 4096;

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Clone, Copy)]
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
}
