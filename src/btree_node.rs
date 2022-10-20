use crate::basic_node::BasicNode;
use crate::hash_leaf::HashLeaf;
use crate::head_stripped_node::HeadStrippedNode;
use crate::PrefixTruncatedKey;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::mem::{size_of, ManuallyDrop};
use std::{mem, ptr};

pub const PAGE_SIZE: usize = 4096;

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum BTreeNodeTag {
    BasicLeaf,
    BasicInner,
    HeadTruncatedLeaf,
    HeadTruncatedInner,
    HashLeaf,
}

impl BTreeNodeTag {
    pub fn is_leaf(&self) -> bool {
        match self {
            BTreeNodeTag::BasicLeaf => true,
            BTreeNodeTag::BasicInner => false,
            BTreeNodeTag::HeadTruncatedLeaf => true,
            BTreeNodeTag::HeadTruncatedInner => false,
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
    pub head_truncated: HeadStrippedNode,
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
                    index = self.basic.lower_bound(key).0;
                    parent = self;
                    self = &mut *self.basic.get_child(index);
                },
                BTreeNodeTag::HeadTruncatedLeaf => break,
                BTreeNodeTag::HeadTruncatedInner => unsafe {
                    index = self
                        .head_truncated
                        .lower_bound(PrefixTruncatedKey(&key[self.head_truncated.prefix_len()..]))
                        .0;
                    parent = self;
                    self = &mut *self.head_truncated.get_child(index);
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
            (*leaf).head_truncated = HeadStrippedNode::new_inner(child);
            leaf
        }
    }

    pub fn space_needed(&self, key_length: usize) -> usize {
        match self.tag() {
            BTreeNodeTag::HashLeaf | BTreeNodeTag::BasicLeaf | BTreeNodeTag::HeadTruncatedLeaf => {
                unreachable!()
            }
            BTreeNodeTag::BasicInner => {
                BasicNode::space_needed(key_length, size_of::<*mut BTreeNode>())
            }
            BTreeNodeTag::HeadTruncatedInner => unsafe {
                self.head_truncated
                    .space_needed(key_length, size_of::<*mut BTreeNode>())
            },
        }
    }

    pub fn request_space(&mut self, space: usize) -> Result<(), ()> {
        match self.tag() {
            BTreeNodeTag::HashLeaf => unsafe { self.hash_leaf.request_space(space) },
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe {
                self.basic.request_space(space)
            },
            BTreeNodeTag::HeadTruncatedInner | BTreeNodeTag::HeadTruncatedLeaf => unsafe {
                self.head_truncated.request_space(space)
            },
        }
    }

    pub fn insert_child(
        &mut self,
        key: PrefixTruncatedKey,
        prefix_len: usize,
        child: *mut BTreeNode,
    ) -> Result<(), ()> {
        match self.tag() {
            BTreeNodeTag::BasicInner => unimplemented!(),
            BTreeNodeTag::HeadTruncatedInner => unsafe {
                self.head_truncated
                    .insert(key, prefix_len, &(child as usize).to_ne_bytes())
            },
            BTreeNodeTag::HashLeaf | BTreeNodeTag::HeadTruncatedLeaf | BTreeNodeTag::BasicLeaf => {
                unreachable!()
            }
        }
    }

    pub fn is_underfull(&self) -> bool {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe {
                self.basic.free_space_after_compaction() >= PAGE_SIZE * 3 / 4
            },
            BTreeNodeTag::HeadTruncatedInner | BTreeNodeTag::HeadTruncatedLeaf => unsafe {
                self.head_truncated.free_space_after_compaction() >= PAGE_SIZE * 3 / 4
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
            BTreeNodeTag::HeadTruncatedInner => unsafe {
                self.head_truncated.merge_children_check(child_index)
            },
            BTreeNodeTag::HeadTruncatedLeaf => unreachable!(),
            BTreeNodeTag::HashLeaf => unreachable!(),
        }
    }

    pub unsafe fn try_merge_right(
        &mut self,
        right: *mut BTreeNode,
        separator: PrefixTruncatedKey,
        separator_prefix_len: usize,
    ) -> Result<(), ()> {
        debug_assert!((*right).tag() == self.tag());
        debug_assert!(right != self);
        match self.tag() {
            BTreeNodeTag::BasicInner => {
                self.basic
                    .merge_right_inner(&mut (*right).basic, separator, separator_prefix_len)
            }
            BTreeNodeTag::BasicLeaf => self.basic.merge_right_leaf(&mut (*right).basic),
            BTreeNodeTag::HeadTruncatedInner => todo!(),
            BTreeNodeTag::HeadTruncatedLeaf => todo!(),
            BTreeNodeTag::HashLeaf => todo!(),
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<()> {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe { self.basic.remove(key) },
            BTreeNodeTag::HeadTruncatedInner | BTreeNodeTag::HeadTruncatedLeaf => unsafe {
                self.head_truncated.remove(key)
            },
            BTreeNodeTag::HashLeaf => {
                todo!()
            }
        }
    }
}
