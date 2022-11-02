use crate::basic_node::BasicNode;
use crate::hash_leaf::HashLeaf;
use crate::head_node::{U32HeadNode, U64HeadNode};
use crate::{FatTruncatedKey, PrefixTruncatedKey};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::mem::{size_of, ManuallyDrop};
use std::{mem, ptr};
use std::intrinsics::transmute;
use crate::inner_node::InnerNode;

pub const PAGE_SIZE: usize = 4096;

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum BTreeNodeTag {
    BasicLeaf = 0,
    HashLeaf = 1,
    BasicInner = 128,
    U64HeadNode = 129,
    U32HeadNode = 130,
}

impl BTreeNodeTag {
    pub fn is_leaf(&self) -> bool {
        match self {
            BTreeNodeTag::BasicLeaf => true,
            BTreeNodeTag::BasicInner => false,
            BTreeNodeTag::HashLeaf => true,
            BTreeNodeTag::U64HeadNode => false,
            BTreeNodeTag::U32HeadNode => false,
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
    pub u64_head_node: ManuallyDrop<U64HeadNode>,
    pub u32_head_node: ManuallyDrop<U32HeadNode>,
}

impl BTreeNode {
    pub fn write_inner<N: InnerNode>(&mut self, src: N) -> &mut N {
        unsafe {
            ptr::copy_nonoverlapping((&src) as *const _ as *const Self, self);
            mem::forget(src);
            transmute::<&mut Self,_>(self)
        }
    }

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
                    // eprintln!("descend {}",index);
                    self = &mut *self.basic.get_child(index);
                },
                BTreeNodeTag::HashLeaf => break,
                BTreeNodeTag::U64HeadNode => unsafe {
                    index = self.u64_head_node.find_child_for_key(key);
                    parent = self;
                    self = &mut *self.u64_head_node.get_child(index);
                },
                BTreeNodeTag::U32HeadNode => unsafe {
                    index = self.u32_head_node.find_child_for_key(key);
                    parent = self;
                    self = &mut *self.u32_head_node.get_child(index);
                },
            };
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
            //(*leaf).basic = BasicNode::new_leaf();
            leaf
        }
    }

    pub fn new_inner(child: *mut BTreeNode) -> *mut BTreeNode {
        unsafe {
            let node = Self::alloc();
            (*node).u32_head_node = ManuallyDrop::new(U32HeadNode::new(
                BTreeNodeTag::U32HeadNode,
                PrefixTruncatedKey(&[]),
                PrefixTruncatedKey(&[]),
                0,
                child,
            ));
            //(*node).basic = BasicNode::new_inner(child);
            node
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
            BTreeNodeTag::U64HeadNode => unsafe {
                self.u64_head_node.request_space_for_child(key_length)
            },
            BTreeNodeTag::U32HeadNode => unsafe {
                self.u32_head_node.request_space_for_child(key_length)
            },
        }
    }

    /// key must be truncated to length returned from request_space
    pub fn insert_child(
        &mut self,
        index: usize,
        key: PrefixTruncatedKey,
        child: *mut BTreeNode,
    ) -> Result<(), ()> {
        match self.tag() {
            BTreeNodeTag::BasicInner => unsafe {
                self.basic
                    .raw_insert(index, key, &(child as usize).to_ne_bytes());
                Ok(())
            },
            BTreeNodeTag::U64HeadNode => unsafe {
                U64HeadNode::insert_child(&mut self.u64_head_node, index, key, child)
            },
            BTreeNodeTag::U32HeadNode => unsafe {
                U32HeadNode::insert_child(&mut self.u32_head_node, index, key, child)
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
            BTreeNodeTag::U64HeadNode => todo!(),
            BTreeNodeTag::U32HeadNode => todo!(),
        }
    }

    pub fn try_merge_child(&mut self, child_index: usize) -> Result<(), ()> {
        match self.tag() {
            BTreeNodeTag::BasicInner => unsafe { self.basic.merge_children_check(child_index) },
            BTreeNodeTag::BasicLeaf => panic!(),
            BTreeNodeTag::HashLeaf => unreachable!(),
            BTreeNodeTag::U64HeadNode => todo!(),
            BTreeNodeTag::U32HeadNode => todo!(),
        }
    }

    /// merge into right,
    ///self is discarded after this
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
            BTreeNodeTag::HashLeaf => self
                .hash_leaf
                .try_merge_right(&mut (*right).hash_leaf, separator),
            BTreeNodeTag::U64HeadNode => todo!(),
            BTreeNodeTag::U32HeadNode => todo!(),
        }
    }

    pub fn validate_tree(&self, lower: &[u8], upper: &[u8]) {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe {
                self.basic.validate_tree(lower, upper)
            },
            BTreeNodeTag::HashLeaf => unsafe { self.hash_leaf.validate_tree(lower, upper) },
            BTreeNodeTag::U64HeadNode => unsafe { self.u64_head_node.validate_tree(lower, upper) },
            BTreeNodeTag::U32HeadNode => unsafe { self.u32_head_node.validate_tree(lower, upper) },
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<()> {
        match self.tag() {
            BTreeNodeTag::BasicInner | BTreeNodeTag::BasicLeaf => unsafe { self.basic.remove(key) },
            BTreeNodeTag::HashLeaf => unsafe { self.hash_leaf.remove(key) },
            BTreeNodeTag::U64HeadNode => todo!(),
            BTreeNodeTag::U32HeadNode => todo!(),
        }
    }
}
