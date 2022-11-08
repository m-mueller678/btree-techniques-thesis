use crate::basic_node::BasicNode;
use crate::hash_leaf::HashLeaf;
use crate::head_node::{U32HeadNode, U64HeadNode};
use crate::inner_node::{FenceData, InnerConversionSink, InnerConversionSource, merge_to_right, Node};
use crate::{FatTruncatedKey, PrefixTruncatedKey};
use num_enum::{TryFromPrimitive};
use std::intrinsics::transmute;
use std::mem::{ManuallyDrop};
use std::{mem, ptr};


use crate::vtables::BTreeNodeTag;

pub const PAGE_SIZE: usize = 4096;

#[repr(C)]
pub union BTreeNode {
    pub raw_bytes: [u8; PAGE_SIZE],
    pub basic: BasicNode,
    pub hash_leaf: ManuallyDrop<HashLeaf>,
    pub u64_head_node: ManuallyDrop<U64HeadNode>,
    pub u32_head_node: ManuallyDrop<U32HeadNode>,
    pub uninit: (),
}

impl BTreeNode {
    pub fn write_inner<N: InnerConversionSink>(&mut self, src: N) -> &mut N {
        unsafe {
            ptr::copy_nonoverlapping((&src) as *const N as *const Self, self, 1);
            mem::forget(src);
            transmute::<&mut Self, _>(self)
        }
    }

    pub unsafe fn new_uninit() -> Self {
        BTreeNode { uninit: () }
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

    pub unsafe fn alloc() -> *mut BTreeNode {
        Box::into_raw(Box::new(BTreeNode::new_uninit()))
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
                FenceData {
                    lower_fence: PrefixTruncatedKey(&[]),
                    upper_fence: PrefixTruncatedKey(&[]),
                    prefix_len: 0,
                },
                child,
            ));
            //(*node).basic = BasicNode::new_inner(child);
            node
        }
    }

    /// merge into right,
    ///self is discarded after this
    pub unsafe fn try_merge_right(
        &self,
        right: &mut BTreeNode,
        separator: FatTruncatedKey,
    ) -> Result<(), ()> {
        debug_assert!(self.is_underfull());
        if right.tag().is_leaf() {
            debug_assert!(right.is_underfull());
        }
        match (self.tag(), right.tag()) {
            (BTreeNodeTag::HashLeaf, BTreeNodeTag::HashLeaf) => self.hash_leaf.try_merge_right(&mut (*right).hash_leaf, separator),
            (BTreeNodeTag::BasicLeaf, BTreeNodeTag::BasicLeaf) => self.basic.merge_right(false, &mut *right, separator),
            (lt, rt) => {
                debug_assert!(lt.is_inner());
                debug_assert!(rt.is_inner());
                merge_to_right::<BasicNode>(self, right, separator)
            }
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
            BTreeNodeTag::U64HeadNode => unreachable!(),
            BTreeNodeTag::U32HeadNode => unreachable!(),
        }
    }
}
