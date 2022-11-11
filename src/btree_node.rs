use crate::basic_node::BasicNode;
use crate::hash_leaf::HashLeaf;
use crate::inner_node::{FallbackInnerConversionSink, FenceData, InnerConversionSink, InnerConversionSource, merge_to_right, Node};
use crate::{FatTruncatedKey};
use num_enum::{TryFromPrimitive};
use std::intrinsics::transmute;
use std::mem::{ManuallyDrop};
use std::{mem, ptr};
use std::ops::Range;
use crate::head_node::{AsciiHeadNode, U32ZeroPaddedHeadNode, U64ExplicitHeadNode, U64ZeroPaddedHeadNode};
use crate::vtables::BTreeNodeTag;

#[cfg(feature = "inner_basic")]
pub type DefaultInnerNodeConversionSink = FallbackInnerConversionSink<FallbackInnerConversionSink<U32ZeroPaddedHeadNode, U64ZeroPaddedHeadNode>, BasicNode>;
#[cfg(feature = "inner_padded")]
pub type DefaultInnerNodeConversionSink = FallbackInnerConversionSink<FallbackInnerConversionSink<U32ZeroPaddedHeadNode, U64ZeroPaddedHeadNode>, BasicNode>;
#[cfg(feature = "inner_explicit_length")]
pub type DefaultInnerNodeConversionSink = FallbackInnerConversionSink<FallbackInnerConversionSink<U32ZeroPaddedHeadNode, U64ZeroPaddedHeadNode>, BasicNode>;
#[cfg(feature = "inner_ascii")]
pub type DefaultInnerNodeConversionSink = FallbackInnerConversionSink<AsciiHeadNode, BasicNode>;

pub const PAGE_SIZE: usize = 4096;

#[repr(C)]
pub union BTreeNode {
    pub raw_bytes: [u8; PAGE_SIZE],
    pub basic: BasicNode,
    pub hash_leaf: ManuallyDrop<HashLeaf>,
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
        while self.tag().is_inner() && !filter(self) {
            index = self.to_inner().find_child_index(key);
            parent = self;
            self = unsafe { &mut *self.to_inner().get_child(index) };
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
        struct RootSource {
            child: *mut BTreeNode,
        }
        impl InnerConversionSource for RootSource {
            fn fences(&self) -> FenceData {
                FenceData::empty()
            }

            fn key_count(&self) -> usize {
                0
            }

            fn get_child(&self, index: usize) -> *mut BTreeNode {
                debug_assert_eq!(index, 0);
                self.child
            }

            fn get_key(&self, _index: usize, _dst: &mut [u8], _strip_prefix: usize) -> Result<usize, ()> {
                panic!()
            }

            fn get_key_length_sum(&self, _range: Range<usize>) -> usize {
                0
            }

            fn get_key_length_max(&self, _range: Range<usize>) -> usize {
                0
            }
        }
        unsafe {
            let node = Self::alloc();
            DefaultInnerNodeConversionSink::create(&mut *node, &RootSource { child }).unwrap();
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
}
