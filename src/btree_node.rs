use crate::basic_node::BasicNode;
use crate::hash_leaf::HashLeaf;
use crate::node_traits::{FenceData, InnerConversionSink, InnerConversionSource, merge_to_right};
use crate::{FatTruncatedKey};
use num_enum::{TryFromPrimitive};
use std::intrinsics::transmute;
use std::mem::{ManuallyDrop};
use std::{mem, ptr};
use std::ops::Range;
use crate::adaptive::{adapt_inner, infrequent};
use crate::art_node::ArtNode;
use crate::branch_cache::BranchCacheAccessor;
use crate::vtables::BTreeNodeTag;
#[allow(unused_imports)]
use crate::head_node;
#[allow(unused_imports)]
use crate::node_traits::FallbackInnerConversionSink;
use crate::util::reinterpret_mut;


#[cfg(feature = "inner_basic")]
pub type DefaultInnerNodeConversionSink = BasicNode;
#[cfg(feature = "inner_art")]
pub type DefaultInnerNodeConversionSink = ArtNode;
#[cfg(feature = "inner_padded")]
pub type DefaultInnerNodeConversionSink = FallbackInnerConversionSink<FallbackInnerConversionSink<head_node::U32ZeroPaddedHeadNode, head_node::U64ZeroPaddedHeadNode>, BasicNode>;
#[cfg(feature = "inner_explicit_length")]
pub type DefaultInnerNodeConversionSink = FallbackInnerConversionSink<FallbackInnerConversionSink<head_node::U32ExplicitHeadNode, head_node::U64ExplicitHeadNode>, BasicNode>;
#[cfg(feature = "inner_ascii")]
pub type DefaultInnerNodeConversionSink = FallbackInnerConversionSink<head_node::AsciiHeadNode, BasicNode>;

#[cfg(feature = "strip-prefix_true")]
pub const STRIP_PREFIX: bool = true;
#[cfg(feature = "strip-prefix_false")]
pub const STRIP_PREFIX: bool = false;

pub const PAGE_SIZE: usize = 4096;

#[repr(C)]
pub union BTreeNode {
    pub raw_bytes: [u8; PAGE_SIZE],
    pub basic: BasicNode,
    pub hash_leaf: ManuallyDrop<HashLeaf>,
    pub uninit: (),
    pub art_node: ManuallyDrop<ArtNode>,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct BTreeNodeHead {
    pub tag: BTreeNodeTag,
    pub adaption_state: AdaptionState,
}

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct AdaptionState(u8);

impl AdaptionState {
    pub fn new() -> Self {
        AdaptionState(0)
    }

    pub fn set_adapted(&mut self, a: bool) {
        self.0 = a as u8;
    }

    pub fn is_adapted(&self) -> bool {
        self.0 != 0
    }
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

    pub fn adaption_state(&mut self) -> &mut AdaptionState {
        unsafe { reinterpret_mut::<u8, AdaptionState>(&mut self.raw_bytes[1]) }
    }

    /// descends to target node, returns target node, parent, and index within parent
    pub fn descend(
        mut self: &mut Self,
        key: &[u8],
        mut filter: impl FnMut(*mut BTreeNode) -> bool,
        bc: &mut BranchCacheAccessor,
    ) -> (*mut BTreeNode, *mut BTreeNode, usize) {
        let mut parent = ptr::null_mut();
        let mut index = 0;
        bc.reset();
        while self.tag().is_inner() && !filter(self) {
            index = self.to_inner().find_child_index(key, bc);
            parent = self;
            if cfg!(feature = "descend-adapt-inner_10") {
                if !self.adaption_state().is_adapted() && infrequent(10) {
                    adapt_inner(self);
                    self.adaption_state().set_adapted(true);
                }
            } else if cfg!(feature = "descend-adapt-inner_100") {
                if !self.adaption_state().is_adapted() && infrequent(100) {
                    adapt_inner(self);
                    self.adaption_state().set_adapted(true);
                }
            } else if cfg!(feature = "descend-adapt-inner_1000") {
                if !self.adaption_state().is_adapted() && infrequent(1000) {
                    adapt_inner(self);
                    self.adaption_state().set_adapted(true);
                }
            } else {
                assert!(cfg!(feature = "descend-adapt-inner_none"))
            }
            self = unsafe { &mut *self.to_inner().get_child(index) };
        }
        (self, parent, index)
    }


    /// returns true if all leaf nodes after self are outside range
    /// bc must be inactive
    pub fn range_lookup(
        &mut self,
        mut lower_inclusive: Option<&[u8]>,
        upper_inclusive: Option<&[u8]>,
        callback: &mut dyn FnMut(&[u8]),
        bc: &mut BranchCacheAccessor,
    ) {
        if self.tag().is_inner() {
            let this = self.to_inner();
            let first_child_index = lower_inclusive.map(|k| this.find_child_index(k, bc)).unwrap_or(0);
            let upper_child_index = upper_inclusive.map(|k| this.find_child_index(k, bc));
            let key_count = this.key_count();
            for child in first_child_index..upper_child_index.unwrap_or(key_count) + 1 {
                let last = upper_child_index == Some(child);
                unsafe {
                    &mut *this.get_child(child)
                }.range_lookup(lower_inclusive, upper_inclusive.filter(|_| last), callback, bc);
                lower_inclusive = None;
            }
        } else {
            self.to_leaf_mut().range_lookup(lower_inclusive, upper_inclusive, callback)
        }
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
            if cfg!(feature = "leaf_hash") {
                (*leaf).hash_leaf = ManuallyDrop::new(HashLeaf::new())
            } else if cfg!(feature = "leaf_basic") {
                (*leaf).basic = BasicNode::new_leaf();
            } else {
                panic!();
            }
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
