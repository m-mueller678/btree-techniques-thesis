use crate::basic_node::BasicNode;
use crate::hash_leaf::HashLeaf;
use crate::node_traits::{FenceData, InnerConversionSink, InnerConversionSource, merge_to_right};
use crate::{FatTruncatedKey};
use num_enum::{TryFromPrimitive};
use std::intrinsics::transmute;
use std::mem::{ManuallyDrop};
use std::{mem, ptr};
use std::hint::black_box;
use std::ops::Range;
use std::simd::Simd;
use std::sync::atomic::Ordering;
use rand::{Rng};
use rand::distributions::Uniform;
use rand::distributions::uniform::{UniformInt, UniformSampler};
use rand::prelude::SliceRandom;
use crate::adaptive::{adapt_inner, infrequent, RAND};
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

#[cfg(feature = "basic-prefix_true")]
pub const BASIC_PREFIX: bool = true;
#[cfg(feature = "basic-prefix_false")]
pub const BASIC_PREFIX: bool = false;

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

const LEAVE_NOTIFY_POINT_WEIGHT: f64 = 0.0083333333333333333333333333333 * LEAVE_ADAPTION_RANGE as f64;
const LEAVE_NOTIFY_RANGE_WEIGHT: f64 = 0.025 * LEAVE_ADAPTION_RANGE as f64;
const LEAVE_KEY_WEIGHT: f64 = 0.01;
#[cfg(feature = "leave-adapt-range_3")]
const LEAVE_ADAPTION_RANGE: u8 = 3;
#[cfg(feature = "leave-adapt-range_7")]
const LEAVE_ADAPTION_RANGE: u8 = 7;
#[cfg(feature = "leave-adapt-range_15")]
const LEAVE_ADAPTION_RANGE: u8 = 15;
#[cfg(feature = "leave-adapt-range_31")]
const LEAVE_ADAPTION_RANGE: u8 = 31;
const BITS_PER_RAND: u32 = 32;
const RAND_BIT: u64 = 1 << BITS_PER_RAND;

impl BTreeNode {
    fn leave_convert_common(&mut self, residual_random: u64) {
        let rand_a = residual_random & (RAND_BIT - 1);
        const KEY_THESHOLD: u64 = (LEAVE_KEY_WEIGHT * RAND_BIT as f64) as u64;
        'key_scan: {
            if rand_a < KEY_THESHOLD {
                type u16x8 = packed_simd_2::u16x8;
                let is_short = match self.tag() {
                    BTreeNodeTag::BasicLeaf => {
                        let slots = unsafe { self.basic.slots() };
                        if slots.len() == 0 {
                            break 'key_scan;
                        }
                        let indices: u16x8 = UniformInt::<u16x8>::sample_single(u16x8::splat(0), u16x8::splat(slots.len() as u16), unsafe { &mut *RAND });
                        (0..u16x8::lanes()).all(|i| slots[indices.extract(i) as usize].key_len <= 4)
                    }
                    BTreeNodeTag::HashLeaf => {
                        let slots = unsafe { self.hash_leaf.slots() };
                        if slots.len() == 0 {
                            break 'key_scan;
                        }
                        let indices: u16x8 = UniformInt::<u16x8>::sample_single(u16x8::splat(0), u16x8::splat(slots.len() as u16), unsafe { &mut *RAND });
                        (0..u16x8::lanes()).all(|i| slots[indices.extract(i) as usize].key_len <= 4)
                    }
                    _ => unreachable!()
                };
                self.head_mut().adaption_state.0 = self.head_mut().adaption_state.0 % 128 + if is_short { 128 } else { 0 };
            }
        }
        match self.tag() {
            BTreeNodeTag::BasicLeaf => if self.head_mut().adaption_state.0 == 0 {
                HashLeaf::from_basic(self);
                eprintln!("ERROR");
            }
            BTreeNodeTag::HashLeaf => if self.head_mut().adaption_state.0 >= LEAVE_ADAPTION_RANGE {
                use std::sync::atomic::*;
                if black_box(false) {
                    let is_err = HashLeaf::to_basic(self).is_err();
                    if cfg!(debug_assertions) {
                        static TOTAL: AtomicUsize = AtomicUsize::new(0);
                        static FAILED: AtomicUsize = AtomicUsize::new(0);
                        let total = TOTAL.fetch_add(1, Ordering::Relaxed);
                        let failed = FAILED.fetch_add(is_err as usize, Ordering::Relaxed);
                        if total % 1024 == 0 {
                            eprintln!("leave to basic convert fail rate: {}", failed as f64 / total as f64);
                        }
                    }
                }
            }
            _ => unreachable!()
        }
    }

    pub fn leave_notify_point_op(&mut self) {
        #[cfg(feature = "leaf_adapt")]{
            const THRESHOLD: u64 = (LEAVE_NOTIFY_POINT_WEIGHT * RAND_BIT as f64) as u64;
            let rand = unsafe { &mut *RAND }.gen::<u64>();
            if rand & (RAND_BIT - 1) < THRESHOLD {
                let head = self.head_mut();
                if head.adaption_state.0 % 128 > 0 {
                    head.adaption_state.0 -= 1;
                }
            }
            self.leave_convert_common(rand >> BITS_PER_RAND)
        }
    }

    pub fn leave_notify_range_op(&mut self) {
        #[cfg(feature = "leaf_adapt")]{
            const THRESHOLD: u64 = (LEAVE_NOTIFY_RANGE_WEIGHT * RAND_BIT as f64) as u64;
            let rand = unsafe { &mut *RAND }.gen::<u64>();
            if rand & (RAND_BIT - 1) < THRESHOLD {
                let head = self.head_mut();
                if head.adaption_state.0 % 128 < LEAVE_ADAPTION_RANGE {
                    head.adaption_state.0 += 1;
                }
            }
            self.leave_convert_common(rand >> BITS_PER_RAND)
        }
    }

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

    pub fn head_mut(&mut self) -> &mut BTreeNodeHead {
        // this method is intended for dynamic leave layout selection
        // interpretation of head is different for inner and leave nodes
        debug_assert!(self.tag().is_leaf());
        unsafe { &mut *(self as *mut BTreeNode as *mut BTreeNodeHead) }
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
            index = self.to_inner_mut().find_child_index(key, bc);
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

    pub unsafe fn alloc() -> *mut BTreeNode {
        Box::into_raw(Box::new(BTreeNode::new_uninit()))
    }

    pub unsafe fn dealloc(node: *mut BTreeNode) {
        drop(Box::from_raw(node));
    }

    pub fn new_leaf() -> *mut BTreeNode {
        unsafe {
            let leaf = Self::alloc();
            if cfg!(feature = "leaf_hash") || cfg!(feature = "leaf_adapt") {
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
        &mut self,
        right: &mut BTreeNode,
        separator: FatTruncatedKey,
    ) -> Result<(), ()> {
        debug_assert!(self.is_underfull());
        if right.tag().is_leaf() {
            debug_assert!(right.is_underfull());
        }
        match (self.tag(), right.tag()) {
            (BTreeNodeTag::BasicLeaf, BTreeNodeTag::BasicLeaf) => self.basic.merge_right(false, &mut *right, separator),
            (lt, rt) => {
                if lt.is_leaf() {
                    if lt == BTreeNodeTag::BasicLeaf {
                        HashLeaf::from_basic(self);
                    }
                    if rt == BTreeNodeTag::BasicLeaf {
                        HashLeaf::from_basic(right);
                    }
                    debug_assert!(self.tag() == BTreeNodeTag::HashLeaf);
                    debug_assert!(right.tag() == BTreeNodeTag::HashLeaf);
                    self.hash_leaf.try_merge_right(&mut (*right).hash_leaf, separator)
                } else {
                    debug_assert!(rt.is_inner());
                    merge_to_right::<BasicNode>(self, right, separator)
                }
            }
        }
    }
}
