use std::ptr;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use crate::inner_node::{InnerNode, LeafNode, Node};
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use crate::basic_node::BasicNode;
use crate::BTreeNode;
use std::ptr::DynMetadata;
use crate::art_node::ArtNode;
use crate::hash_leaf::HashLeaf;
use crate::head_node::{AsciiHeadNode, U32ExplicitHeadNode, U32ZeroPaddedHeadNode, U64ExplicitHeadNode, U64ZeroPaddedHeadNode};

static mut INNER_VTABLES: [MaybeUninit<DynMetadata<dyn InnerNode>>; 7] = [MaybeUninit::uninit(); 7];
static mut LEAF_VTABLES: [MaybeUninit<DynMetadata<dyn LeafNode>>; 2] = [MaybeUninit::uninit(); 2];
static mut NODE_VTABLES: [MaybeUninit<DynMetadata<dyn Node>>; 14] = [MaybeUninit::uninit(); 14];

/// must be called before BTreeNode methods are used
pub fn init_vtables() {
    fn make_leaf_vtables<N: LeafNode>(tag: BTreeNodeTag) {
        let tag: u8 = tag.into();
        let tag = tag as usize;
        assert!(tag % 2 == 0);
        let ptr: *mut N = ptr::null_mut();
        unsafe {
            LEAF_VTABLES[tag / 2].write(ptr::metadata(ptr as *mut (dyn LeafNode)));
            NODE_VTABLES[tag].write(ptr::metadata(ptr as *mut dyn Node));
        }
    }

    fn make_inner_vtables<N: InnerNode>(tag: BTreeNodeTag) {
        let tag: u8 = tag.into();
        let tag = tag as usize;
        assert!(tag % 2 == 1);
        let ptr: *mut N = ptr::null_mut();
        unsafe {
            INNER_VTABLES[tag / 2].write(ptr::metadata(ptr as *mut (dyn InnerNode)));
            NODE_VTABLES[tag].write(ptr::metadata(ptr as *mut dyn Node));
        }
    }
    make_leaf_vtables::<BasicNode>(BTreeNodeTag::BasicLeaf);
    make_leaf_vtables::<HashLeaf>(BTreeNodeTag::HashLeaf);

    make_inner_vtables::<BasicNode>(BTreeNodeTag::BasicInner);
    make_inner_vtables::<U32ExplicitHeadNode>(BTreeNodeTag::U32ExplicitHead);
    make_inner_vtables::<U64ExplicitHeadNode>(BTreeNodeTag::U64ExplicitHead);
    make_inner_vtables::<U32ZeroPaddedHeadNode>(BTreeNodeTag::U32ZeroPaddedHead);
    make_inner_vtables::<U64ZeroPaddedHeadNode>(BTreeNodeTag::U64ZeroPaddedHead);
    make_inner_vtables::<AsciiHeadNode>(BTreeNodeTag::AsciiHead);
    make_inner_vtables::<ArtNode>(BTreeNodeTag::ArtInner);
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Clone, Copy, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum BTreeNodeTag {
    BasicLeaf = 0,
    BasicInner = 1,
    HashLeaf = 2,
    U64ExplicitHead = 3,
    U32ExplicitHead = 5,
    U64ZeroPaddedHead = 7,
    U32ZeroPaddedHead = 9,
    AsciiHead = 11,
    ArtInner = 13,
}

impl BTreeNodeTag {
    pub fn is_leaf(&self) -> bool {
        let t: u8 = (*self).into();
        t % 2 == 0
    }

    pub fn is_inner(&self) -> bool {
        !self.is_leaf()
    }
}

impl Deref for BTreeNode {
    type Target = dyn Node;

    fn deref(&self) -> &Self::Target {
        unsafe {
            &*ptr::from_raw_parts(
                self as *const Self as *const (),
                NODE_VTABLES[self.tag() as usize].assume_init(),
            )
        }
    }
}

impl DerefMut for BTreeNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            &mut *ptr::from_raw_parts_mut(
                self as *mut Self as *mut (),
                NODE_VTABLES[self.tag() as usize].assume_init(),
            )
        }
    }
}

impl BTreeNode {
    pub fn to_inner(&self) -> &dyn InnerNode {
        unsafe {
            debug_assert!(self.tag().is_inner());
            let vtable = INNER_VTABLES[self.tag() as usize / 2].assume_init();
            &*ptr::from_raw_parts(
                self as *const Self as *const (),
                vtable,
            )
        }
    }

    pub fn to_inner_mut(&mut self) -> &mut dyn InnerNode {
        unsafe {
            debug_assert!(self.tag().is_inner());
            let vtable = INNER_VTABLES[self.tag() as usize / 2].assume_init();
            &mut *ptr::from_raw_parts_mut(
                self as *mut Self as *mut (),
                vtable,
            )
        }
    }

    pub fn to_leaf(&self) -> &dyn LeafNode {
        unsafe {
            debug_assert!(self.tag().is_leaf());
            let vtable = LEAF_VTABLES[self.tag() as usize / 2].assume_init();
            &*ptr::from_raw_parts(
                self as *const Self as *const (),
                vtable,
            )
        }
    }

    pub fn to_leaf_mut(&mut self) -> &mut dyn LeafNode {
        unsafe {
            debug_assert!(self.tag().is_leaf());
            let vtable = LEAF_VTABLES[self.tag() as usize / 2].assume_init();
            &mut *ptr::from_raw_parts_mut(
                self as *mut Self as *mut (),
                vtable,
            )
        }
    }
}
