use std::ptr;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use crate::inner_node::{InnerNode, Node};
use std::mem::MaybeUninit;
use std::ops::Deref;
use crate::basic_node::BasicNode;
use crate::BTreeNode;
use crate::head_node::{U32HeadNode, U64HeadNode};
use std::ptr::DynMetadata;
use crate::hash_leaf::HashLeaf;

static mut INNER_VTABLES: [MaybeUninit<DynMetadata<dyn InnerNode>>; 3] = [MaybeUninit::uninit(); 3];
static mut NODE_VTABLES: [MaybeUninit<DynMetadata<dyn Node>>; 6] = [MaybeUninit::uninit(); 6];

/// must be called before BTreeNode methods are used
pub fn init_vtables() {
    fn make_leaf_vtables<N: Node>(tag: BTreeNodeTag) {
        let tag: u8 = tag.into();
        let tag = tag as usize;
        assert!(tag % 2 == 0);
        let ptr: *mut N = ptr::null_mut();
        unsafe {
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
    make_inner_vtables::<U64HeadNode>(BTreeNodeTag::U64HeadNode);
    make_inner_vtables::<U32HeadNode>(BTreeNodeTag::U32HeadNode);
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum BTreeNodeTag {
    BasicLeaf = 0,
    BasicInner = 1,
    HashLeaf = 2,
    U64HeadNode = 3,
    U32HeadNode = 5,
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
}
