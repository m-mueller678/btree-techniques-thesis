use num_enum::{IntoPrimitive, TryFromPrimitive};
use crate::basic_node::BasicNode;

const PAGE_SIZE: usize = 4096;


#[derive(IntoPrimitive,TryFromPrimitive,Debug)]
#[repr(u8)]
pub enum BTreeNodeTag {
    BasicLeaf,
    BasicInner,
}

#[repr(C)]
pub union BTreeNode{
    pub raw_bytes: [u8; PAGE_SIZE],
    pub basic:BasicNode,
}

impl BTreeNode{
    fn tag(&self)->BTreeNodeTag{
        BTreeNodeTag::try_from_primitive(unsafe{self.raw_bytes[0]}).unwrap()
    }
}

macro_rules! tag_dispatch{
    { $this:ident,BTreeLeaf, $func_name:ident($($arg:ident),*)}=>{
        match $this.tag(){
            BTreeNodeTag::BasicLeaf=> <BasicNode as BTreeLeaf>::$func_name($this,$($arg),*),
            x@(
                BTreeNodeTag::BasicInner
            )=> panic!("expected leaf, got {:?}",x),
        }
    };
    { $this:ident,BTreeInner, $func_name:ident($($arg:ident),*)}=>{
        match $this.tag(){
            BTreeNodeTag::BasicInner=> <BasicNode as BTreeInner>::$func_name($this,$($arg),*),
            x@(
                BTreeNodeTag::BasicLeaf
            )=> panic!("expected inner, got {:?}",x),
        }
    }
}

macro_rules! node_types {
    {
        traits: {
            $(
                pub trait $trait_name:ident{
                    $(
                        $(#[$attribute:meta])*
                        unsafe fn $func_name:ident(this:$this_type:ty$(,$arg_name:ident:$arg_type:ty)*)->$ret_type:ty
                    );*
                }
            )*
        }
    }=>{
        $(
            pub trait $trait_name{
                $(
                    $(#[$attribute])*
                    unsafe fn $func_name (this:$this_type$(,$arg_name:$arg_type)*)->$ret_type;
                )*
            }

            impl $trait_name for BTreeNode{
                $(
                    unsafe fn $func_name (this:$this_type$(,$arg_name:$arg_type)*)->$ret_type{
                        tag_dispatch!{this,$trait_name,$func_name($($arg_name),*)}
                    }
                )*
            }
        )*

    }
}

node_types! {
    traits:{
        pub trait BTreeLeaf {
            unsafe fn destroy(this: &mut BTreeNode)->();
            unsafe fn space_needed(this: &BTreeNode, key_length: usize, payloadLength: usize) -> usize;
            unsafe fn request_space(this: &mut BTreeNode, space: usize) -> bool;
            unsafe fn split_node(this: &mut BTreeNode, parent: &BTreeNode)->();
            unsafe fn remove(this: &mut BTreeNode, key: &[u8]) -> Result<(), ()>;
            unsafe fn is_underfull(this: &BTreeNode) -> bool;
            /// merge into right node.
            /// on success self was moved from and should be forgotten
            unsafe fn merge_right(this: &mut BTreeNode, separator: &[u8], separator_prefix_len: usize, right: &mut BTreeNode) -> Result<(), ()>
        }

        pub trait BTreeInner {
            unsafe fn destroy(this: &mut BTreeNode)->();
            unsafe fn space_needed(this: &BTreeNode, key_length: usize) -> usize;
            unsafe fn request_space(this: &mut BTreeNode, space: usize) -> bool;
            unsafe fn insert(this: &mut BTreeNode, key: &[u8], child: Box<BTreeNode>) -> Result<(), Box<BTreeNode>>;
            unsafe fn split_node(this: &mut BTreeNode, parent: &BTreeNode)->();
            unsafe fn remove(this: &mut BTreeNode, key: &[u8]) -> Option<()>;
            unsafe fn is_underfull(this: &BTreeNode) -> bool;
            // merges adjacent children if appropriate
            unsafe fn merge_children_check(this: &mut BTreeNode, child: usize) -> bool;
            /// see [BTreeLeaf::merge_right]
            unsafe fn mergeRight(this: &mut BTreeNode, separator: &[u8], separator_prefix_len: usize, right: &mut BTreeNode) -> Result<(), ()>
        }
    }
}