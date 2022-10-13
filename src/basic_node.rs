use crate::btree_node::{BTreeInner, BTreeLeaf, BTreeNode};

#[derive(Clone,Copy)]
pub struct BasicNode{}

impl BTreeLeaf for BasicNode{
    unsafe fn destroy(this: &mut BTreeNode) -> () {
        todo!()
    }

    unsafe fn space_needed(this: &BTreeNode, key_length: usize, payloadLength: usize) -> usize {
        todo!()
    }

    unsafe fn request_space(this: &mut BTreeNode, space: usize) -> bool {
        todo!()
    }

    unsafe fn split_node(this: &mut BTreeNode, parent: &BTreeNode) -> () {
        todo!()
    }

    unsafe fn remove(this: &mut BTreeNode, key: &[u8]) -> Result<(), ()> {
        todo!()
    }

    unsafe fn is_underfull(this: &BTreeNode) -> bool {
        todo!()
    }

    unsafe fn merge_right(this: &mut BTreeNode, separator: &[u8], separator_prefix_len: usize, right: &mut BTreeNode) -> Result<(), ()> {
        todo!()
    }
}

impl BTreeInner for BasicNode{
    unsafe fn destroy(this: &mut BTreeNode) -> () {
        todo!()
    }

    unsafe fn space_needed(this: &BTreeNode, key_length: usize) -> usize {
        todo!()
    }

    unsafe fn request_space(this: &mut BTreeNode, space: usize) -> bool {
        todo!()
    }

    unsafe fn insert(this: &mut BTreeNode, key: &[u8], child: Box<BTreeNode>) -> Result<(), Box<BTreeNode>> {
        todo!()
    }

    unsafe fn split_node(this: &mut BTreeNode, parent: &BTreeNode) -> () {
        todo!()
    }

    unsafe fn remove(this: &mut BTreeNode, key: &[u8]) -> Option<()> {
        todo!()
    }

    unsafe fn is_underfull(this: &BTreeNode) -> bool {
        todo!()
    }

    unsafe fn merge_children_check(this: &mut BTreeNode, child: usize) -> bool {
        todo!()
    }

    unsafe fn mergeRight(this: &mut BTreeNode, separator: &[u8], separator_prefix_len: usize, right: &mut BTreeNode) -> Result<(), ()> {
        todo!()
    }
}