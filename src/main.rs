use std::mem::size_of;
use btree::btree_node::BTreeNode;

fn main(){
    dbg!(size_of::<BTreeNode>());
}