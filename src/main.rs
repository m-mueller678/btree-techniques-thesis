use std::mem::size_of;
use btree::basic_node::BasicNode;

fn main(){
    dbg!(size_of::<BasicNode>());
}