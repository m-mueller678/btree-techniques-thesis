use btree::basic_node::BasicNode;
use std::mem::size_of;

fn main() {
    dbg!(size_of::<BasicNode>());
}
