use std::{ptr, slice};
use crate::basic_node::BasicNode;
use crate::btree_node::{BTreeNode, BTreeNodeTag};

pub mod btree_node;
pub mod basic_node;
pub mod util;

pub struct BTree {
    root: Box<BTreeNode>,
}

#[no_mangle]
pub extern "C" fn btree_new() -> *mut BTree {
    Box::leak(Box::new(BTree {
        root: Box::new(BTreeNode {
            basic: BasicNode::init_leaf(),
        })
    }))
}

#[no_mangle]
pub unsafe extern "C" fn btree_insert(b_tree: *mut BTree, key: *const u8, key_len: u64, payload: *const u8, paylod_len: u64) -> bool {
    #[cfg(debug_assertions)]
    println!("debug");
    println!("insert");
    true
}

#[no_mangle]
pub unsafe extern "C" fn btree_lookup(b_tree: *mut BTree, key: *const u8, key_len: u64, payload_len_out: *mut u64) -> *const u8 {
    let key = slice::from_raw_parts(key, key_len as usize);
    let b_tree = &mut *b_tree;
    let (node, _, _) = b_tree.root.descend(key, |_| false);
    let node = &*node;
    match node.tag() {
        BTreeNodeTag::BasicInner => unreachable!(),
        BTreeNodeTag::BasicLeaf => {
            let node = &node.basic;
            let (index, found) = node.lower_bound(key);
            if found {
                let slice = node.slots()[index].value(node.as_bytes());
                ptr::write(payload_len_out, slice.len() as u64);
                slice.as_ptr()
            } else {
                ptr::null()
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn btree_remove(b_tree: *mut BTree, key: *const u8, key_len: u64) -> bool {
    println!("remove");
    true
}

#[no_mangle]
pub unsafe extern "C" fn btree_destroy(b_tree: *mut BTree) {
    drop(Box::<BTree>::from_raw(b_tree));
}
