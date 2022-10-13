use std::ptr;
use crate::btree_node::BTreeNode;

pub mod btree_node;
pub mod basic_node;

pub struct BTree{
    root:Box<BTreeNode>,
}

#[no_mangle]
pub extern "C" fn btree_new() -> *mut BTree {
    Box::leak(Box::new(BTree {}))
}

#[no_mangle]
pub unsafe extern "C" fn btree_insert(b_tree: *mut BTree, key: *const u8, key_len: u64, payload: *const u8, paylod_len: u64) -> bool {
    #[cfg(debug_assertions)]
    println!("debug");
    println!("insert");
    true
}

#[no_mangle]
pub unsafe extern "C" fn btree_lookup(b_tree: *mut BTree, key: *const u8, key_len: u64, payload_len_out: *mut u64) -> *mut u8 {
    println!("lookup");
    ptr::null_mut()
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
