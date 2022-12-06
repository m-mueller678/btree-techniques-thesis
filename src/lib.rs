#![feature(portable_simd)]
#![feature(pointer_is_aligned)]
#![feature(int_roundings)]
#![feature(ptr_metadata)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_slice)]
#![feature(is_sorted)]
extern crate core;

use crate::btree_node::{BTreeNode, PAGE_SIZE};
use crate::vtables::init_vtables;
use crate::op_count::count_op;
use b_tree::BTree;
use std::ops::Deref;
use std::slice;
use std::sync::Once;
use crate::node_stats::print_stats;


pub mod b_tree;
pub mod basic_node;
pub mod btree_node;
mod find_separator;
pub mod hash_leaf;
pub mod head_node;
pub mod node_traits;
pub mod op_count;
pub mod util;
mod vtables;
pub mod node_stats;
pub mod art_node;
pub mod adaptive;
pub mod branch_cache;
pub mod bench;

pub fn ensure_init() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        init_vtables();
    });
}

#[no_mangle]
pub extern "C" fn btree_new() -> *mut BTree {
    ensure_init();
    count_op();
    Box::leak(Box::new(BTree::new()))
}

#[no_mangle]
pub unsafe extern "C" fn btree_insert(
    b_tree: *mut BTree,
    key: *const u8,
    key_len: u64,
    payload: *const u8,
    payload_len: u64,
) {
    count_op();
    BTree::insert(
        &mut *b_tree,
        slice::from_raw_parts(key, key_len as usize),
        slice::from_raw_parts(payload, payload_len as usize),
    )
}

#[no_mangle]
pub unsafe extern "C" fn btree_lookup(
    b_tree: *mut BTree,
    key: *const u8,
    key_len: u64,
    payload_len_out: *mut u64,
) -> *mut u8 {
    count_op();
    let key = slice::from_raw_parts(key, key_len as usize);
    let b_tree = &mut *b_tree;
    b_tree.lookup(payload_len_out, key)
}

#[no_mangle]
pub unsafe extern "C" fn btree_remove(b_tree: *mut BTree, key: *const u8, key_len: u64) -> bool {
    count_op();
    let key = slice::from_raw_parts(key, key_len as usize);
    let b_tree = &mut *b_tree;
    b_tree.remove(key)
}

#[no_mangle]
pub unsafe extern "C" fn btree_destroy(b_tree: *mut BTree) {
    count_op();
    drop(Box::<BTree>::from_raw(b_tree));
}

#[no_mangle]
pub unsafe extern "C" fn btree_print_info(b_tree: *mut BTree) {
    count_op();
    if cfg!( debug_assertions ) {
        print_stats(&*b_tree);
    }
}

#[no_mangle]
pub unsafe extern "C" fn btree_scan_asc(b_tree: *mut BTree, key: *const u8, key_len: u64, key_buffer: *mut u8, continue_callback: extern "C" fn(*const u8) -> bool) {
    unimplemented!();
}

#[no_mangle]
pub unsafe extern "C" fn btree_scan_desc(b_tree: *mut BTree, key: *const u8, key_len: u64, key_buffer: *mut u8, continue_callback: extern "C" fn(*const u8) -> bool) {
    unimplemented!();
}


#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Debug)]
pub struct PrefixTruncatedKey<'a>(pub &'a [u8]);

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Debug)]
pub struct HeadTruncatedKey<'a>(pub &'a [u8]);

#[derive(Copy, Clone, Debug)]
pub struct FatTruncatedKey<'a> {
    prefix_len: usize,
    remainder: &'a [u8],
}

impl<'a> FatTruncatedKey<'a> {
    pub fn full(key: &'a [u8]) -> Self {
        FatTruncatedKey {
            prefix_len: 0,
            remainder: key,
        }
    }
}

impl Deref for PrefixTruncatedKey<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}
