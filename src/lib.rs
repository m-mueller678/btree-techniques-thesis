#![feature(portable_simd)]
#![feature(pointer_is_aligned)]
#![feature(int_roundings)]
#![feature(ptr_metadata)]
extern crate core;

use crate::btree_node::{BTreeNode, BTreeNodeTag, PAGE_SIZE};
use crate::inner_node::init_vtables;
use crate::op_count::{count_op, op_late};
use b_tree::BTree;
use std::sync::Once;
use std::{ptr, slice};

pub mod b_tree;
pub mod basic_node;
pub mod btree_node;
mod find_separator;
pub mod hash_leaf;
pub mod head_node;
pub mod inner_node;
pub mod op_count;
pub mod util;

#[no_mangle]
pub extern "C" fn btree_new() -> *mut BTree {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        init_vtables();
    });
    count_op();
    crate::inner_node::init_vtables();
    Box::leak(Box::new(BTree {
        root: BTreeNode::new_leaf(),
    }))
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
) -> *const u8 {
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
