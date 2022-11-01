#![feature(portable_simd)]
#![feature(pointer_is_aligned)]
#![feature(int_roundings)]
extern crate core;

use crate::btree_node::{BTreeNode, BTreeNodeTag, PAGE_SIZE};
use std::{ptr, slice};

pub mod basic_node;
pub mod btree_node;
mod find_separator;
pub mod hash_leaf;
pub mod head_node;
pub mod util;

pub struct BTree {
    root: *mut BTreeNode,
}

#[cfg(debug_assertions)]
static OP_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn count_op() {
    #[cfg(debug_assertions)]{
        OP_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(debug_assertions)]
#[allow(dead_code)]
extern "C" fn op_count() -> usize {
    OP_COUNT.load(std::sync::atomic::Ordering::Relaxed)
}


#[no_mangle]
pub extern "C" fn btree_new() -> *mut BTree {
    count_op();
    Box::leak(Box::new(BTree {
        root: BTreeNode::new_leaf(),
    }))
}

impl BTree {
    fn insert(&mut self, key: &[u8], payload: &[u8]) {
        assert!((key.len() + payload.len()) as usize <= PAGE_SIZE / 4);
        unsafe {
            let (node, parent, pos) = (&mut *self.root).descend(key, |_| false);
            let node = &mut *node;
            match node.tag() {
                BTreeNodeTag::BasicInner => unreachable!(),
                BTreeNodeTag::U64HeadNode => unreachable!(),
                BTreeNodeTag::U32HeadNode => unreachable!(),
                BTreeNodeTag::BasicLeaf => {
                    if node.basic.insert(key, payload).is_ok() {
                        //self.validate();
                        return;
                    }
                    // node is full: split and restart
                    self.split_node(node, parent, key, pos);
                    self.insert(key, payload);
                }
                BTreeNodeTag::HashLeaf => {
                    if node.hash_leaf.insert(key, payload).is_ok() {
                        //self.validate();
                        return;
                    }
                    // node is full: split and restart
                    self.split_node(node, parent, key, pos);
                    self.insert(key, payload);
                }
            }
        }
    }

    unsafe fn split_node(
        &mut self,
        node: *mut BTreeNode,
        mut parent: *mut BTreeNode,
        key: &[u8],
        index_in_parent: usize,
    ) {
        if parent.is_null() {
            parent = BTreeNode::new_inner(node);
            self.root = parent;
        }
        let success = match (*node).tag() {
            BTreeNodeTag::BasicLeaf | BTreeNodeTag::BasicInner => {
                (*node).basic.split_node(&mut *parent, index_in_parent, key)
            }
            BTreeNodeTag::HashLeaf => {
                (&mut *node)
                    .hash_leaf
                    .split_node(&mut *parent, index_in_parent, key)
            }
            BTreeNodeTag::U64HeadNode => {
                (&mut *node)
                    .u64_head_node
                    .split_node(&mut *parent, index_in_parent, key)
            }
            BTreeNodeTag::U32HeadNode => {
                (&mut *node)
                    .u32_head_node
                    .split_node(&mut *parent, index_in_parent, key)
            }
        };
        self.validate();
        if success.is_err() {
            self.ensure_space(parent, key);
        }
    }

    unsafe fn ensure_space(&mut self, to_split: *mut BTreeNode, key: &[u8]) {
        let (node, parent, pos) = (*self.root).descend(key, |n| n == to_split);
        debug_assert!(node == to_split);
        self.split_node(to_split, parent, key, pos);
    }

    #[allow(unused_variables)]
    unsafe fn validate(&self) {
        // this is very slow for large trees
        const DO_TREE_VALIDATION: bool = false;
        if DO_TREE_VALIDATION {
            (*self.root).validate_tree(&[], &[]);
        }
    }
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
    let (node, _, _) = (*b_tree.root).descend(key, |_| false);
    let node = &*node;
    match node.tag() {
        BTreeNodeTag::BasicInner => unreachable!(),
        BTreeNodeTag::U64HeadNode => todo!(),
        BTreeNodeTag::U32HeadNode => todo!(),
        BTreeNodeTag::BasicLeaf => {
            let node = &node.basic;
            let (index, found) = node.lower_bound(node.truncate(key));
            if found {
                let slice = node.slots()[index].value(node.as_bytes());
                ptr::write(payload_len_out, slice.len() as u64);
                slice.as_ptr()
            } else {
                ptr::null()
            }
        }
        BTreeNodeTag::HashLeaf => {
            if let Some(val) = node.hash_leaf.lookup(key) {
                *payload_len_out = val.len() as u64;
                val.as_ptr()
            } else {
                ptr::null()
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn btree_remove(b_tree: *mut BTree, key: *const u8, key_len: u64) -> bool {
    count_op();
    let key = slice::from_raw_parts(key, key_len as usize);
    let b_tree = &mut *b_tree;
    let mut merge_target: *mut BTreeNode = ptr::null_mut();
    loop {
        let (node, parent, index) = (&mut *b_tree.root).descend(key, |n| n == merge_target);
        if merge_target.is_null() {
            let not_found = (*node).remove(key).is_none();
            b_tree.validate();
            if not_found {
                return false; // todo validate
            }
            if (*node).is_underfull() {
                merge_target = node;
            } else {
                return true;
            }
        }
        debug_assert!(merge_target == node);
        if parent.is_null() {
            break;
        }
        if (*parent).try_merge_child(index).is_ok() && (*parent).is_underfull() {
            b_tree.validate();
            merge_target = parent;
            continue;
        } else {
            b_tree.validate();
            break;
        }
    }
    true
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
