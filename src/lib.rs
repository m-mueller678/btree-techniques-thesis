use crate::btree_node::{BTreeNode, BTreeNodeTag, PAGE_SIZE};
use std::{ptr, slice};

pub mod basic_node;
pub mod btree_node;
pub mod util;

pub struct BTree {
    root: *mut BTreeNode,
}

#[no_mangle]
pub extern "C" fn btree_new() -> *mut BTree {
    Box::leak(Box::new(BTree {
        root: BTreeNode::new_leaf(),
    }))
}

impl BTree {
    fn insert(&mut self, key: &[u8], payload: &[u8]) {
        assert!((key.len() + payload.len()) as usize <= PAGE_SIZE / 4);
        unsafe {
            let (node, parent, _) = (&mut *self.root).descend(key, |_| false);
            let node = &mut *node;
            match node.tag() {
                BTreeNodeTag::BasicInner => unreachable!(),
                BTreeNodeTag::BasicLeaf => {
                    if node.basic.insert(key, payload).is_ok() {
                        return;
                    }
                    // node is full: split and restart
                    self.split_node(node, parent, key);
                    self.insert(key, payload);
                }
            }
        }
    }

    unsafe fn split_node(&mut self, node: *mut BTreeNode, mut parent: *mut BTreeNode, key: &[u8]) {
        if parent.is_null() {
            parent = BTreeNode::new_inner(node);
            self.root = parent;
        }
        let success = match (*node).tag() {
            BTreeNodeTag::BasicLeaf | BTreeNodeTag::BasicInner => {
                (*node).basic.split_node(&mut *parent)
            }
        };
        if success.is_err() {
            self.ensure_space(parent, key);
        }
        self.validate(self.root, &[], &[]);
    }

    unsafe fn ensure_space(&mut self, to_split: *mut BTreeNode, key: &[u8]) {
        let (node, parent, _) = (*self.root).descend(key, |n| n == to_split);
        debug_assert!(node == to_split);
        self.split_node(to_split, parent, key);
    }

    unsafe fn validate(&self, node: *mut BTreeNode, lower_fence: &[u8], upper_fence: &[u8]) {
        if !cfg!(debug_assertions) {
            return;
        }
        let tag = (*node).tag();
        match tag {
            BTreeNodeTag::BasicLeaf | BTreeNodeTag::BasicInner => {
                let node = &(*node).basic;
                node.validate();
                assert_eq!(node.fence(false), lower_fence);
                assert_eq!(node.fence(true), upper_fence);
                if tag.is_inner() {
                    let mut current_lower_fence = lower_fence.to_vec();
                    let mut current_upper_fence = current_lower_fence.clone();
                    for (i, s) in node.slots().iter().enumerate() {
                        assert!(current_upper_fence.len() >= node.prefix().len());
                        current_upper_fence.truncate(node.prefix().len());
                        current_upper_fence.extend_from_slice(s.key(node.as_bytes()));
                        self.validate(node.get_child(i), &current_lower_fence, &current_upper_fence);
                        std::mem::swap(&mut current_lower_fence, &mut current_upper_fence);
                    }
                    self.validate(node.upper(), &current_lower_fence, upper_fence);
                }
            }
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
    let key = slice::from_raw_parts(key, key_len as usize);
    let b_tree = &mut *b_tree;
    let (node, _, _) = (*b_tree.root).descend(key, |_| false);
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
