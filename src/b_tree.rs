use crate::{BTreeNode, BTreeNodeTag, PAGE_SIZE};
use std::ptr;

pub struct BTree {
    pub root: *mut BTreeNode,
}

impl BTree {
    pub fn new() -> Self {
        BTree {
            root: BTreeNode::new_leaf(),
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn insert(&mut self, key: &[u8], payload: &[u8]) {
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

    #[tracing::instrument(skip(self))]
    pub unsafe fn lookup(&mut self, payload_len_out: *mut u64, key: &[u8]) -> *const u8 {
        tracing::info!("lookup {key:?}");
        let (node, _, _) = (*self.root).descend(key, |_| false);
        let node = &*node;
        match node.tag() {
            BTreeNodeTag::BasicInner => unreachable!(),
            BTreeNodeTag::U64HeadNode => unreachable!(),
            BTreeNodeTag::U32HeadNode => unreachable!(),
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

    #[tracing::instrument(skip(self))]
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

    #[tracing::instrument(skip(self))]
    unsafe fn ensure_space(&mut self, to_split: *mut BTreeNode, key: &[u8]) {
        let (node, parent, pos) = (*self.root).descend(key, |n| n == to_split);
        debug_assert!(node == to_split);
        self.split_node(to_split, parent, key, pos);
    }

    unsafe fn validate(&self) {
        #[cfg(debug_assertions)]{
            // this is very slow for large trees
            const DO_TREE_VALIDATION: bool = true;
            if DO_TREE_VALIDATION && crate::op_count::op_late() {
                self.force_validate();
            }
        }
    }

    #[cfg_attr(debug_assertions, no_mangle)]
    #[tracing::instrument(skip(self), level = "debug")]
    unsafe fn force_validate(&self) {
        (*self.root).validate_tree(&[], &[]);
    }

    #[tracing::instrument(skip(self))]
    pub unsafe fn remove(&mut self, key: &[u8]) -> bool {
        let mut merge_target: *mut BTreeNode = ptr::null_mut();
        loop {
            let (node, parent, index) = (&mut *self.root).descend(key, |n| n == merge_target);
            if merge_target.is_null() {
                let not_found = (*node).remove(key).is_none();
                self.validate();
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
            debug_assert!((*node).is_underfull());
            if (*parent).try_merge_child(index).is_ok() && (*parent).is_underfull() {
                self.validate();
                merge_target = parent;
                continue;
            } else {
                self.validate();
                break;
            }
        }
        true
    }
}
