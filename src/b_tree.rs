use crate::{BTreeNode, op_count, PAGE_SIZE};
use std::ptr;
use crate::branch_cache::BranchCacheAccessor;
use crate::util::trailing_bytes;
use op_count::count_op;


pub struct BTree {
    pub root: *mut BTreeNode,
    branch_cache: BranchCacheAccessor,
}

impl BTree {
    pub fn new() -> Self {
        count_op();
        BTree {
            root: BTreeNode::new_leaf(),
            branch_cache: BranchCacheAccessor::new(),
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn insert(&mut self, key: &[u8], payload: &[u8]) {
        count_op();
        assert!((key.len() + payload.len()) as usize <= PAGE_SIZE / 4);
        unsafe {
            let (node, parent, pos) = (&mut *self.root).descend(key, |_| false, &mut self.branch_cache);
            let node = &mut *node;
            if node.to_leaf_mut().insert(key, payload).is_ok() {
                return;
            }
            self.split_node(node, parent, key, pos);
            self.insert(key, payload);
        }
    }

    #[tracing::instrument(skip(self))]
    pub unsafe fn lookup(&mut self, payload_len_out: *mut u64, key: &[u8]) -> *mut u8 {
        count_op();
        tracing::info!("lookup {key:?}");
        let (node, _, _) = (*self.root).descend(key, |_| false, &mut self.branch_cache);
        let node = &mut *node;
        if let Some(data) = node.to_leaf_mut().lookup(key) {
            ptr::write(payload_len_out, data.len() as u64);
            data.as_mut_ptr()
        } else {
            ptr::null_mut()
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
        count_op();
        if parent.is_null() {
            parent = BTreeNode::new_inner(node);
            self.root = parent;
        }
        let success = (*node).split_node((&mut *parent).to_inner_mut(), index_in_parent, key);
        self.validate();
        if success.is_err() {
            self.ensure_space(parent, key);
        }
    }

    #[tracing::instrument(skip(self))]
    unsafe fn ensure_space(&mut self, to_split: *mut BTreeNode, key: &[u8]) {
        let (node, parent, pos) = (*self.root).descend(key, |n| n == to_split, &mut self.branch_cache);
        debug_assert!(node == to_split);
        self.split_node(to_split, parent, key, pos);
    }

    unsafe fn validate(&self) {
        #[cfg(debug_assertions)]
        {
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
        count_op();
        let mut merge_target: *mut BTreeNode = ptr::null_mut();
        loop {
            let (node, parent, index) = (&mut *self.root).descend(key, |n| n == merge_target, &mut self.branch_cache);
            if merge_target.is_null() {
                let not_found = (&mut *node).to_leaf_mut().remove(key).is_none();
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
            if (*parent).to_inner_mut().merge_children_check(index).is_ok() && (*parent).is_underfull() {
                (&mut *parent).adaption_state().set_adapted(false);
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

    pub fn range_lookup(&mut self, initial_start: &[u8], key_out: *mut u8, callback: &mut dyn FnMut(usize, &[u8]) -> bool) {
        count_op();
        let mut get_key_buffer = [0u8; PAGE_SIZE / 4];
        let mut start_key_buffer = [0u8; PAGE_SIZE / 4];
        start_key_buffer[..initial_start.len()].copy_from_slice(initial_start);
        let mut start_key_len = initial_start.len();

        loop {
            self.branch_cache.reset();
            let mut parent = None;
            let mut node = unsafe { &mut *self.root };
            let mut index = 0;
            loop {
                if node.tag().is_inner() {
                    let node_inner = node.to_inner_mut();
                    index = node_inner.find_child_index(&start_key_buffer[..start_key_len], &mut self.branch_cache);
                    let child = unsafe { &mut *node_inner.get_child(index) };
                    parent = Some(node_inner);
                    node = child;
                } else {
                    unsafe {
                        if !node.to_leaf_mut().range_lookup(&start_key_buffer[..start_key_len], key_out, callback) {
                            return;
                        }
                        if let Some(p) = parent {
                            let fence_data = p.fences();
                            let count = p.key_count();
                            let upper = if index < count {
                                let upper_len = p.get_key(index, &mut get_key_buffer, 0).unwrap();
                                trailing_bytes(&get_key_buffer, upper_len)
                            } else {
                                fence_data.upper_fence.to_stripped(fence_data.prefix_len).0
                            };
                            if upper.is_empty() {
                                return;
                            }
                            start_key_buffer[fence_data.prefix_len..][..upper.len()].copy_from_slice(upper);
                            start_key_buffer[fence_data.prefix_len + upper.len()] = 0;
                            start_key_len = fence_data.prefix_len + upper.len() + 1;
                        } else {
                            return;
                        }
                    }
                    break;
                }
            }
        }
    }

    pub fn range_lookup_desc(&mut self, initial_start: &[u8], key_out: *mut u8, callback: &mut dyn FnMut(usize, &[u8]) -> bool) {
        count_op();
        let mut get_key_buffer = [0u8; PAGE_SIZE / 4];
        let mut start_key_buffer = [0u8; PAGE_SIZE / 4];
        start_key_buffer[..initial_start.len()].copy_from_slice(initial_start);
        let mut start_key_len = initial_start.len();

        loop {
            self.branch_cache.reset();
            let mut parent = None;
            let mut node = unsafe { &mut *self.root };
            let mut index = 0;
            loop {
                if node.tag().is_inner() {
                    let node_inner = node.to_inner_mut();
                    index = node_inner.find_child_index(&start_key_buffer[..start_key_len], &mut self.branch_cache);
                    let child = unsafe { &mut *node_inner.get_child(index) };
                    parent = Some(node_inner);
                    node = child;
                } else {
                    unsafe {
                        if !node.to_leaf_mut().range_lookup_desc(&start_key_buffer[..start_key_len], key_out, callback) {
                            return;
                        }
                        if let Some(p) = parent {
                            let fence_data = p.fences();
                            let count = p.key_count();
                            let lower = if index > 0 {
                                let upper_len = p.get_key(index - 1, &mut get_key_buffer, 0).unwrap();
                                trailing_bytes(&get_key_buffer, upper_len)
                            } else {
                                fence_data.lower_fence.to_stripped(fence_data.prefix_len).0
                            };
                            if lower.is_empty() {
                                return;
                            }
                            start_key_buffer[fence_data.prefix_len..][..lower.len()].copy_from_slice(lower);
                            start_key_len = fence_data.prefix_len + lower.len();
                        } else {
                            return;
                        }
                    }
                    break;
                }
            }
        }
    }
}
