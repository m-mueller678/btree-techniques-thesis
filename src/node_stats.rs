use counter::Counter;
use crate::{BTree, BTreeNode};
use crate::vtables::BTreeNodeTag;

pub struct InnerNodeData {
    pub depth: usize,
    pub prefix_len: usize,
    pub fences: [Vec<u8>; 2],
    pub keys: Vec<Vec<u8>>,
    pub tag: BTreeNodeTag,
}

fn total_node_count(stats: &[InnerNodeData]) -> usize {
    let max_depth = stats.iter().map(|n| n.depth).max().unwrap();
    let leaf_count: usize = stats.iter().filter(|n| n.depth == max_depth).map(|n| n.keys.len() + 1).sum();
    let lc2 = stats.iter().map(|n| n.keys.len() + 1).sum::<usize>() + 1;
    assert_eq!(leaf_count + stats.len(), lc2);
    lc2 + stats.len()
}

pub fn btree_to_inner_node_stats(b_tree: &BTree) -> Vec<InnerNodeData> {
    let mut ret = Vec::new();
    fn visit(node: &BTreeNode, depth: usize, out: &mut Vec<InnerNodeData>) {
        let mut buffer = [0u8; 1 << 12];
        if node.tag().is_leaf() {
            return;
        }
        let tag = node.tag();
        let node = node.to_inner();
        let fences = node.fences();
        let mut data = InnerNodeData {
            depth,
            prefix_len: fences.prefix_len,
            fences: [fences.lower_fence.0.to_vec(), fences.upper_fence.0.to_vec()],
            keys: vec![],
            tag,
        };
        for i in 0..node.key_count() {
            let key_len = node.get_key(i, &mut buffer, 0).unwrap();
            data.keys.push(buffer[buffer.len() - key_len..].to_vec());
        }
        out.push(data);
        assert!(out.len() < 1_000_000);
        for i in 0..node.key_count() + 1 {
            visit(unsafe { &*node.get_child(i) }, depth + 1, out)
        }
    }
    visit(unsafe { &*b_tree.root }, 0, &mut ret);
    ret
}

pub fn print_tag_counts(b_tree: &BTree) {
    let mut counter = Counter::new();
    fn visit(node: &BTreeNode, depth: usize, out: &mut Counter<BTreeNodeTag>) {
        out.update(std::iter::once(node.tag()));
        if node.tag().is_leaf() {
            return;
        }
        let node = node.to_inner();
        for i in 0..node.key_count() + 1 {
            visit(unsafe { &*node.get_child(i) }, depth + 1, out)
        }
    }
    visit(unsafe { &*b_tree.root }, 0, &mut counter);
    eprintln!("tag counts: {:?}", counter.most_common());
    eprintln!("op counts: {:?}", b_tree.operations.most_common());
}


pub fn print_stats(b_tree: &BTree) {
    let nodes = btree_to_inner_node_stats(b_tree);
    let tag_counts: counter::Counter<_> = nodes.iter().map(|n| n.tag).collect();
    eprintln!("tag counts:");
    for (l, c) in tag_counts.most_common() {
        eprintln!("\t{:40?}|{:8}|{:5.2}%", l, c, c as f64 / nodes.len() as f64 * 100.0)
    };
    eprintln!("height: {:?}", nodes.iter().map(|n| n.depth).max().unwrap() + 2);
    let inner_length_counts: Counter<_> = nodes.iter().flat_map(|n| n.keys.iter().map(|k| k.len())).collect();
    let total_inner_keys: usize = inner_length_counts.total();
    eprintln!("average inner key length: {:6.2}", inner_length_counts.iter().map(|(l, c)| l * c).sum::<usize>() as f64 / total_inner_keys as f64);
    eprintln!("common inner key lengths:");
    for (l, c) in inner_length_counts.k_most_common_ordered(10) {
        eprintln!("\t{:3}: {:5.2}%", l, c as f64 / total_inner_keys as f64 * 100.0)
    };
    eprintln!("node count: {}", total_node_count(&nodes));
}