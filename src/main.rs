use btree::b_tree::BTree;
use btree::btree_node::BTreeNode;
use btree::init;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

struct NodeData {
    depth: usize,
    prefix_len: usize,
    fences: [Vec<u8>; 2],
    keys: Vec<Vec<u8>>,
}

fn data_set_to_nodes(name: &str, val_len: usize) -> Vec<NodeData> {
    let value = vec![24; val_len];
    let f = BufReader::new(File::open(format!("data/{name}")).unwrap());
    let mut b_tree = BTree::new();
    for l in f.lines() {
        b_tree.insert(l.unwrap().as_bytes(), value.as_slice());
    }
    let mut ret = Vec::new();
    fn visit(node: &BTreeNode, depth: usize, out: &mut Vec<NodeData>) {
        let mut buffer = [0u8; 1 << 9];
        if node.tag().is_leaf() {
            return;
        }
        let node = node.to_inner_conversion_source();
        let fences = node.fences();
        let mut data = NodeData {
            depth,
            prefix_len: fences.prefix_len,
            fences: [fences.lower_fence.0.to_vec(), fences.upper_fence.0.to_vec()],
            keys: vec![],
        };
        for i in 0..node.key_count() {
            let key_len = node.get_key(i, &mut buffer, 0).unwrap();
            data.keys.push(buffer[buffer.len() - key_len..].to_vec());
        }
        out.push(data);
        for i in 0..node.key_count() + 1 {
            visit(unsafe { &*node.get_child(i) }, depth + 1, out)
        }
    }
    visit(unsafe { &*b_tree.root }, 0, &mut ret);
    ret
}

fn main() {
    init();
    for name in ["access", "genome", "urls", "wiki"] {
        let mut counts = BTreeMap::new();
        let mut max_counts = BTreeMap::new();
        let mut total = 0;
        let mut node_count = 0;
        for node in data_set_to_nodes(name, 24) {
            node_count += 1;
            *max_counts
                .entry(node.keys.iter().map(|x| x.len()).max().unwrap())
                .or_insert(0usize) += 1;
            for k in &node.keys {
                *counts.entry(k.len()).or_insert(0usize) += 1;
                total += 1;
            }
        }
        dbg!(name, node_count);
        let mut cumulative = 0;
        for (k, v) in counts.iter().take(16) {
            cumulative += *v;
            let frac = cumulative as f64 / total as f64;
            print!("{:4}:{:4.2}%,", k, frac * 100.0);
            if frac > 0.999 {
                break;
            }
        }
        println!();
        let mut cumulative = 0;
        for (k, v) in max_counts.iter().take(16) {
            cumulative += *v;
            let frac = cumulative as f64 / node_count as f64;
            print!("{:4}:{:4.2}%,", k, frac * 100.0);
            if frac > 0.999 {
                break;
            }
        }
        println!();
    }
}
