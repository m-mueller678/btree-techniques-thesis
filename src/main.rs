use btree::b_tree::BTree;
use btree::{ensure_init};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use btree::node_stats::{btree_to_inner_node_stats, NodeData};

fn data_set_to_nodes(name: &str, val_len: usize) -> Vec<NodeData> {
    let value = vec![24; val_len];
    let f = BufReader::new(File::open(format!("data/{name}")).unwrap());
    let mut b_tree = BTree::new();
    for l in f.lines() {
        b_tree.insert(l.unwrap().as_bytes(), value.as_slice());
    }
    btree_to_inner_node_stats(&b_tree)
}

fn main() {
    ensure_init();
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
