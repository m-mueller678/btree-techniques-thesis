#![feature(is_sorted)]

use btree::b_tree::BTree;
use btree::{ensure_init, PrefixTruncatedKey};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering};
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro128PlusPlus;
use smallvec::SmallVec;
use btree::head_node::{FullKeyHead, FullKeyHeadNoTag};
use btree::node_stats::{btree_to_inner_node_stats, NodeData};

fn test_head<H: FullKeyHead>(rng: &mut impl Rng, max_fence_len: usize) {
    let mut buffer = [0u8; 1 << 9];
    let mut keys = SmallVec::<[(&[u8], H, bool); 1024]>::new();
    let mut offset = 0;
    rng.fill_bytes(&mut buffer);
    loop {
        let fence_size = rng.gen_range(1..max_fence_len);
        let lookup_size = rng.gen_range(0..max_fence_len * 2);
        if offset + fence_size + lookup_size > buffer.len() {
            break;
        }
        let f1 = &buffer[offset..][..fence_size];
        let l2 = &buffer[offset..][..lookup_size];
        let l1 = &buffer[offset + fence_size..][..lookup_size];
        if let Some(fh1) = H::make_fence_head(PrefixTruncatedKey(f1)) {
            keys.push((f1, fh1, true));
        }
        keys.push((l1, H::make_needle_head(PrefixTruncatedKey(l1)), false));
        keys.push((l2, H::make_needle_head(PrefixTruncatedKey(l2)), false));
        offset += fence_size + lookup_size;
    }

    keys.sort_by(|a, b|
        a.1.cmp(&b.1).then(a.2.cmp(&b.2))
    );
    let mut last_fence = None;
    let mut max_key = [].as_slice();
    for &(k, h, f) in &keys {
        if let Some(last_fence) = last_fence {
            if f && last_fence == k {
                continue;
            };
            assert!(last_fence < k, "{last_fence:?} - {k:?}\n{keys:?}", );
        }
        if f {
            assert!(max_key <= k, "{k:?} - {max_key:?}");
            last_fence = Some(k);
        }
        max_key = max_key.max(k);
    }
}

fn test_thread(id: usize) {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let mut rng = Xoshiro128PlusPlus::seed_from_u64(0x33445566778899aa);
    for _ in 0..id {
        rng.long_jump();
    }
    loop {
        let iterations = rng.gen_range(0..256);
        for _ in 0..iterations {
            test_head::<u32>(&mut rng, 5);
            test_head::<u64>(&mut rng, 9);
        }
        let c = COUNTER.fetch_add(iterations, Ordering::Relaxed);
        let DISPLAY_DIV = 100_000;
        if c / DISPLAY_DIV != (c + iterations) / DISPLAY_DIV {
            eprintln!("{}", (c + iterations) / DISPLAY_DIV);
        }
    }
}

fn main() {
    for i in 0..35 {
        std::thread::spawn(move || test_thread(i));
    }
    test_thread(36);
}
