#![feature(is_sorted)]


use btree::{bench, ensure_init, PrefixTruncatedKey};


use std::hint::black_box;
use std::{fs, ptr};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::BufRead;

use std::time::{Duration, Instant};
use rand::{Rng, RngCore, SeedableRng};
use rand::prelude::SliceRandom;
use rand_xoshiro::Xoshiro128PlusPlus;
use serde_json::json;
use smallvec::SmallVec;
use btree::b_tree::BTree;
use btree::head_node::{AsciiHead, FullKeyHead};
use btree::node_stats::{btree_to_inner_node_stats, total_node_count};
use btree::node_traits::node_print;
use rayon::prelude::*;

pub fn test_head<H: FullKeyHead>(rng: &mut impl Rng, max_fence_len: usize) {
    let mut buffer = [0u8; 1 << 9];
    let mut keys = SmallVec::<[(&[u8], H, bool); 1024]>::new();
    let mut offset = 0;
    rng.fill_bytes(&mut buffer);
    for b in &mut buffer {
        *b &= 127;
    }
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
            assert_eq!(f1, fh1.restore().as_slice());
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
    for &(k, _h, f) in &keys {
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

pub fn test_thread(id: usize) {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let mut rng = Xoshiro128PlusPlus::seed_from_u64(0x33445566778899aa);
    for _ in 0..id {
        rng.long_jump();
    }
    loop {
        let iterations = rng.gen_range(0..256);
        for _ in 0..iterations {
            test_head::<AsciiHead>(&mut rng, 10);
        }
        let c = COUNTER.fetch_add(iterations, Ordering::Relaxed);
        const DISPLAY_DIV: usize = 100_000;
        if c / DISPLAY_DIV != (c + iterations) / DISPLAY_DIV {
            eprintln!("{}", (c + iterations) / DISPLAY_DIV);
        }
    }
}

pub fn perf<H: FullKeyHead>() {
    let mut rng = Xoshiro128PlusPlus::seed_from_u64(0x33445566778899aa);
    let mut buffer = vec![0u8; 1 << 16];
    let mut lens = Vec::new();
    let mut count_acc = 0;
    let mut duration_acc = Duration::ZERO;
    for _ in 0..500 {
        rng.fill_bytes(&mut buffer);
        lens.clear();
        let mut total_len = 0;
        loop {
            let l = rng.gen_range(0..20);
            total_len += l;
            if total_len > buffer.len() { break; }
            lens.push(total_len);
        }
        let start = Instant::now();
        for _ in 0..1000 {
            for range in lens.windows(2) {
                let key = &buffer[range[0]..range[1]];
                black_box(H::make_needle_head(PrefixTruncatedKey(key)));
            }
        }
        duration_acc += start.elapsed();
        count_acc += (lens.len() - 1) * 1000;
    }
    println!("{},{},{}", duration_acc.as_nanos() as f64 / count_acc as f64, cfg!(feature = "use-full-length_true"), std::any::type_name::<H>())
}

fn main() {
    ensure_init();

    let mut data: Option<(Vec<Vec<u8>>, String)> = None;
    if let Ok(var) = std::env::var("INT") {
        assert!(data.is_none());
        let count = var.parse::<f64>().unwrap();
        assert!(count >= 0.0);
        assert!(count < u32::MAX as f64);
        assert!(count.fract() == 0.0);
        let count: u32 = count as u32;
        data = Some(((0..count).map(|x| { x.to_le_bytes().to_vec() }).collect(), format!("INT-{}", count)));
    }
    if let Ok(var) = std::env::var("FILE") {
        assert!(data.is_none());
        let file = std::io::BufReader::new(std::fs::File::open(&var).unwrap());
        data = Some((file.lines().map(|l| { l.unwrap().into_bytes() }).collect(), format!("FILE-{}", var)));
    }
    let (mut keys, data_name) = data.expect("no bench");

    keys.shuffle(&mut Xoshiro128PlusPlus::seed_from_u64(123));
    let chunk: usize = std::env::var("CHUNK").unwrap().parse().unwrap();
    let sizes: Vec<usize> = (chunk * 50..).step_by(10).skip(1).take(5).collect();
    sizes.into_par_iter().for_each(|value_len| {
        let mut tree = BTree::new();
        let value = vec![0u8; value_len];
        for k in &keys {
            tree.insert(k, &value);
        }
        let stats = btree_to_inner_node_stats(&tree);
        let node_count = total_node_count(&stats);
        for k in &keys {
            // drop is not implemented, remove to avoid memory leaks
            unsafe { assert!(tree.remove(k)) };
        }
        #[cfg(feature = "strip-prefix_true")] let variant = "true";
        #[cfg(feature = "strip-prefix_false")] let variant = "false";
        println!("{}", serde_json::to_string(&json!({"value_len":value_len,"data":data_name,"node_count":node_count,"variant":variant})).unwrap());
    });
}
