use std::hint::black_box;
use std::io::BufRead;
use rand::{Rng, RngCore, SeedableRng};
use rand::distributions::Uniform;
use rand::distributions::Distribution;
use rand::prelude::SliceRandom;
use rand_xoshiro::Xoshiro128PlusPlus;
use crate::{BTree, ensure_init};

fn build_info() -> (&'static str, &'static str) {
    let header = include_str!("../build-info.h");
    let parts: Vec<_> = header.split('"').collect();
    (parts[1], parts[3])
}

#[repr(usize)]
#[derive(Clone, Copy)]
enum Op {
    Lookup,
    Insert,
    Remove,
    RangeLookup,
}

#[derive(Default)]
struct StatAggregator {
    sum: u64,
    count: u64,
}

impl StatAggregator {
    fn submit(&mut self, sample: u64) {
        self.sum += sample;
        self.count += 1;
    }

    fn time_fn<R>(&mut self, f: impl FnOnce() -> R) -> R {
        let t1 = minstant::Instant::now();
        let r = f();
        let t2 = minstant::Instant::now();
        self.submit(t2.duration_since(t1).as_nanos() as u64);
        r
    }
}

// prevent inlining to improve perf usability
#[inline(never)]
fn init_bench(value_length: usize, data: &mut [&[u8]]) -> (BTree, Xoshiro128PlusPlus, Vec<u8>) {
    let mut rng = Xoshiro128PlusPlus::from_entropy();
    assert!(minstant::is_tsc_available());
    let core_id = core_affinity::get_core_ids().unwrap().choose(&mut rng).cloned().unwrap();
    assert!(core_affinity::set_for_current(core_id));
    let mut value = vec![0u8; value_length];
    rng.fill_bytes(&mut value);
    let mut tree = BTree::new();
    data.shuffle(&mut rng);
    for k in &data[..data.len() / 2] {
        tree.insert(k, &value);
    }
    data.sort_unstable();
    (tree, rng, value)
}

fn bench(op_count: usize,
         range_ratio: f64,
         modify_ratio: f64,
         locality: f64,
         value_length: usize,
         data: &mut [&[u8]],
) -> [StatAggregator; 4] {
    let modify_probability = modify_ratio * 2.0 / (1.0 - range_ratio);
    let (mut tree, mut rng, value) = init_bench(value_length, data);
    let rng = &mut rng;
    let mut stats: [StatAggregator; 4] = Default::default();
    let index_distribution = Uniform::new(0, data.len());
    let local_distribution = Uniform::new(0, 20);
    let mut index = 0;
    for _ in 0..op_count {
        if rng.gen_bool(locality) {
            index = (index + local_distribution.sample(rng)).min(data.len() - 1);
        } else {
            index = index_distribution.sample(rng);
        }
        if rng.gen_bool(range_ratio) {
            let range_start = index.saturating_sub(20);
            let mut count = 0;
            stats[Op::RangeLookup as usize].time_fn(|| black_box(tree.range_lookup(black_box(data[range_start]..=data[index]), black_box(&mut |_| { count += 1; }))));
            debug_assert!(count <= index - range_start + 1);
            if cfg!(debug_assertions) {
                let point_count = data[range_start..=index].iter().filter(|k| {
                    let mut out = 0;
                    !unsafe { tree.lookup(&mut out, k).is_null() }
                }).count();
                assert_eq!(point_count, count);
            }
        } else {
            let mut out = 0;
            let index = index_distribution.sample(rng);
            unsafe {
                let has_key = stats[Op::Lookup as usize].time_fn(|| !black_box(tree.lookup(black_box(&mut out), black_box(data[index])).is_null()));
                if rng.gen_bool(modify_probability) {
                    if has_key {
                        stats[Op::Remove as usize].time_fn(|| black_box(tree.remove(black_box(data[index]))));
                    } else {
                        stats[Op::Insert as usize].time_fn(|| black_box(tree.insert(black_box(data[index]), black_box(&value))));
                    }
                }
            }
        }
    }
    stats
}

pub fn bench_main() {
    ensure_init();
    let bump = bumpalo::Bump::new();
    let mut data: Option<(Vec<&[u8]>, String)> = None;
    if let Ok(var) = std::env::var("INT") {
        assert!(data.is_none());
        let count = var.parse::<f64>().unwrap();
        assert!(count >= 0.0);
        assert!(count < u32::MAX as f64);
        assert!(count.fract() == 0.0);
        let count: u32 = count as u32;
        data = Some(((0..count).map(|x| {
            &*bump.alloc_slice_copy(&x.to_le_bytes())
        }).collect(), format!("INT-{}", count)));
    }
    if let Ok(var) = std::env::var("FILE") {
        assert!(data.is_none());
        let file = std::io::BufReader::new(std::fs::File::open(&var).unwrap());
        data = Some((file.lines().map(|l| {
            &*bump.alloc_slice_copy(l.unwrap().as_bytes())
        }).collect(), format!("FILE-{}", var)));
    }
    let (mut keys, data_name) = data.expect("no bench");
    let total_count = std::env::var("OP_COUNT").map(|x| x.parse().unwrap()).unwrap_or(1e6) as usize;
    let range_ratio = std::env::var("RANGE_RATIO").map(|x| x.parse().unwrap()).unwrap_or(0.2);
    let locality = std::env::var("LOCALITY").map(|x| x.parse().unwrap()).unwrap_or(0.2);
    let modify_ratio = std::env::var("MODIFY_RATIO").map(|x| x.parse().unwrap()).unwrap_or(0.3);
    let value_length = std::env::var("VALUE_SIZE").map(|x| x.parse().unwrap()).unwrap_or(8);
    let stats = bench(total_count, range_ratio, modify_ratio, locality, value_length, &mut keys);
    let (build_header, build_values) = build_info();
    println!("bench,op,total_count,op_count,range_ratio,locality,modify_ratio,value_length,average time{}", build_header);
    for op in [Op::Lookup, Op::RangeLookup, Op::Remove, Op::Insert] {
        let stat = &stats[op as usize];
        let average_time = stat.sum as f64 / stat.count as f64;
        let op_count = stat.count;
        let op = match &op {
            Op::Insert => "insert",
            Op::Remove => "remove",
            Op::Lookup => "point-lookup",
            Op::RangeLookup => "range-lookup",
        };
        println!("{data_name},{op},{total_count},{op_count},{range_ratio},{locality},{modify_ratio},{value_length},{average_time}{}", build_values);
    }
}