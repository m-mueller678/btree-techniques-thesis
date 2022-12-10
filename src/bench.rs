use std::hint::black_box;
use std::io::BufRead;
use bumpalo::Bump;
use rand::{RngCore, SeedableRng};
use rand::distributions::{WeightedIndex};
use rand::distributions::Distribution;
use rand::prelude::SliceRandom;
use rand_distr::Zipf;
use rand_xoshiro::Xoshiro128PlusPlus;
use enum_iterator::Sequence;
use crate::{BTree, ensure_init, PAGE_SIZE};

fn build_info() -> (&'static str, &'static str) {
    let header = include_str!("../build-info.h");
    let parts: Vec<_> = header.split('"').collect();
    (parts[1], parts[3])
}

#[repr(usize)]
#[derive(Clone, Copy, Sequence, Debug)]
enum Op {
    Hit,
    Miss,
    Update,
    Insert,
    Remove,
    Range,
}

const OP_RATES: [usize; 6] = [
    40, 40,
    5, 5,
    5, 5,
];
const VALUE_LEN: usize = 8;
const RANGE_LEN: usize = 20;
const ZIPF_EXPONENT: f64 = 0.05;

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
fn init_bench<'a>(value_length: usize, mut data: Vec<Vec<u8>>, bump: &'a Bump, initial_size: usize) -> (BTree, Xoshiro128PlusPlus, Vec<u8>, Vec<&'a [u8]>) {
    let mut rng = Xoshiro128PlusPlus::seed_from_u64(123);
    assert!(minstant::is_tsc_available());
    let core_id = core_affinity::get_core_ids().unwrap().choose(&mut rng).cloned().unwrap();
    assert!(core_affinity::set_for_current(core_id));

    let mut value = vec![0u8; value_length];
    rng.fill_bytes(&mut value);
    let mut tree = BTree::new();
    data.shuffle(&mut rng);
    let mut shuffled = Vec::new();
    for k in data {
        shuffled.push(&*bump.alloc_slice_copy(&k));
    }
    for x in &shuffled[..initial_size] {
        tree.insert(x, &value);
    }
    (tree, rng, value, shuffled)
}

fn bench(op_count: usize,
         sample_op: WeightedIndex<usize>,
         initial_size: usize,
         value_length: usize,
         range_length: usize,
         zipf_exponent: f64,
         data: Vec<Vec<u8>>,
) -> [StatAggregator; Op::CARDINALITY] {
    let bump = Bump::with_capacity(data.iter().map(|s| s.len()).sum());
    let (mut tree, mut rng, payload, shuffled) = init_bench(value_length, data, &bump, initial_size);
    #[cfg(debug_assertions)]
        let mut std_set = std::collections::btree_set::BTreeSet::new();
    #[cfg(debug_assertions)]{
        for k in &shuffled[..initial_size] {
            std_set.insert(*k);
        }
    }
    let rng = &mut rng;
    let mut stats: [StatAggregator; Op::CARDINALITY] = Default::default();
    let mut range_lookup_key_out = [0u8; PAGE_SIZE];
    let mut inserted_start = 0usize;
    let mut inserted_count = initial_size;

    fn zipf_sample(n: usize, s: f64, rng: &mut Xoshiro128PlusPlus) -> usize {
        Zipf::new(n as u64, s).unwrap().sample(rng) as usize - 1
    }

    for _ in 0..op_count {
        let op = sample_op.sample(rng);
        match enum_iterator::all::<Op>().nth(op).unwrap() {
            Op::Hit => {
                let index = (inserted_start + inserted_count - 1 - zipf_sample(inserted_count, zipf_exponent, rng)) % shuffled.len();
                let mut out = 0;
                let found = unsafe {
                    stats[op].time_fn(||
                        black_box(tree.lookup(black_box(&mut out), black_box(shuffled[index])))
                    )
                };
                debug_assert!(!found.is_null());
            }
            Op::Miss => {
                let index = (inserted_start + inserted_count + zipf_sample(shuffled.len() - inserted_count, zipf_exponent, rng)) % shuffled.len();
                let mut out = 0;
                let found = unsafe {
                    stats[op].time_fn(||
                        black_box(tree.lookup(black_box(&mut out), black_box(shuffled[index])))
                    )
                };
                debug_assert!(found.is_null());
            }
            Op::Update => {
                let index = (inserted_start + inserted_count - 1 - zipf_sample(inserted_count, zipf_exponent, rng)) % shuffled.len();
                stats[op].time_fn(||
                    black_box(tree.insert(black_box(shuffled[index]), black_box(&payload)))
                );
            }
            Op::Insert => {
                let index = (inserted_start + inserted_count) % shuffled.len();
                assert!(inserted_count < shuffled.len());
                inserted_count += 1;
                stats[op].time_fn(||
                    black_box(tree.insert(black_box(shuffled[index]), black_box(&payload)))
                );
                #[cfg(debug_assertions)]{
                    std_set.insert(&shuffled[index]);
                }
            }
            Op::Remove => {
                let index = inserted_start;
                assert!(inserted_count > 0);
                inserted_count -= 1;
                inserted_start = (inserted_start + 1) % shuffled.len();
                let found = unsafe {
                    stats[op].time_fn(||
                        black_box(tree.remove(black_box(shuffled[index])))
                    )
                };
                #[cfg(debug_assertions)]{
                    std_set.remove(&shuffled[index]);
                }
                debug_assert!(found);
            }
            Op::Range => {
                let index = (inserted_start + inserted_count - 1 - zipf_sample(inserted_count, zipf_exponent, rng)) % shuffled.len();
                #[cfg(debug_assertions)]
                    let expected: Vec<&[u8]> = std_set.range(shuffled[index]..).take(range_length).cloned().collect();
                let mut count = 0;
                stats[op].time_fn(||
                    black_box(
                        tree.range_lookup(&shuffled[index], range_lookup_key_out.as_mut_ptr(), &mut |key_len, _value| {
                            #[cfg(debug_assertions)]{
                                assert!(expected[count] == &range_lookup_key_out[..key_len])
                            }
                            count += 1;
                            count < range_length
                        })
                    ));
                #[cfg(debug_assertions)]{
                    assert!(count == expected.len());
                }
            }
        }
    }
    stats
}

pub fn bench_main() {
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
    let (keys, data_name) = data.expect("no bench");
    let total_count = std::env::var("OP_COUNT").map(|x| x.parse().unwrap()).unwrap_or(1e6) as usize;
    assert!(OP_RATES.iter().sum::<usize>() == 100);
    let sample_op = WeightedIndex::new(OP_RATES).unwrap();
    let stats = bench(total_count, sample_op, keys.len() / 2, VALUE_LEN, RANGE_LEN, ZIPF_EXPONENT, keys);
    let (build_header, build_values) = build_info();
    println!("bench,op,total_count,op_count,average time{}", build_header);
    for op in enum_iterator::all::<Op>() {
        let stat = &stats[op as usize];
        let average_time = stat.sum as f64 / stat.count as f64;
        let op_count = stat.count;
        println!("{data_name},{op:?},{total_count},{op_count},{average_time}{}", build_values);
    }
}