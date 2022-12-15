use std::hint::black_box;
use std::io::BufRead;
use std::process::Command;
use bumpalo::Bump;
use rand::{RngCore, SeedableRng};
use rand::distributions::{WeightedIndex};
use rand::distributions::Distribution;
use rand::prelude::SliceRandom;
use rand_distr::Zipf;
use rand_xoshiro::Xoshiro128PlusPlus;
use enum_iterator::Sequence;
use perf_event::{Counter, Group};
use perf_event::events::{Cache, CacheOp, CacheResult, Hardware, Software, WhichCache};
use serde_json::json;
use crate::{BTree, ensure_init, PAGE_SIZE};

fn build_info() -> serde_json::Map<String, serde_json::Value> {
    let header = include_str!("../build-info.h");
    let parts: Vec<_> = header.split('"').collect();
    parts[1].split(",")
        .map(|s| s.to_owned())
        .zip(
            parts[3].split(",")
                .map(|s| serde_json::Value::String(s.to_owned()))
        )
        .filter(|x| !x.0.is_empty()).collect()
}

fn host_name() -> String {
    let out = Command::new("hostname").output().unwrap().stdout;
    String::from_utf8_lossy(&out).to_string()
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

struct Perf {
    counter_group: Group,
    task_clock: Counter,
    cycles: Counter,
    instructions: Counter,
    l1d_misses: Counter,
    l1i_misses: Counter,
    ll_misses: Counter,
    branch_misses: Counter,
}

impl Perf {
    fn new() -> Self {
        let mut group = Group::new().unwrap();
        Perf {
            task_clock: perf_event::Builder::new().group(&mut group).kind(Software::TASK_CLOCK).build().unwrap(),
            cycles: perf_event::Builder::new().group(&mut group).kind(Hardware::CPU_CYCLES).build().unwrap(),
            instructions: perf_event::Builder::new().group(&mut group).kind(Hardware::INSTRUCTIONS).build().unwrap(),
            l1d_misses: perf_event::Builder::new().group(&mut group).kind(Cache { which: WhichCache::L1D, operation: CacheOp::READ, result: CacheResult::MISS }).build().unwrap(),
            l1i_misses: perf_event::Builder::new().group(&mut group).kind(Cache { which: WhichCache::L1I, operation: CacheOp::READ, result: CacheResult::MISS }).build().unwrap(),
            ll_misses: perf_event::Builder::new().group(&mut group).kind(Hardware::CACHE_MISSES).build().unwrap(),
            branch_misses: perf_event::Builder::new().group(&mut group).kind(Hardware::BRANCH_MISSES).build().unwrap(),
            counter_group: group,
        }
    }

    fn to_json(&mut self) -> serde_json::Value {
        json!({
            "task_clock":self.task_clock.read().unwrap(),
            "cycles":self.cycles.read().unwrap(),
            "instructions":self.instructions.read().unwrap(),
            "l1d_misses":self.l1d_misses.read().unwrap(),
            "l1i_misses":self.l1i_misses.read().unwrap(),
            "ll_misses":self.ll_misses.read().unwrap(),
            "branch_misses":self.branch_misses.read().unwrap(),
        })
    }
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
) -> ([StatAggregator; Op::CARDINALITY], Perf) {
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
    let mut perf = Perf::new();

    fn zipf_sample(n: usize, s: f64, rng: &mut Xoshiro128PlusPlus) -> usize {
        Zipf::new(n as u64, s).unwrap().sample(rng) as usize - 1
    }

    perf.counter_group.enable().unwrap();
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
    perf.counter_group.disable().unwrap();
    (stats, perf)
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
    let (stats, mut perf) = bench(total_count, sample_op, keys.len() / 2, VALUE_LEN, RANGE_LEN, ZIPF_EXPONENT, keys);
    let build_info = build_info().into();
    let common_info = json!({
        "data":data_name,
        "total_count":total_count,
        "value_len":VALUE_LEN,
        "range_len":RANGE_LEN,
        "zipf_exponent":ZIPF_EXPONENT,
        "op_rates":OP_RATES,
        "host": host_name()
    });
    for op in enum_iterator::all::<Op>() {
        let stat = &stats[op as usize];
        let op_count = stat.count;
        let average_time = stat.sum as f64 / stat.count as f64;
        let op_info = json!({
            "op": format!("{op:?}"),
            "op_count": op_count,
            "time": average_time,
        });
        print_joint_objects(&[&build_info, &common_info, &op_info]);
    }
    let perf_info = perf.to_json();
    print_joint_objects(&[&build_info, &common_info, &perf_info]);
}

fn print_joint_objects(objects: &[&serde_json::Value]) {
    let joint: serde_json::Map<_, _> = objects.iter().flat_map(|o| o.as_object().unwrap().iter()).map(|(s, v)| (s.clone(), v.clone())).collect();
    println!("{}", serde_json::to_string(&joint).unwrap());
}