use std::collections::{BTreeSet, HashMap};
use std::hint::black_box;
use std::io::BufRead;
use std::process::Command;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use crate::{BTree, btree_print_info, ensure_init, PAGE_SIZE};

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


fn mem_info() -> serde_json::Value {
    let statm = procfs::process::Process::myself().unwrap().statm().unwrap();
    json!({
        "memory": statm.size,
        "statm":statm,
    })
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

#[derive(Default)]
struct StatAggregator {
    sum: u64,
    count: u64,
}

struct Perf {
    counters: Vec<(&'static str, Counter)>,
}

impl Perf {
    fn new() -> Self {
        let mut counters = Vec::new();
        counters.push(("task_clock", perf_event::Builder::new().kind(Software::TASK_CLOCK).build().unwrap()));
        counters.push(("cycles", perf_event::Builder::new().kind(Hardware::CPU_CYCLES).build().unwrap()));
        counters.push(("instructions", perf_event::Builder::new().kind(Hardware::INSTRUCTIONS).build().unwrap()));
        counters.push(("l1d_misses", perf_event::Builder::new().kind(Cache { which: WhichCache::L1D, operation: CacheOp::READ, result: CacheResult::MISS }).build().unwrap()));
        counters.push(("l1i_misses", perf_event::Builder::new().kind(Cache { which: WhichCache::L1I, operation: CacheOp::READ, result: CacheResult::MISS }).build().unwrap()));
        counters.push(("ll_misses", perf_event::Builder::new().kind(Hardware::CACHE_MISSES).build().unwrap()));
        counters.push(("branch_misses", perf_event::Builder::new().kind(Hardware::BRANCH_MISSES).build().unwrap()));
        Self { counters }
    }

    fn read_counter(c: &mut Counter) -> f64 {
        let x = c.read_count_and_time().unwrap();
        x.count as f64 * (x.time_enabled as f64 / x.time_running as f64)
    }

    fn to_json(&mut self) -> serde_json::Value {
        serde_json::Value::Object(self.counters.iter_mut().map(|(n, c)| (n.to_string(), Self::read_counter(c).into())).collect())
    }
}

struct Bench {
    stats: [StatAggregator; Op::CARDINALITY],
    sample_no_range: WeightedIndex<usize>,
    sample_range: WeightedIndex<usize>,
    instruction_buffer: Vec<u8>,
    initial_size: usize,
    value_length: usize,
    range_length: usize,
    zipf_exponent: f64,
    inserted_start: usize,
    inserted_count: usize,
    data: Vec<Vec<u8>>,
    payload: Vec<u8>,
    perf: Perf,
    rng: Xoshiro128PlusPlus,
    tree: BTree,
    #[cfg(debug_assertions)]
    std_set: BTreeSet<Vec<u8>>,
    time_controller: TimeController,
}

struct TimeController {
    time: u64,
    op: usize,
    history: Vec<HistoryEntry>,
}


struct HistoryEntry {
    op_time: f64,
    basic_conversions: f64,
    basic_conversion_attempts: f64,
    hash_conversions: f64,
}

pub static BASIC_CONVERSION_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
pub static BASIC_CONVERSIONS: AtomicUsize = AtomicUsize::new(0);
pub static HASH_CONVERSIONS: AtomicUsize = AtomicUsize::new(0);

impl TimeController {
    const EPOCH_LEN: usize = 125_000;
    const EPOCH_PER_SWITCH: usize = 2_000;
    const SWITCH_PERIOD_COUNT: usize = 5;
    fn new() -> Self {
        TimeController {
            time: 0,
            op: 0,
            history: Vec::with_capacity(Self::EPOCH_PER_SWITCH * Self::SWITCH_PERIOD_COUNT),
        }
    }

    fn time_fn<R>(&mut self, f: impl FnOnce() -> R) -> R {
        let t1 = minstant::Instant::now();
        let r = f();
        let t2 = minstant::Instant::now();
        self.push_op(t2.duration_since(t1).as_nanos() as u64);
        r
    }

    fn push_op(&mut self, time: u64) {
        assert!(!self.is_end());
        self.time += time;
        self.op += 1;
        if self.op == Self::EPOCH_LEN {
            self.history.push(HistoryEntry {
                op_time: self.time as f64 / self.op as f64,
                basic_conversions: BASIC_CONVERSIONS.swap(0, Ordering::Relaxed) as f64 / self.op as f64,
                basic_conversion_attempts: BASIC_CONVERSION_ATTEMPTS.swap(0, Ordering::Relaxed) as f64 / self.op as f64,
                hash_conversions: HASH_CONVERSIONS.swap(0, Ordering::Relaxed) as f64 / self.op as f64,
            });
            self.op = 0;
            self.time = 0;
        }
    }

    fn is_end(&self) -> bool {
        self.history.len() == Self::SWITCH_PERIOD_COUNT * Self::EPOCH_PER_SWITCH
    }

    fn is_range_period(&self, op_num: usize) -> bool {
        (op_num / Self::EPOCH_LEN / Self::EPOCH_PER_SWITCH) % 2 == 1
    }
}

impl Bench {
    fn init(
        sample_range: WeightedIndex<usize>,
        sample_no_range: WeightedIndex<usize>,
        initial_size: usize,
        value_length: usize,
        range_length: usize,
        zipf_exponent: f64,
        mut data: Vec<Vec<u8>>,
    ) -> Self {
        let mut rng = Xoshiro128PlusPlus::seed_from_u64(123);
        assert!(minstant::is_tsc_available());

        let mut value = vec![0u8; value_length];
        rng.fill_bytes(&mut value);
        let mut tree = BTree::new();
        data.shuffle(&mut rng);
        for x in &data[..initial_size] {
            tree.insert(x, &value);
        }
        unsafe { btree_print_info(&mut tree) };
        Bench {
            stats: Default::default(),
            sample_range,
            sample_no_range,
            instruction_buffer: Vec::new(),
            initial_size,
            value_length,
            range_length,
            zipf_exponent,
            inserted_start: 0,
            inserted_count: initial_size,
            #[cfg(debug_assertions)]
            std_set: {
                let mut s = BTreeSet::new();
                for x in &data[..initial_size] {
                    s.insert(x.clone());
                }
                s
            },
            data,
            payload: value,
            perf: Perf::new(),
            rng,
            tree,
            time_controller: TimeController::new(),
        }
    }

    fn zipf_sample(&mut self, n: usize) -> usize {
        assert!(n > 0);
        Zipf::new(n as u64, self.zipf_exponent).unwrap().sample(&mut self.rng) as usize - 1
    }

    fn op_from_usize(n: usize) -> Op {
        let op_enum = enum_iterator::all::<Op>().nth(n).unwrap();
        assert!(op_enum as usize == n);
        op_enum
    }

    fn run_buffered(&mut self) {
        let mut i = 0;
        for c in &mut self.perf.counters {
            c.1.enable().unwrap();
        }
        let mut range_lookup_key_out = [0u8; PAGE_SIZE];
        while i < self.instruction_buffer.len() {
            let op = Self::op_from_usize(self.instruction_buffer[i] as usize);
            let len_bytes: &[u8; 2] = self.instruction_buffer[i + 1..][..2].try_into().unwrap();
            let len = u16::from_ne_bytes(*len_bytes) as usize;
            let key = &self.instruction_buffer[i + 3..][..len];
            i += len + 3;
            match op {
                Op::Hit => {
                    let mut out = 0;
                    let found = unsafe {
                        self.time_controller.time_fn(||
                            black_box(self.tree.lookup(black_box(&mut out), black_box(key)))
                        )
                    };
                    debug_assert!(!found.is_null());
                }
                Op::Miss => {
                    let mut out = 0;
                    let found = unsafe {
                        self.time_controller.time_fn(||
                            black_box(self.tree.lookup(black_box(&mut out), black_box(key)))
                        )
                    };
                    debug_assert!(found.is_null());
                }
                Op::Update => {
                    self.time_controller.time_fn(||
                        black_box(self.tree.insert(black_box(key), black_box(&self.payload)))
                    );
                }
                Op::Insert => {
                    self.time_controller.time_fn(||
                        black_box(self.tree.insert(black_box(key), black_box(&self.payload)))
                    );
                    #[cfg(debug_assertions)]{
                        self.std_set.insert(key.to_owned());
                    }
                }
                Op::Remove => {
                    let found = unsafe {
                        self.time_controller.time_fn(||
                            black_box(self.tree.remove(black_box(key)))
                        )
                    };
                    #[cfg(debug_assertions)]{
                        self.std_set.remove(key);
                    }
                    debug_assert!(found);
                }
                Op::Range => {
                    #[cfg(debug_assertions)]
                        let expected: Vec<&Vec<u8>> = self.std_set.range(key.to_owned()..).take(self.range_length).collect();
                    let mut count = 0;
                    self.time_controller.time_fn(||
                        black_box(
                            self.tree.range_lookup(&key, range_lookup_key_out.as_mut_ptr(), &mut |key_len, _value| {
                                #[cfg(debug_assertions)]{
                                    assert!(expected[count] == &range_lookup_key_out[..key_len])
                                }
                                count += 1;
                                count < self.range_length
                            })
                        ));
                    #[cfg(debug_assertions)]{
                        assert!(count == expected.len());
                    }
                }
            }
            if self.time_controller.is_end() {
                return;
            }
        }
        for c in &mut self.perf.counters {
            c.1.disable().unwrap();
        }
        debug_assert!(i == self.instruction_buffer.len());
        self.instruction_buffer.clear();
    }

    fn run(mut self) -> TimeController {
        for op_num in 0.. {
            let op = if self.time_controller.is_range_period(op_num) {
                self.sample_range.sample(&mut self.rng)
            } else {
                self.sample_no_range.sample(&mut self.rng)
            };
            let index = match Self::op_from_usize(op) {
                Op::Hit | Op::Update | Op::Range => (self.inserted_start + self.inserted_count - 1 - self.zipf_sample(self.inserted_count)) % self.data.len(),
                Op::Miss => (self.inserted_start + self.inserted_count + self.zipf_sample(self.data.len() - self.inserted_count)) % self.data.len(),
                Op::Insert => {
                    let index = (self.inserted_start + self.inserted_count) % self.data.len();
                    self.inserted_count += 1;
                    index
                }
                Op::Remove => {
                    let index = self.inserted_start;
                    self.inserted_count -= 1;
                    self.inserted_start = (self.inserted_start + 1) % self.data.len();
                    index
                }
            };
            self.instruction_buffer.push(op as u8);
            self.instruction_buffer.extend_from_slice(&(self.data[index].len() as u16).to_ne_bytes());
            self.instruction_buffer.extend_from_slice(&self.data[index]);
            const INSTRUCTION_BUFFER_SIZE: usize = if cfg!(debug_assertions) { 1 } else { 100_000 };
            if self.instruction_buffer.len() >= INSTRUCTION_BUFFER_SIZE {
                self.run_buffered();
                if self.time_controller.is_end() {
                    break;
                }
            }
        }
        unsafe { btree_print_info(&mut self.tree) };
        std::mem::forget(self.tree);
        self.time_controller
    }
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

    let value_len: usize = std::env::var("VALUE_LEN").as_deref().unwrap_or("8").parse().unwrap();
    let range_len: usize = std::env::var("RANGE_LEN").as_deref().unwrap_or("10").parse().unwrap();
    let zipf_exponent: f64 = std::env::var("ZIPF_EXPONENT").as_deref().unwrap_or("0.15").parse().unwrap();
    let sample_no_range = WeightedIndex::new([40, 40, 5, 5, 5, 5]).unwrap();
    let sample_range = WeightedIndex::new([40, 40, 5, 5, 5, 1805]).unwrap();

    let initial_size = if std::env::var("START_EMPTY").as_deref().unwrap_or("0") == "1" { 0 } else { keys.len() / 2 };

    let time_controller = Bench::init(sample_range, sample_no_range, initial_size, value_len, range_len, zipf_exponent, keys).run();
    let mem_info = mem_info();
    let build_info = build_info().into();
    let common_info = json!({
        "data":data_name,
        "value_len":value_len,
        "range_len":range_len,
        "zipf_exponent":zipf_exponent,
        "host": host_name(),
        "run_start":  std::time::SystemTime::now()
    });
    for (t, d) in time_controller.history.iter().enumerate() {
        let hist_info = json!({
            "t":t,
            "op_time": d.op_time,
            "basic_conversions": d.basic_conversions,
            "basic_conversion_attempts": d.basic_conversion_attempts,
            "hash_conversions": d.hash_conversions,
        });
        print_joint_objects(&[&build_info, &common_info, &hist_info]);
    }
    print_joint_objects(&[&build_info, &common_info, &mem_info]);
}

pub fn print_tpcc_result(time: f64, tx_count: u64, warehouses: u64) {
    let mem_info = mem_info();
    let tpcc = json!({
        "host": host_name(),
        "run_start":  std::time::SystemTime::now(),
        "warehouse_count":warehouses,
        "tx_count": tx_count,
        "time": time,
    });
    print_joint_objects(&[&build_info().into(), &tpcc, &mem_info]);
}

fn print_joint_objects(objects: &[&serde_json::Value]) {
    // this is just a convenient place to set the flag, as all benchmarks call this at the end.
    crate::MEASUREMENT_COMPLETE.store(true, Ordering::Relaxed);
    let joint: serde_json::Map<_, _> = objects.iter().flat_map(|o| o.as_object().unwrap().iter()).map(|(s, v)| (s.clone(), v.clone())).collect();
    println!("{}", serde_json::to_string(&joint).unwrap());
}