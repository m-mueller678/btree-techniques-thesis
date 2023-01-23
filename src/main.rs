#![feature(is_sorted)]


use std::fmt::format;
use btree::{bench, PrefixTruncatedKey};


use std::hint::black_box;
use std::ptr;

use std::sync::atomic::{AtomicUsize, Ordering};

use std::time::{Duration, Instant};
use rand::{Rng, RngCore, SeedableRng};
use rand_xoshiro::Xoshiro128PlusPlus;
use smallvec::SmallVec;
use btree::head_node::{AsciiHead, ExplicitLengthHead, FullKeyHead, FullKeyHeadNoTag, ZeroPaddedHead};
use btree::node_traits::node_print;


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
    if std::env::var("no-exist-Fn9JyhBOj/9XQvKIXDIOdc5Iu+Y=").is_ok() {
        // force linker to keep this function, it is useful for debugging
        unsafe { node_print(ptr::null()) };
    }

    fn to_ascii(s:PrefixTruncatedKey)->(Option<u64>,u64){
        (
            AsciiHead::make_fence_head(s).map(|s|s.0),
            AsciiHead::make_needle_head(s).0,
        )
    }
    fn to_padded(s:PrefixTruncatedKey)->(Option<u64>,u64){
        (
            ZeroPaddedHead::<u64>::make_fence_head(s).map(|s|s.0),
            ZeroPaddedHead::<u64>::make_needle_head(s).0,
        )
    }fn to_explicit_length(s:PrefixTruncatedKey)->(Option<u64>,u64){
        (
            ExplicitLengthHead::<u64>::make_fence_head(s).map(|s|s.0),
            ExplicitLengthHead::<u64>::make_needle_head(s).0,
        )
    }

    let strings=[
        b"a".as_slice(),
        b"\x01\x00".as_slice(),
        b"aaaaaaaa".as_slice(),
        b"a".as_slice(),
        b"aaaaaaaa\x00".as_slice(),
        b"aaaaaaaaaa".as_slice(),
        b"a\x7fa".as_slice(),
        b"\xff\xff\xff\xff\xff\xff\xff\xff".as_slice()
    ];
    for string in strings{
        let display:String = string.iter().map(|c|{
            if c.is_ascii_graphic(){
                format!("{}",*c as char)
            }else{
                format!("\\x{:02x}",c)
            }
        }).collect();
        print!("\"{}\"",display);
        let f=to_ascii;
        let (stored,lookup) = f(PrefixTruncatedKey(string));
        let stored = stored.map(|s|format!("{:08b}",s)).unwrap_or("invalid".into());
        print!("& {} & {:016b}",stored,lookup);
        println!();
    }

    for string in [
        [1,1].as_slice(),
        [1,1,1,1].as_slice(),
    ]{
        print!("{}",format!("{:?}",string).replace("[","\\{").replace("]","\\}"));
        let stored = ExplicitLengthHead::<u32>::make_fence_head(PrefixTruncatedKey(string)).map(|h|h.0);
        let lookup = ExplicitLengthHead::<u32>::make_needle_head(PrefixTruncatedKey(string)).0;
        let stored = stored.map(|s|format!("{:08x}",s)).unwrap_or("invalid".into());
        print!("& \\texttt{{{}}} & \\texttt{{{:08x}}}",stored,lookup);
        println!("\\\\");
    }
    dbg!();

    for string in [
        [1,1].as_slice(),
        [1,1,1,1].as_slice(),
        [1,1,1,1,0].as_slice(),
        [1,0].as_slice(),
        [255,255,255,255].as_slice(),
    ]{
        print!("{}",format!("{:?}",string).replace("[","\\{").replace("]","\\}"));
        let stored = ZeroPaddedHead::<u32>::make_fence_head(PrefixTruncatedKey(string)).map(|h|h.0);
        let lookup = ZeroPaddedHead::<u32>::make_needle_head(PrefixTruncatedKey(string)).0;
        let stored = stored.map(|s|format!("{:08x}",s)).unwrap_or("invalid".into());
        print!("& \\texttt{{{}}} & \\texttt{{{:08x}}}",stored,lookup);
        println!("\\\\");
    }
    dbg!();
}
