use std::collections::btree_set::BTreeSet;
use std::mem::size_of;
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256PlusPlus;
use crate::inner_node::{FenceData, InnerConversionSink, InnerConversionSource, split_in_place};
use crate::{BTreeNode, ensure_init, PAGE_SIZE, PrefixTruncatedKey};
use crate::basic_node::BasicNode;
use crate::head_node::{U32HeadNode, U64HeadNode};
use crate::util::get_key_from_slice;

fn assert_node_eq(a: &dyn InnerConversionSource, b: &dyn InnerConversionSource) {
    let a_count = a.key_count();
    assert_eq!(a_count, b.key_count());
    assert_eq!(a.fences(), b.fences());
    let mut buffer = [0u8; PAGE_SIZE];
    for i in 0..a_count + 1 {
        assert_eq!(a.get_child(i), b.get_child(i));
    }
    for i in 0..a_count {
        let len_a = a.get_key(i, &mut buffer[..], 0).unwrap();
        let len_b = b.get_key(i, &mut buffer[..PAGE_SIZE - len_a], 0).unwrap();
        assert_eq!(buffer[PAGE_SIZE - len_a..], buffer[PAGE_SIZE - len_a - len_b..][..len_b]);
    }
}

fn random_source(rng: &mut impl Rng, count: usize, max_key_len: usize) -> impl InnerConversionSource {
    struct Src {
        keys: Vec<Vec<u8>>,
        prefix_len: usize,
    }

    impl InnerConversionSource for Src {
        fn fences(&self) -> FenceData {
            FenceData {
                prefix_len: self.prefix_len,
                lower_fence: PrefixTruncatedKey(self.keys.first().unwrap().as_slice()),
                upper_fence: PrefixTruncatedKey(self.keys.last().unwrap().as_slice()),
            }
        }

        fn key_count(&self) -> usize {
            self.keys.len() - 2
        }

        fn get_child(&self, index: usize) -> *mut BTreeNode {
            (index * size_of::<BTreeNode>()) as *mut BTreeNode
        }

        fn is_underfull(&self) -> bool {
            unimplemented!()
        }

        fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()> {
            assert!(index < self.keys.len() - 2);
            get_key_from_slice(PrefixTruncatedKey(self.keys[index + 1].as_slice()), dst, strip_prefix)
        }

        fn print(&self) {
            unimplemented!()
        }
    }

    let mut keys = BTreeSet::new();
    while keys.len() < count + 2 {
        let mut k = vec![0u8; rng.gen_range(0..=max_key_len)];
        rng.fill_bytes(&mut k[..]);
        keys.insert(k);
    }
    Src { keys: keys.into_iter().collect(), prefix_len: rng.gen_range(0..128) }
}

fn max_random_node_size<N: InnerConversionSink>(rng: impl Rng + Clone, max_key_len: usize) -> usize {
    let mut low = 0;
    let mut high = 32;
    let mut node = unsafe { BTreeNode::new_uninit() };
    while N::create(&mut node, &random_source(&mut rng.clone(), high, max_key_len)).is_ok() {
        low = high;
        high *= 2;
    }
    while low < high {
        let mid = (low + high + 1) / 2;
        if N::create(&mut node, &random_source(&mut rng.clone(), mid, max_key_len)).is_ok() {
            low = mid
        } else {
            high = mid - 1;
        }
    }
    high
}

#[test]
fn test_node_conversions() {
    fn test_conversion_traits<N: InnerConversionSource + InnerConversionSink>(rng: impl Rng + Clone, max_key_len: usize) {
        let max_count = max_random_node_size::<N>(rng.clone(), max_key_len);
        for count in [max_count / 4, max_count / 2, max_count] {
            let rng_src = random_source(&mut rng.clone(), count, max_key_len);
            let mut created = unsafe { BTreeNode::new_uninit() };
            N::create(&mut created, &rng_src).unwrap();
            assert_node_eq(&rng_src, created.to_inner_conversion_source())
        }
    }

    ensure_init();
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(0x1234567890abcdef);
    for _ in 0..10 {
        let rng = Xoshiro256PlusPlus::from_rng(&mut rng).unwrap();
        test_conversion_traits::<BasicNode>(rng.clone(), 4);
        test_conversion_traits::<BasicNode>(rng.clone(), 100);
        test_conversion_traits::<U32HeadNode>(rng.clone(), 3);
        test_conversion_traits::<U64HeadNode>(rng.clone(), 7);
    }
}
