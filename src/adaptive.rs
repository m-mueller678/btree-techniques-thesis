use once_cell::sync::Lazy;
use rand::{Rng, SeedableRng};
use crate::BTreeNode;
use crate::head_node::{U32ExplicitHeadNode, U32ZeroPaddedHeadNode, U64ExplicitHeadNode, U64ZeroPaddedHeadNode};
use crate::node_traits::{InnerConversionSink};
use crate::vtables::BTreeNodeTag;
use rand::distributions::Distribution;
use rand::rngs::SmallRng;

pub static mut RAND: Lazy<SmallRng> = Lazy::new(|| SmallRng::seed_from_u64(0x0123456789abcdef));

#[inline]
pub fn infrequent(infrequency: u32) -> bool {
    let distribution = rand::distributions::Bernoulli::from_ratio(1, infrequency).unwrap();
    distribution.sample(unsafe { &mut *RAND })
}

pub fn gen_random() -> u32 {
    unsafe { &mut *RAND }.gen()
}

pub fn adapt_inner(node: &mut BTreeNode) {
    unsafe {
        let tag = node.tag();
        let existing_head_len = match tag {
            BTreeNodeTag::U32ZeroPaddedHead => 4,
            BTreeNodeTag::U32ExplicitHead => 4,
            BTreeNodeTag::U64ZeroPaddedHead => 8,
            BTreeNodeTag::U64ExplicitHead => 8,
            BTreeNodeTag::AsciiHead => 8,
            _ => usize::MAX,
        };
        let dyn_node = node.to_inner();
        let key_count = dyn_node.key_count();
        let max_len = dyn_node.get_key_length_max(0..key_count);
        let mut contains_known_trailing_zeros = false;
        let mut tmp = BTreeNode::new_uninit();
        let copy_back = 'try_nodes: {
            if max_len <= 3 && existing_head_len > 4 {
                U32ExplicitHeadNode::create(&mut tmp, dyn_node).unwrap();
                break 'try_nodes true;
            }
            if max_len <= 4 && existing_head_len > 4 {
                if U32ZeroPaddedHeadNode::create(&mut tmp, dyn_node).is_ok() {
                    break 'try_nodes true;
                } else {
                    contains_known_trailing_zeros = true;
                }
            }
            if max_len <= 7 && existing_head_len > 8 {
                U64ExplicitHeadNode::create(&mut tmp, dyn_node).unwrap();
                break 'try_nodes true;
            }
            if max_len <= 8 && existing_head_len > 8 && !contains_known_trailing_zeros {
                if U64ZeroPaddedHeadNode::create(&mut tmp, dyn_node).is_ok() {
                    break 'try_nodes true;
                }
            }
            false
        };
        if copy_back {
            *node = tmp;
        }
    }
}