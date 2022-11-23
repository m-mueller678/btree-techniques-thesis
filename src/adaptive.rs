use rand::{thread_rng};
use crate::BTreeNode;
use crate::head_node::{U32ExplicitHeadNode, U64ExplicitHeadNode};
use crate::inner_node::{InnerConversionSink};
use crate::vtables::BTreeNodeTag;
use rand::distributions::Distribution;

#[inline]
pub fn infrequent(infrequency: u32) -> bool {
    let distribution = rand::distributions::Bernoulli::from_ratio(1, infrequency).unwrap();
    distribution.sample(&mut thread_rng())
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
        if key_count < 20 {
            // small nodes have too few keys to make a good decision on
            return;
        }
        let max_len = dyn_node.get_key_length_max(0..key_count);
        let mut contains_known_trailing_zeros = false;
        let mut tmp = BTreeNode::new_uninit();
        let copy_back = 'try_nodes: {
            if max_len <= 3 && existing_head_len > 4 {
                U32ExplicitHeadNode::create(&mut tmp, dyn_node).unwrap();
                break 'try_nodes true;
            }
            if max_len <= 4 && existing_head_len > 4 {
                if U32ExplicitHeadNode::create(&mut tmp, dyn_node).is_ok() {
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
                if U64ExplicitHeadNode::create(&mut tmp, dyn_node).is_ok() {
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