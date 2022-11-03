use crate::{FatTruncatedKey, HeadTruncatedKey, PrefixTruncatedKey};
use smallvec::SmallVec;
use crate::inner_node::FenceData;

pub fn head(key: PrefixTruncatedKey) -> (u32, HeadTruncatedKey) {
    let mut k_padded = [0u8; 4];
    let head_len = key.0.len().min(4);
    k_padded[..head_len].copy_from_slice(&key.0[..head_len]);
    (
        u32::from_be_bytes(k_padded),
        HeadTruncatedKey(&key.0[head_len..]),
    )
}

pub fn short_slice<T>(s: &[T], offset: u16, len: u16) -> &[T] {
    &s[offset as usize..][..len as usize]
}

pub fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(a, b)| a == b).count()
}

pub fn trailing_bytes(b: &[u8], count: usize) -> &[u8] {
    &b[b.len() - count..]
}

pub fn partial_restore(
    old_prefix_len: usize,
    segments: &[&[u8]],
    new_prefix_len: usize,
) -> SmallBuff {
    debug_assert!(old_prefix_len <= new_prefix_len);
    let prefix_growth = new_prefix_len - old_prefix_len;
    let total_len = segments.iter().map(|s| s.len()).sum::<usize>() + old_prefix_len;
    let mut buffer = SmallBuff::with_capacity(total_len - new_prefix_len);
    let mut strip_amount = prefix_growth;
    for segment in segments {
        let strip_now = strip_amount.min(segment.len());
        strip_amount -= strip_now;
        buffer.extend_from_slice(&segment[strip_now..]);
    }
    debug_assert!(buffer.len() + new_prefix_len == total_len);
    buffer
}

pub type SmallBuff = SmallVec<[u8; 32]>;

pub fn merge_fences(
    left: FatTruncatedKey,
    separator: FatTruncatedKey,
    right: FatTruncatedKey,
    set_fences: impl FnOnce(FenceData),
) {
    debug_assert!(left.prefix_len >= separator.prefix_len);
    debug_assert!(right.prefix_len >= separator.prefix_len);
    if left.prefix_len == right.prefix_len {
        set_fences(FenceData {
            lower_fence: PrefixTruncatedKey(left.remainder),
            upper_fence: PrefixTruncatedKey(right.remainder),
            prefix_len: left.prefix_len,
        }
        );
    } else if left.prefix_len > right.prefix_len {
        let lower = partial_restore(
            separator.prefix_len,
            &[
                &separator.remainder[..left.prefix_len - separator.prefix_len],
                left.remainder,
            ],
            right.prefix_len,
        );
        set_fences(
            FenceData {
                lower_fence: PrefixTruncatedKey(&lower),
                upper_fence: PrefixTruncatedKey(right.remainder),
                prefix_len: right.prefix_len,
            }
        );
    } else {
        let upper = partial_restore(
            separator.prefix_len,
            &[
                &separator.remainder[..right.prefix_len - separator.prefix_len],
                right.remainder,
            ],
            left.prefix_len,
        );
        set_fences(
            FenceData {
                lower_fence: PrefixTruncatedKey(left.remainder),
                upper_fence: PrefixTruncatedKey(&upper),
                prefix_len: left.prefix_len,
            }
        );
    }
}

/// implementation of InnerNode::get_key
pub fn get_key_from_slice(src: PrefixTruncatedKey, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()> {
    let src = &src.0[strip_prefix..];
    if dst.len() < src.len() {
        return Err(());
    }
    let dst_len = dst.len();
    dst[dst_len - src.len()..].copy_from_slice(src);
    Ok(src.len())
}