use crate::node_traits::{FenceData, FenceRef};
use crate::{FatTruncatedKey, HeadTruncatedKey, PrefixTruncatedKey};
use smallvec::SmallVec;
use crate::btree_node::STRIP_PREFIX;

pub fn head(key: &[u8]) -> (u32, HeadTruncatedKey) {
    let mut k_padded = [0u8; 4];
    let head_len = key.len().min(4);
    k_padded[..head_len].copy_from_slice(&key[..head_len]);
    (
        u32::from_be_bytes(k_padded),
        HeadTruncatedKey(&key[head_len..]),
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

/// helper for node split.
/// computes new fences and new separator for parent.
pub struct SplitFences<'a> {
    buffer: Option<SmallBuff>,
    src: FenceData<'a>,
    parent_prefix_len: usize,
    separator: PrefixTruncatedKey<'a>,
    prefix_src: &'a [u8],
}

impl<'a> SplitFences<'a> {
    #[inline(always)]
    pub fn new(src: FenceData<'a>, separator: PrefixTruncatedKey<'a>, parent_prefix_len: usize, prefix_src: &'a [u8]) -> Self {
        Self {
            buffer: None,
            src,
            parent_prefix_len,
            separator,
            prefix_src,
        }
    }

    #[inline(always)]
    pub fn separator(&mut self) -> PrefixTruncatedKey {
        if STRIP_PREFIX {
            PrefixTruncatedKey(self.init_buffer().as_slice())
        } else {
            let p = self.parent_prefix_len;
            PrefixTruncatedKey(&self.init_buffer()[p..])
        }
    }

    #[inline(always)]
    pub fn lower(&mut self) -> FenceData {
        let src = self.src;
        FenceData {
            upper_fence: self.fence_sep(),
            ..src
        }.restrip()
    }

    #[inline(always)]
    pub fn upper(&mut self) -> FenceData {
        let src = self.src;
        FenceData {
            lower_fence: self.fence_sep(),
            ..src
        }.restrip()
    }

    #[inline(always)]
    fn init_buffer(&mut self) -> &mut SmallBuff {
        self.buffer.get_or_insert_with(|| partial_restore(
            0,
            &[&self.prefix_src[..self.src.prefix_len], self.separator.0],
            if STRIP_PREFIX { self.parent_prefix_len } else { 0 },
        ))
    }

    #[inline(always)]
    fn fence_sep(&mut self) -> FenceRef {
        if STRIP_PREFIX {
            FenceRef(self.separator.0)
        } else {
            FenceRef(self.init_buffer().as_slice())
        }
    }
}

#[cfg(feature = "strip-prefix_true")]
pub struct MergeFences<'a> {
    buffer: once_cell::unsync::OnceCell<SmallBuff>,
    left_fences: FenceData<'a>,
    separator: FatTruncatedKey<'a>,
    right_fences: FenceData<'a>,
}

#[cfg(feature = "strip-prefix_false")]
pub struct MergeFences<'a> {
    fences: FenceData<'a>,
}


impl<'a> MergeFences<'a> {
    #[inline(always)]
    pub fn new(
        left_fences: FenceData<'a>,
        #[allow(unused_variables)]
        separator: FatTruncatedKey<'a>,
        right_fences: FenceData<'a>,
    ) -> Self {
        #[cfg(feature = "strip-prefix_true")]
        return {
            debug_assert!(left_fences.prefix_len >= separator.prefix_len);
            debug_assert!(right_fences.prefix_len >= separator.prefix_len);
            MergeFences {
                buffer: once_cell::unsync::OnceCell::new(),
                left_fences,
                separator,
                right_fences,
            }
        };
        #[cfg(feature = "strip-prefix_false")]
        return MergeFences {
            fences: FenceData {
                prefix_len: left_fences.prefix_len.min(right_fences.prefix_len),
                lower_fence: left_fences.lower_fence,
                upper_fence: right_fences.upper_fence,
            }
        };
    }

    #[inline(always)]
    pub fn fences(&self) -> FenceData {
        #[cfg(feature = "strip-prefix_true")]return {
            if self.left_fences.prefix_len == self.right_fences.prefix_len {
                FenceData {
                    lower_fence: self.left_fences.lower_fence,
                    upper_fence: self.right_fences.upper_fence,
                    prefix_len: self.left_fences.prefix_len,
                }
            } else if self.left_fences.prefix_len > self.right_fences.prefix_len {
                let lower = self.buffer.get_or_init(|| partial_restore(
                    self.separator.prefix_len,
                    &[
                        &self.separator.remainder[..self.left_fences.prefix_len - self.separator.prefix_len],
                        self.left_fences.lower_fence.0,
                    ],
                    self.right_fences.prefix_len,
                ));
                FenceData {
                    lower_fence: FenceRef(&lower),
                    ..self.right_fences
                }
            } else {
                let upper = self.buffer.get_or_init(|| partial_restore(
                    self.separator.prefix_len,
                    &[
                        &self.separator.remainder[..self.right_fences.prefix_len - self.separator.prefix_len],
                        self.right_fences.upper_fence.0,
                    ],
                    self.left_fences.prefix_len,
                ));
                FenceData {
                    upper_fence: FenceRef(&upper),
                    ..self.left_fences
                }
            }
        };
        #[cfg(feature = "strip-prefix_false")]
        return self.fences;
    }
}

/// implementation of InnerNode::get_key
pub fn get_key_from_slice(
    src: PrefixTruncatedKey,
    dst: &mut [u8],
    strip_prefix: usize,
) -> Result<usize, ()> {
    let src = &src.0[strip_prefix..];
    if dst.len() < src.len() {
        return Err(());
    }
    let dst_len = dst.len();
    dst[dst_len - src.len()..].copy_from_slice(src);
    Ok(src.len())
}

pub unsafe fn reinterpret<'a, A: 'a, B: 'a>(a: &'a A) -> &'a B {
    &*(a as *const A as usize as *const B)
}

pub unsafe fn reinterpret_mut<'a, A: 'a, B: 'a>(a: &'a mut A) -> &'a mut B {
    &mut *(a as *mut A as usize as *mut B)
}
