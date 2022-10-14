pub fn head(key: &[u8]) -> u32 {
    let mut k_padded = [0u8; 4];
    k_padded[..key.len().min(4)].copy_from_slice(key);
    u32::from_be_bytes(k_padded)
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
