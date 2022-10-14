pub fn head(key:&[u8])->u32{
    let mut k_padded=[0u8;4];
    k_padded[..key.len().min(4)].copy_from_slice(key);
    u32::from_be_bytes(k_padded)
}

pub fn short_slice<T>(s:&[T],offset:u16,len:u16)->&[T]{
    &s[offset as usize..][..len as usize]
}