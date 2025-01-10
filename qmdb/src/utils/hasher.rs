use sha2::{Digest, Sha256};

pub type Hash32 = [u8; 32];

pub const ZERO_HASH32: Hash32 = [0u8; 32];

pub fn hash<T: AsRef<[u8]>>(a: T) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(a);
    hasher.finalize().into()
}

pub fn hash1<T: AsRef<[u8]>>(level: u8, a: T) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update([level]);
    hasher.update(a);
    hasher.finalize().into()
}

pub fn hash2<T: AsRef<[u8]>>(children_level: u8, a: T, b: T) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update([children_level]);
    hasher.update(a);
    hasher.update(b);
    hasher.finalize().into()
}

pub fn hash2x<T: AsRef<[u8]>>(children_level: u8, a: T, b: T, exchange_ab: bool) -> Hash32 {
    if exchange_ab {
        hash2(children_level, b, a)
    } else {
        hash2(children_level, a, b)
    }
}

pub fn node_hash_inplace<T: AsRef<[u8]>>(
    children_level: u8,
    target: &mut [u8],
    src_a: T,
    src_b: T,
) {
    let mut hasher = Sha256::new();
    hasher.update([children_level]);
    hasher.update(src_a);
    hasher.update(src_b);
    target.copy_from_slice(&hasher.finalize());
}
