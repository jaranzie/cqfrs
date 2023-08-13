use std::hash::{Hasher, BuildHasher};

/// Only supports types that are less that 64 bits wide
#[derive(Clone, Copy, Default)]
pub struct ReversibleHasher {
    hash: u64,
}

impl ReversibleHasher {
    fn new() -> Self {
        ReversibleHasher { hash: 0 }
    }

    fn invert_hash(hash: u64) {
            let mut tmp: u64;
            let mut key = hash;

            // Invert key = key + (key << 31)
            tmp = key.wrapping_sub(key << 31);
            key = key.wrapping_sub(tmp << 31);

            // Invert key = key ^ (key >> 28)
            tmp = key ^ key >> 28;
            key = key ^ tmp >> 28;

            // Invert key *= 21
            key = key.wrapping_mul(14933078535860113213);

            // Invert key = key ^ (key >> 14)
            tmp = key ^ key >> 14;
            tmp = key ^ tmp >> 14;
            tmp = key ^ tmp >> 14;
            key = key ^ tmp >> 14;

            // Invert key *= 265
            key = key.wrapping_mul(15244667743933553977);

            // Invert key = key ^ (key >> 24)
            tmp = key ^ key >> 24;
            key = key ^ tmp >> 24;

            // Invert key = (~key) + (key << 21)
            tmp = !key;
            tmp = !(key.wrapping_sub(tmp << 21));
            tmp = !(key.wrapping_sub(tmp << 21));
            key = !(key.wrapping_sub(tmp << 21));
    }
}

impl Hasher for ReversibleHasher {
    fn finish(&self) -> u64 {
        let mut key = self.hash;
        key = (!key).wrapping_add(key << 21); // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
        key = (key.wrapping_add(key << 3)).wrapping_add(key << 8); // key * 265
        key = key ^ (key >> 14);
        key = (key.wrapping_add(key << 2)).wrapping_add(key << 4); // key * 21
        key = key ^ (key >> 28);
        key = key.wrapping_add(key << 31);
        key
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.hash = unsafe {self.hash.unchecked_shl(8)};
            self.hash |= *byte as u64;
        }
    }
}

pub struct BuildReversableHasher;

impl BuildHasher for BuildReversableHasher {
    type Hasher = ReversibleHasher;

    fn build_hasher(&self) -> Self::Hasher {
        ReversibleHasher::new()
    }
}
