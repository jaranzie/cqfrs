#![feature(sync_unsafe_cell, ptr_internals, unchecked_shifts)]
#![feature(unchecked_math)]
// #![feature(ptr_internals)]
#![feature(core_intrinsics)]
mod blocks;
mod cqf;
mod reversible_hasher;
// mod utils;
const SLOTS_PER_BLOCK: usize = 64;
// mod old_cqf;
// pub use old_cqf::CountingQuotientFilter as OldCqf;

pub use cqf::*;
pub use reversible_hasher::*;

mod utils {
    use super::SLOTS_PER_BLOCK;
    // use std::intrinsics::{unchecked_shl, unchecked_sub};
    use bitintr::{Pdep, Tzcnt};

    /// Gets the number of setbits upto and including position (0 indexed)
    pub fn bitrank(value: u64, position: u64) -> u64 {
        debug_assert!(position <= 64);
        let mask = bitmask(position + 1);
        (value & mask).count_ones() as u64
    }

    pub fn count_ones_ignore(val: u64, ignore: u64) -> u64 {
        debug_assert!(ignore <= 64);
        (val & !(bitmask(ignore as u64 % 64))).count_ones() as u64
    }

    /// Select the ith bit in val, 0 indexed
    pub fn index_of_ith_bit(val: u64, i: u64) -> u64 {
        (1 << i as u64).pdep(val).tzcnt()
    }

    /// ignores the first `ignore` bits in `val` and returns the index of the ith bit, 0 indexed
    pub fn index_of_ith_bit_ignore(val: u64, i: u64, ignore: u64) -> u64 {
        debug_assert!(ignore <= 64);
        index_of_ith_bit(val & !(bitmask(ignore)), i)
    }

    pub fn bitmask(num_bits: u64) -> u64 {
        debug_assert!(num_bits <= 64);
        // unsafe { unchecked_sub(unchecked_shl(1, num_bits), 1) }
        if num_bits >= 64 {
            u64::MAX
        } else {
            (1 << num_bits) - 1
        }
    }

    pub fn split_quotient(quotient: u64) -> (usize, u64) {
        let block_index: usize = (quotient / SLOTS_PER_BLOCK as u64) as usize;
        let slot_index = quotient % SLOTS_PER_BLOCK as u64;
        (block_index, slot_index)
    }
    
    pub fn block_index_of(quotient: u64) -> usize {
        (quotient / SLOTS_PER_BLOCK as u64) as usize
    }
    
    pub fn slot_index_of(quotient: u64) -> u64 {
        quotient % SLOTS_PER_BLOCK as u64
    }
    
    pub fn is_bit_set(value: u64, position: u64) -> bool {
        value & (1 << position) != 0
    }
    
    pub fn set_bit(value: &mut u64, position: u64) {
        *value |= 1 << position;
    }
    
    pub fn unset_bit(value: &mut u64, position: u64) {
        *value &= !(1 << position);
    }
    
    pub fn set_bit_to(value: &mut u64, position: u64, bit: bool) {
        if bit {
            set_bit(value, position);
        } else {
            unset_bit(value, position);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        fn test_bitmask() {
            for i in 0..64 {
                assert_eq!(bitmask(i), (1 << i) - 1);
            }
            assert_eq!(bitmask(64), u64::MAX);
        }

        fn test_bitrank() {
            assert_eq!(bitrank(0b1010, 0), 0);
            assert_eq!(bitrank(0b1010, 1), 1);
            assert_eq!(bitrank(0b1010, 2), 1);
            assert_eq!(bitrank(0b1010, 3), 2);
            assert_eq!(bitrank(0b1010, 4), 2);
        }
    }
}

// cqf!(Cqf, u32, u64, u64);

// #[macro_export]
// macro_rules! cqf {
//     ($name:ident, $type:ty, $remainder:ty, $offset:ty) => {

//         fn test() {
//             let x  = 12000123u64;
//             let x_cast: $type = x.into();
//         }

//         // concatentate to make new identfier

//         pub struct $name {
//             ptr: Unique<Block>,
//             len: usize,
//         }

//         pub struct $name {
//             // blocks: blocks::Blocks<$remainder>,
//             remainder_mask: $remainder,
//             offset_mask: $offset,
//             offset_shift: u64,
//             remainder_shift: u64,
//             count_shift: u64,
//             count_mask: $remainder,
//             count_bits: u64,
//             count_max: u64,
//             count_max_mask: $remainder,
//             count_max_shift: u64,
//         }
//     };
//     () => {

//     };
// }
