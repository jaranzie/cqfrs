use bitintr::{Pdep, Tzcnt};

pub fn bitrank(val: u64, pos: u64) -> u64 {
    if pos == 63 {
        (val & u64::MAX).count_ones() as u64
    } else {
        (val & ((2 << pos) - 1)).count_ones() as u64
    }
    // unsafe{u64::unchecked_sub(2 << pos, 1)};
    // (val & unsafe{u64::unchecked_sub(2 << pos, 1)}).count_ones() as usize
    // (val & (2 << pos) - 1).count_ones() as usize
}

pub fn popcntv(val: u64, ignore: u64) -> u64 {
    if ignore % 64 != 0 {
        (val & !(bitmask(ignore as u64 % 64))).count_ones() as u64
    } else {
        val.count_ones() as u64
    }
}

pub fn bitselect(val: u64, rank: u64) -> u64 {
    (1 << rank as u64).pdep(val).tzcnt()
}

pub fn bitselectv(val: u64, ignore: u64, rank: u64) -> u64 {
    bitselect(val & !(bitmask(ignore as u64 % 64)), rank)
}

pub fn bitmask(nbits: u64) -> u64 {
    if nbits == 64 {
        u64::MAX
    } else {
        (1 << nbits) - 1
    }
}
