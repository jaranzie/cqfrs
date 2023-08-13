use std::cell::SyncUnsafeCell;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::ops::IndexMut;
use std::ptr::Unique;

use super::BlockT;
use super::QF_SLOTS_PER_BLOCK;
// use crate::{bitmask, bitrank, bitselectv, popcntv};
use crate::utils::*;
use bitintr::{Pdep, Popcnt, Tzcnt};

type Slot = u64;


// Can maybe switch this to SofA layout, either everything is an array or, one array of offsets
// one array of a struct of occupeid runnetnds and counts, and then remainders

/// Remmainder is the type of the remainder, either u64, u32, u16, or u8
#[repr(C)]
pub struct Block<Remainder: TryFrom<u64>> {
    occupieds: u64,
    runends: u64,
    counts: u64,
    remainders: [Remainder; QF_SLOTS_PER_BLOCK],
    offset: u16,
}

impl<Remainder: Sized + TryFrom<u64>> Block<Remainder> {
    #[inline]
    pub fn slot(&self, slot: usize) -> &Remainder {
        &self.remainders[slot]
    }

    #[inline]
    pub fn slot_mut(&mut self, slot: usize) -> &mut Remainder {
        &mut self.remainders[slot]
    }

    #[inline]
    pub fn flip_occupied(&mut self, slot: usize) {
        self.occupieds ^= 1 << slot;
    }

    #[inline]
    pub fn is_occupied(&self, slot: usize) -> bool {
        ((self.occupieds >> slot) & 1) != 0
    }

    pub fn set_occupied(&mut self, slot: usize, bit: bool) {
        if bit {
            self.occupieds |= 1 << slot;
        } else {
            self.occupieds &= !(1 << slot);
        }
    }

    #[inline]
    pub fn flip_runend(&mut self, slot: usize) {
        self.runends ^= 1 << slot;
    }

    #[inline]
    pub fn is_runend(&self, slot: usize) -> bool {
        ((self.runends >> slot) & 1) != 0
    }

    pub fn set_runend(&mut self, slot: usize, bit: bool) {
        if bit {
            self.runends |= 1 << slot;
        } else {
            self.runends &= !(1 << slot);
        }
    }

    #[inline]
    pub fn flip_count(&mut self, slot: usize) {
        self.counts ^= 1 << slot;
    }

    #[inline]
    pub fn is_count(&self, slot: usize) -> bool {
        ((self.counts >> slot) & 1) != 0
    }

    pub fn set_count(&mut self, slot: usize, bit: bool) {
        if bit {
            self.counts |= 1 << slot;
        } else {
            self.counts &= !(1 << slot);
        }
    }

    pub fn is_empty(&self, slot: usize) -> bool {
        !self.is_occupied(slot) && !self.is_runend(slot) && !self.is_count(slot)
    }

    pub fn offset_lower_bound(&self, slot: u64) -> u64 {
        let occupieds = self.occupieds & bitmask(slot + 1);
        let offset_64: u64 = self.offset.into();
        if offset_64 <= slot {
            let runends = (self.runends & bitmask(slot)) >> offset_64;
            return (occupieds.count_ones() - runends.count_ones()) as u64;
        }
        return offset_64 - slot + occupieds.count_ones() as u64;
    }

    pub fn clear(&mut self) {
        self.offset = 0;
        self.occupieds = 0;
        self.runends = 0;
        self.counts = 0;
        for i in 0..QF_SLOTS_PER_BLOCK {
            self.remainders[i] = match Remainder::try_from(0) {
                Ok(remainder) => remainder,
                Err(_) => panic!("Remainder type must be able to be created from 0"),
            }; // maybe try_from.unwrap() with bitmask before
        }
    }
}

pub struct Blocks<Remainder: Copy + TryFrom<u64>>{
    ptr: Unique<Block<Remainder>>,
    len: usize,
    // inner: &[Block<Remainder>]
}

impl<Remainder: Copy + TryFrom<u64>> Deref for Blocks<Remainder> {
    type Target = [Block<Remainder>];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl<Remainder: Copy + TryFrom<u64>> DerefMut for Blocks<Remainder> {
    fn deref_mut(&mut self) -> &mut [Block<Remainder>] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

// impl<Remainder: Copy + TryFrom<u64>> Index<usize> for Blocks<Remainder> {
//     type Output = Block<Remainder>;

//     fn index(&self, index: usize) -> &Self::Output {
//         &self[index]
//     }
// }

// impl<Remainder: Copy + TryFrom<u64>> IndexMut<usize> for Blocks<Remainder> {
//     fn index_mut (&mut self, index: usize) -> &mut Block<Remainder> {
//         &mut self[index]
//     }
// }

impl<Remainder: Copy + TryFrom<u64>> Blocks<Remainder> {
    pub fn new(ptr: Unique<Block<Remainder>>, len: usize) -> Self {
        Self {
            ptr,
            len,
        }
    }

    pub fn is_empty(&self, block_index: usize, slot_index: usize) -> bool {
        let block = &self[block_index];
        !block.is_occupied(slot_index)
            && !block.is_runend(slot_index)
            && !block.is_count(slot_index)
    }

    pub fn is_occupied(&self, block_index: usize, slot_index: usize) -> bool {
        self[block_index].is_occupied(slot_index)
    }

    pub fn is_runend(&self, block_index: usize, slot_index: usize) -> bool {
        self[block_index].is_runend(slot_index)
    }

    pub fn is_count(&self, block_index: usize, slot_index: usize) -> bool {
        self[block_index].is_count(slot_index)
    }

    pub fn set_occupied(&mut self, block_index: usize, slot_index: usize, bit: bool) {
        self[block_index].set_occupied(slot_index, bit)
    }

    pub fn set_runend(&mut self, block_index: usize, slot_index: usize, bit: bool) {
        self[block_index].set_runend(slot_index, bit)
    }

    pub fn set_count(&mut self, block_index: usize, slot_index: usize, bit: bool) {
        self[block_index].set_count(slot_index, bit)
    }

    // pub fn set_remainder(&self, block_index: usize, slot_index: usize, remainder: u64) {
    //     let block = self.get_block_mut(block_index);
    //     unsafe { (*block).set_slot(slot_index, remainder) }
    // }

    // pub fn get_remainder(&self, block_index: usize, slot_index: usize) -> u64 {
    //     let block = self.get_block(block_index);
    //     unsafe { (*block).get_slot(slot_index) }
    // }

    // pub fn set_remainder_block(block: &mut Block, slot_index: usize, remainder: u64) {
    //     unsafe { (*block).set_slot(slot_index, remainder) }
    // }

    // pub fn get_run_end_index(&self, block_index: usize, slot_index:usize) -> usize {
    //     let block = self.get_block(block_index);
    //     let offset = block.offset;
    //     // Number of occupied slots before current slot
    //     let current_block_rank = bitrank(block.occupieds, slot_index);
    //     // if rank == 0 {
    //     //     return 0;
    //     // }
    //     0
    // }

    // I don't think this is correct if runs can cross blocks
    // pub fn run_end(&self, quotient: usize) -> usize {
    //     let block_idx: usize = quotient / 64;
    //     let intrablock_offset: usize = quotient % 64;
    //     let blocks_offset: usize = self.get_block(block_idx).offset.into();
    //     let intrablock_rank: usize =
    //         bitrank(self.get_block(block_idx).occupieds, intrablock_offset);

    //     if intrablock_rank == 0 {
    //         if blocks_offset <= intrablock_offset {
    //             return quotient;
    //         } else {
    //             return 64 * block_idx + blocks_offset - 1;
    //         }
    //     }

    //     let mut runend_block_index: usize = block_idx + blocks_offset / 64;
    //     let mut runend_ignore_bits: usize = blocks_offset % 64;
    //     let mut runend_rank: usize = intrablock_rank - 1;
    //     let mut runend_block_offset: usize = bitselectv(
    //         self.get_block(runend_block_index).runends,
    //         runend_ignore_bits,
    //         runend_rank,
    //     );

    //     if runend_block_offset == 64 {
    //         if blocks_offset == 0 && intrablock_rank == 0 {
    //             return quotient;
    //         } else {
    //             loop {
    //                 runend_rank -= popcntv(
    //                     self.get_block(runend_block_index).runends,
    //                     runend_ignore_bits,
    //                 );
    //                 runend_block_index += 1;
    //                 runend_ignore_bits = 0;
    //                 runend_block_offset = bitselectv(
    //                     self.get_block(runend_block_index).runends,
    //                     runend_ignore_bits,
    //                     runend_rank,
    //                 );
    //                 if runend_block_offset != 64 {
    //                     break;
    //                 }
    //             }
    //         }
    //     }

    //     let runend_index = 64 * runend_block_index + runend_block_offset;
    //     if runend_index < quotient {
    //         quotient
    //     } else {
    //         runend_index
    //     }
    // }
}
