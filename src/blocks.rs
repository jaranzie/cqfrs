use std::cell::SyncUnsafeCell;

use super::BlockT;
use super::QF_SLOTS_PER_BLOCK;
use super::{bitmask, bitrank, bitselectv, popcntv};
use bitintr::{Pdep, Popcnt, Tzcnt};

#[repr(C)]
pub struct Block {
    pub occupieds: u64,
    pub runends: u64,
    counts: u64,
    remainders: [BlockT; QF_SLOTS_PER_BLOCK],
    pub offset: u16,
    // occupieds: [u64; QF_METADATA_WORDS_PER_BLOCK],
    // runends: [u64; QF_METADATA_WORDS_PER_BLOCK],
    // counts: [u64; QF_METADATA_WORDS_PER_BLOCK],
}

impl Block {
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

    pub fn set_slot(&mut self, slot: usize, remainder: u64) {
        self.remainders[slot] = remainder;
    }

    pub fn get_slot(&self, slot: usize) -> u64 {
        self.remainders[slot]
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
}

#[repr(C)]
pub struct Blocks(pub [SyncUnsafeCell<Block>; 1]);

impl Blocks {
    pub fn get_block(&self, block_index: usize) -> &Block {
        unsafe { &*self.0.get_unchecked(block_index).get() }
    }

    pub fn get_block_mut(&self, block_index: usize) -> &mut Block {
        unsafe { &mut *self.0.get_unchecked(block_index).get() }
    }

    pub fn is_empty(&self, block_index: usize, slot_index: usize) -> bool {
        let block = self.get_block(block_index);
        !block.is_occupied(slot_index)
            && !block.is_runend(slot_index)
            && !block.is_count(slot_index)
    }

    pub fn is_occupied(&self, block_index: usize, slot_index: usize) -> bool {
        let block = self.get_block(block_index);
        block.is_occupied(slot_index)
    }

    pub fn is_runend(&self, block_index: usize, slot_index: usize) -> bool {
        let block = self.get_block(block_index);
        block.is_runend(slot_index)
    }

    pub fn is_count(&self, block_index: usize, slot_index: usize) -> bool {
        let block = self.get_block(block_index);
        block.is_count(slot_index)
    }

    pub fn set_occupied(&self, block_index: usize, slot_index: usize, bit: bool) {
        let block = self.get_block_mut(block_index);
        unsafe { (*block).set_occupied(slot_index, bit) }
    }

    pub fn set_runend(&self, block_index: usize, slot_index: usize, bit: bool) {
        let block = self.get_block_mut(block_index);
        unsafe { (*block).set_runend(slot_index, bit) }
    }

    pub fn set_count(&self, block_index: usize, slot_index: usize, bit: bool) {
        let block = self.get_block_mut(block_index);
        unsafe { (*block).set_count(slot_index, bit) }
    }

    pub fn set_remainder(&self, block_index: usize, slot_index: usize, remainder: u64) {
        let block = self.get_block_mut(block_index);
        unsafe { (*block).set_slot(slot_index, remainder) }
    }

    pub fn get_remainder(&self, block_index: usize, slot_index: usize) -> u64 {
        let block = self.get_block(block_index);
        unsafe { (*block).get_slot(slot_index) }
    }

    pub fn set_remainder_block(block: &mut Block, slot_index: usize, remainder: u64) {
        unsafe { (*block).set_slot(slot_index, remainder) }
    }

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


    fn run_end(&self, quotient: usize) -> usize {
        let block_idx: usize = quotient / 64;
        let intrablock_offset: usize = quotient % 64;
        let block = self.get_block_mut(block_idx);
        let blocks_offset: usize = block.offset.into();
        let intrablock_rank: usize =
            bitrank(block.occupieds, intrablock_offset);

        if intrablock_rank == 0 {
            if blocks_offset <= intrablock_offset {
                return quotient;
            } else {
                return 64 * block_idx + blocks_offset - 1;
            }
        }

        let mut runend_block_index: usize = block_idx + blocks_offset / 64;
        let mut runend_ignore_bits: usize = blocks_offset % 64;
        let mut runend_rank: usize = intrablock_rank - 1;
        let mut runend_block_offset: usize = bitselectv(
            self.get_block(runend_block_index).runends,
            runend_ignore_bits,
            runend_rank,
        );

        if runend_block_offset == 64 {
            if blocks_offset == 0 && intrablock_rank == 0 {
                return quotient;
            } else {
                loop {
                    runend_rank -= popcntv(
                        self.get_block(runend_block_index).runends,
                        runend_ignore_bits,
                    );
                    runend_block_index += 1;
                    runend_ignore_bits = 0;
                    runend_block_offset = bitselectv(
                        self.get_block(runend_block_index).runends,
                        runend_ignore_bits,
                        runend_rank,
                    );
                    if runend_block_offset != 64 {
                        break;
                    }
                }
            }
        }

        let runend_index = 64 * runend_block_index + runend_block_offset;
        if runend_index < quotient {
            quotient
        } else {
            runend_index
        }
    }
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

// Blocks::set_remainder_block(&mut block, slot_index, remainder);
