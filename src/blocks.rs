use super::BlockT;
use super::QF_SLOTS_PER_BLOCK;
use super::{bitmask, bitrank, bitselectv, popcntv};
use bitintr::{Pdep, Popcnt, Tzcnt};
use std::cell::SyncUnsafeCell;

use crate::{BlockIndex, Quotient, Remainder, SlotIndex};


#[repr(C)]
pub struct Block {
    occupieds: u64,
    runends: u64,
    counts: u64,
    remainders: [BlockT; QF_SLOTS_PER_BLOCK],
    pub offset: u16,
}

impl Block {
    #[inline]
    pub fn flip_occupied(&mut self, slot: SlotIndex) {
        self.occupieds ^= 1 << slot;
    }

    #[inline]
    pub fn is_occupied(&self, slot: SlotIndex) -> bool {
        ((self.occupieds >> slot) & 1) != 0
    }

    pub fn set_occupied(&mut self, slot: SlotIndex, set: bool) {
        if set {
            self.occupieds |= 1 << slot;
        } else {
            self.occupieds &= !(1 << slot);
        }
    }

    #[inline]
    pub fn flip_runend(&mut self, slot: SlotIndex) {
        self.runends ^= 1 << slot;
    }

    #[inline]
    pub fn is_runend(&self, slot: SlotIndex) -> bool {
        ((self.runends >> slot) & 1) != 0
    }

    pub fn set_runend(&mut self, slot: SlotIndex, set: bool) {
        if set {
            self.runends |= 1 << slot;
        } else {
            self.runends &= !(1 << slot);
        }
    }

    #[inline]
    pub fn flip_count(&mut self, slot: SlotIndex) {
        self.counts ^= 1 << slot;
    }

    #[inline]
    pub fn is_count(&self, slot: SlotIndex) -> bool {
        ((self.counts >> slot) & 1) != 0
    }

    pub fn set_count(&mut self, slot: SlotIndex, set: bool) {
        if set {
            self.counts |= 1 << slot;
        } else {
            self.counts &= !(1 << slot);
        }
    }

    #[inline]
    pub fn set_slot(&mut self, slot: SlotIndex, remainder: Remainder) {
        self.remainders[slot as usize] = remainder;
    }

    #[inline]
    pub fn get_slot(&self, slot: SlotIndex) -> Remainder {
        self.remainders[slot as usize]
    }

    pub fn get_offset(&self) -> u16 {
        self.offset
    }

    pub fn set_offset(&mut self, offset: u16) {
        self.offset = offset;
    }

    pub fn has_metadata_bits_set(&self, slot: SlotIndex) -> bool {
        self.is_occupied(slot) || self.is_runend(slot) || self.is_count(slot)
    }

    pub fn offset_lower_bound(&self, slot: SlotIndex) -> u64 {
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
    // pub fn might_be_empty(&self, quotient: Quotient) -> bool {
    //     let block = self.get_block(quotient);
    //     return block.is_count(quotient % 64)
    //         && block.is_occupied(quotient % 64)
    //         && block.is_runend(quotient % 64);
    // }
    pub fn get_block(&self, quotient: Quotient) -> &Block {
        unsafe { &*self.0.get_unchecked(quotient as usize).get() }
    }

    pub fn get_block_mut(&self, quotient: Quotient) -> &mut Block {
        unsafe { &mut *self.0.get_unchecked(quotient as usize).get() }
    }

    pub fn has_metadata_bits_set(&self, quotient: Quotient) -> bool {
        let block = self.get_block(quotient);
        block.is_occupied(quotient % 64)
            && block.is_runend(quotient % 64)
            && block.is_count(quotient % 64)
    }

    pub fn is_occupied(&self, quotient: Quotient) -> bool {
        let block = self.get_block(quotient);
        block.is_occupied(quotient % 64)
    }

    pub fn is_runend(&self, quotient: Quotient) -> bool {
        let block = self.get_block(quotient);
        block.is_runend(quotient % 64)
    }

    pub fn is_count(&self, quotient: Quotient) -> bool {
        let block = self.get_block(quotient);
        block.is_count(quotient % 64)
    }

    pub fn set_occupied(&self, quotient: Quotient, bit: bool) {
        let block = self.get_block_mut(quotient);
        block.set_occupied(quotient % 64, bit)
    }

    pub fn set_runend(&self, quotient: Quotient, bit: bool) {
        let block = self.get_block_mut(quotient);
        block.set_runend(quotient % 64, bit)
    }

    pub fn set_count(&self, quotient: Quotient, bit: bool) {
        let block = self.get_block_mut(quotient);
        block.set_count(quotient % 64, bit)
    }

    pub fn flip_count(&self, quotient: Quotient) {
        let block = self.get_block_mut(quotient);
        block.flip_count(quotient % 64)
    }

    pub fn flip_runend(&self, quotient: Quotient) {
        let block = self.get_block_mut(quotient);
        block.flip_runend(quotient % 64)
    }

    pub fn flip_occupied(&self, quotient: Quotient) {
        let block = self.get_block_mut(quotient);
        block.flip_occupied(quotient % 64)
    }

    pub fn set_remainder(&self, quotient: Quotient, remainder: u64) {
        let block = self.get_block_mut(quotient);
        (block).set_slot(quotient % 64, remainder)
    }

    pub fn get_remainder(&self, quotient: Quotient) -> Remainder {
        let block = self.get_block(quotient);
        (block).get_slot(quotient % 64)
    }

    pub fn decode_counter(&self, quotient: Quotient) -> (u64, u64) {
        let block = self.get_block(quotient);
        let next_block = self.get_block(quotient + 1);
        if !next_block.is_count(quotient % 64) || block.is_runend(quotient % 64) {
            return (quotient + 1, 1);
        } else {
            let count = next_block.get_slot(quotient % 64);
            return (quotient + 2, count);
        }
    }

    pub fn find_run_start(&self, quotient: Quotient) -> Quotient {
        // let block = self.get_block(quotient);
        // let offset : u64= block.get_offset().into();
        // if offset <= quotient%64 {
        //     return quotient;
        // } else {
        //     return quotient - offset + 64;
        // }
        return self.find_run_end(quotient - 1) + 1;
    }

    pub fn find_run_end(&self, quotient: Quotient) -> Quotient {
        let block_idx = quotient / 64;
        let slot_index = quotient % 64;
        let offset: u64 = self.get_block(block_idx).offset.into();
        let num_occupied_before_slot = bitrank(self.get_block(block_idx).occupieds, slot_index);

        // Case where no occupied slots before one we're looking for
        if num_occupied_before_slot == 0 {
            // If slot index is outside of offset, return quotient
            if offset <= slot_index {
                return quotient;
            } else {
                // Otherwise, quotient is shifted by offset
                return (block_idx * 64) + offset - 1;
                // return quotient + offset - 1;
            }
        }

        let mut runend_block_index = block_idx + offset / 64;
        let mut runend_ignore_bits = offset % 64;
        let mut runend_rank = num_occupied_before_slot - 1;
        let mut runend_block_offset = bitselectv(
            self.get_block(runend_block_index).runends,
            runend_ignore_bits,
            runend_rank,
        );

        if runend_block_offset == 64 {
            // if offset == 0 && num_occupied_before_slot == 0 {
            //     return quotient;
            // } else {
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
            // }
        }

        let runend_index = 64 * runend_block_index + runend_block_offset;
        if runend_index < quotient {
            quotient
        } else {
            runend_index
        }
    }

    pub fn get_run_range(&self, quotient: Quotient) -> (Quotient, Quotient) {
        let run_start = self.find_run_start(quotient);
        let run_end = self.find_run_end(quotient);
        return (run_start, run_end);
    }

    /// quotient is assumed to be run start
    pub fn find_quotient_of_remainder(&self, quotient: Quotient, remainder: Remainder) -> Quotient {
        let run_start = quotient;
        let mut current_quotient = run_start;
        let mut current_remainder = self.get_remainder(run_start);
        while current_remainder < remainder && !self.is_runend(current_quotient) {
            if current_remainder == remainder {
                return current_quotient;
            }
            let (next_quotient, count) = self.decode_counter(current_quotient);
            current_quotient = next_quotient;
            current_remainder = self.get_remainder(current_quotient);
        }
        return current_quotient;
    }
}
