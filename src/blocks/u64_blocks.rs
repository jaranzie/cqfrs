use libc::c_void;

use super::{Blocks, Offset};
use crate::SLOTS_PER_BLOCK;
use std::ops::{Deref, DerefMut};
use std::ptr::Unique;
pub type Remainder = u64;

#[repr(C)]
pub struct Block {
    occupieds: u64,
    runends: u64,
    counts: u64,
    remainders: [Remainder; SLOTS_PER_BLOCK],
    offset: u16,
    // padding: [u16; 3],
}

pub struct U64Blocks {
    ptr: Unique<Block>,
    len: usize,
}

impl U64Blocks {
    pub fn new(ptr: *mut u8, len: usize) -> Self {
        let ptr = unsafe { Unique::new_unchecked(ptr as *mut Block) };
        Self { ptr, len }
    }
}

impl Blocks for U64Blocks {
    type Remainder = Remainder;

    fn bytes_needed(num_blocks: usize) -> usize {
        num_blocks * std::mem::size_of::<Block>()
    }

    fn offset(&self, quotient: u64) -> Offset {
        let (block_index, _) = Self::split_quotient(quotient);
        self.offset_by_block(block_index)
    }

    fn decode_counter(&self, quotient: &mut u64, remainder: &mut Self::Remainder, count: &mut u64) {
        let (block_index, slot_index) = Self::split_quotient(*quotient);
        *remainder = *self.slot_by_block(block_index, slot_index);
        if self.is_runend(*quotient) || !self.is_count(*quotient + 1) {
            *count = 1;
        } else {
            // Only works for u64
            *count = *self.slot(*quotient + 1);
            *quotient += 1;
            // let mut qptr = *quotient + 1;
            // let mut c: u64 = 0;
            // while self.is_count(qptr) {
            //     c <<= Self::Remainder::BITS;
            //     c |= self.slot(qptr);
            //     qptr += 1;
            // }
            // *quotient = qptr;
            // *count = c;
        }
    }

    fn offset_mut(&mut self, quotient: u64) -> &mut Offset {
        let (block_index, _) = Self::split_quotient(quotient);
        self.offset_by_block_mut(block_index)
    }

    fn occupieds(&self, quotient: u64) -> u64 {
        let (block_index, _) = Self::split_quotient(quotient);
        self.occupieds_by_block(block_index)
    }

    fn runends(&self, quotient: u64) -> u64 {
        let (block_index, _) = Self::split_quotient(quotient);
        self.runends_by_block(block_index)
    }

    fn counts(&self, quotient: u64) -> u64 {
        let (block_index, _) = Self::split_quotient(quotient);
        self.counts_by_block(block_index)
    }

    fn slot(&self, quotient: u64) -> &Self::Remainder {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        &self.slot_by_block(block_index, slot_index)
    }

    fn slot_mut(&mut self, quotient: u64) -> &mut Self::Remainder {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.slot_by_block_mut(block_index, slot_index)
    }

    fn is_occupied(&self, quotient: u64) -> bool {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.is_occupied_by_block(block_index, slot_index)
    }

    fn is_runend(&self, quotient: u64) -> bool {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.is_runend_by_block(block_index, slot_index)
    }

    fn is_count(&self, quotient: u64) -> bool {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.is_count_by_block(block_index, slot_index)
    }

    fn set_occupied(&mut self, quotient: u64, bit: bool) {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.set_occupied_by_block(block_index, slot_index, bit)
    }

    fn set_runend(&mut self, quotient: u64, bit: bool) {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.set_runend_by_block(block_index, slot_index, bit)
    }

    fn set_count(&mut self, quotient: u64, bit: bool) {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.set_count_by_block(block_index, slot_index, bit)
    }

    fn offset_by_block(&self, block: usize) -> Offset {
        self[block].offset as Offset
    }

    fn offset_by_block_mut(&mut self, block: usize) -> &mut Offset {
        &mut self[block].offset
    }

    fn occupieds_by_block(&self, block: usize) -> u64 {
        self[block].occupieds
    }

    fn runends_by_block(&self, block: usize) -> u64 {
        self[block].runends
    }

    fn counts_by_block(&self, block: usize) -> u64 {
        self[block].counts
    }

    fn slot_by_block(&self, block: usize, slot: usize) -> &Self::Remainder {
        &self[block].remainders[slot]
    }

    fn slot_by_block_mut(&mut self, block: usize, slot: usize) -> &mut Self::Remainder {
        &mut self[block].remainders[slot]
    }

    fn is_occupied_by_block(&self, block: usize, slot: usize) -> bool {
        self[block].occupieds & (1 << slot) != 0
    }

    fn is_runend_by_block(&self, block: usize, slot: usize) -> bool {
        self[block].runends & (1 << slot) != 0
    }

    fn is_count_by_block(&self, block: usize, slot: usize) -> bool {
        self[block].counts & (1 << slot) != 0
    }

    fn set_occupied_by_block(&mut self, block: usize, slot: usize, bit: bool) {
        if bit {
            self[block].occupieds |= 1 << slot;
        } else {
            self[block].occupieds &= !(1 << slot);
        }
    }

    fn set_runend_by_block(&mut self, block: usize, slot: usize, bit: bool) {
        if bit {
            self[block].runends |= 1 << slot;
        } else {
            self[block].runends &= !(1 << slot);
        }
    }

    fn set_count_by_block(&mut self, block: usize, slot: usize, bit: bool) {
        if bit {
            self[block].counts |= 1 << slot;
        } else {
            self[block].counts &= !(1 << slot);
        }
    }

    fn num_blocks(&self) -> usize {
        self.len
    }

    fn madvise_dont_need(&self, current_quotient: u64) {
        let ptr_start = self.ptr.as_ptr() as *mut c_void;
        let aligned_ptr_start = unsafe { ptr_start.offset(ptr_start.align_offset(4096) as isize) };
        let ptr_end =
            unsafe { (self.slot(current_quotient) as *const Self::Remainder).offset(-4096) };
        if ptr_end as usize > aligned_ptr_start as usize {
            let len = ptr_end as usize - aligned_ptr_start as usize;
            unsafe { libc::madvise(ptr_start, len, libc::MADV_DONTNEED) };
        }
    }
}

impl Deref for U64Blocks {
    type Target = [Block];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for U64Blocks {
    fn deref_mut(&mut self) -> &mut [Block] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}
