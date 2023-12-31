use libc::c_void;
use std::ops::{Deref, DerefMut};
use crate::SLOTS_PER_BLOCK;
use std::ptr::Unique;
type Remainder = u32;
use crate::utils::*;

#[repr(C)]
pub struct Block {
    // Bitmask indicating which slots in the block are occupied
    occupieds: u64,

    // Bitmask indicating which slots in the block are run ends
    runends: u64,

    // Bitmask indicating which slots in the block are counts
    counts: u64,

    // Offset value
    offset: u64,

    // Array of remainders associated with each slot in the block
    remainders: [Remainder; SLOTS_PER_BLOCK],
}

impl Block {
    fn set_occupied(&mut self, slot_index: u64) {
        set_bit(&mut self.occupieds, slot_index);
    }

    fn set_runend(&mut self, slot_index: u64) {
        set_bit(&mut self.runends, slot_index);
    }

    fn set_count(&mut self, slot_index: u64) {
        set_bit(&mut self.counts, slot_index);
    }

    fn set_remainder(&mut self, slot_index: u64, remainder: Remainder) {
        self.remainders[slot_index as usize] = remainder;
    }

    fn is_occupied(&self, slot_index: u64) -> bool {
        is_bit_set(self.occupieds, slot_index)
    }

    fn is_runend(&self, slot_index: u64) -> bool {
        is_bit_set(self.runends, slot_index)
    }

    fn is_count(&self, slot_index: u64) -> bool {
        is_bit_set(self.counts, slot_index)
    }

    fn remainder(&self, slot_index: u64) -> Remainder {
        self.remainders[slot_index as usize]
    }

    fn set_occupied_to(&mut self, slot_index: u64, bit: bool) {
        set_bit_to(&mut self.occupieds, slot_index, bit);
    }

    fn set_runend_to(&mut self, slot_index: u64, bit: bool) {
        set_bit_to(&mut self.runends, slot_index, bit);
    }

    fn set_count_to(&mut self, slot_index: u64, bit: bool) {
        set_bit_to(&mut self.counts, slot_index, bit);
    }

    fn has_metadata_bit_set(&self, slot_index: u64) -> bool {
        self.is_occupied(slot_index) || self.is_runend(slot_index) || self.is_count(slot_index)
    }
}

pub struct Blocks {
    ptr: Unique<Block>,
    len: usize,
}

impl Deref for Blocks {
    type Target = [Block];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for Blocks {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [Block] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Blocks {
    pub fn new(ptr: *mut u8, len: usize) -> Self {
        let ptr =
            unsafe { Unique::new(ptr as *mut Block).expect("Null pointer was provied to blocks") };
        Self { ptr, len }
    }
}

impl Blocks {
    fn bytes_needed(num_blocks: usize) -> usize {
        num_blocks * std::mem::size_of::<Block>()
    }

    fn run_end_quotient(&self, quotient: u64) -> u64 {
        let (block_index, slot_index) = split_quotient(quotient);
        assert!(block_index < self.len);
        let current_block_offset = self[block_index].offset;
        let intrablock_rank = bitrank(self[block_index].occupieds, slot_index);

        // There are no setbits in this block
        if intrablock_rank == 0 && current_block_offset <= slot_index {
            return quotient;
        } else if intrablock_rank == 0 {
            return SLOTS_PER_BLOCK as u64 * block_index as u64 + current_block_offset - 1;
        }
        let mut runend_block_index = block_index + (current_block_offset / 64) as usize;
        let mut runend_ignore_bits = current_block_offset % 64;
        let mut runend_rank = intrablock_rank - 1;
        let mut runend_block_offset: u64;
        loop {
            runend_block_offset = index_of_ith_bit_ignore(
                self[runend_block_index].runends,
                runend_rank,
                runend_ignore_bits,
            );
            if runend_block_offset != 64 {
                break;
            }
            runend_rank -= count_ones_ignore(self[runend_block_index].runends, runend_ignore_bits);
            runend_block_index += 1;
            if runend_block_index >= self.len {
                return self.len as u64 * SLOTS_PER_BLOCK as u64;
            }
            runend_ignore_bits = 0;
        }
        let runend_index = 64 * runend_block_index + runend_block_offset as usize;
        if (runend_index as u64) < quotient {
            quotient
        } else {
            runend_index as u64
        }
    }

    fn offset_lower_bound(&self, quotient: u64) -> u64 {
        let (block_index, slot_index) = split_quotient(quotient);
        assert!(block_index < self.len);
        let occupieds = self[block_index].occupieds;
        let offset = self[block_index].offset;
        if offset <= slot_index as u64 {
            let runends = (self[block_index].runends & bitmask(slot_index as u64)) >> offset;
            return occupieds
                .count_ones()
                .checked_sub(runends.count_ones())
                .unwrap() as u64;
        }
        offset + occupieds.count_ones() as u64 - slot_index as u64
    }

    fn run_start_quotient(&self, quotient: u64) -> u64 {
        if quotient == 0 {
            return 0;
        }
        self.run_end_quotient(quotient - 1) + 1
    }

    fn find_first_empty_slot(&self, mut from_quotient: u64) -> u64 {
        loop {
            let jump = self.offset_lower_bound(from_quotient);
            if jump == 0 {
                return from_quotient;
            }
            from_quotient += jump;
        }
    }

    fn merge_insert(
        &mut self,
        insert_pointer: &mut u64,
        quotient: u64,
        next_quotient: u64,
        remainder: u64,
        count: u64,
    ) {
        if count == 0 {
            return;
        }
        let remainder = remainder as Remainder;
        let (quotient_block_index, quotient_slot_index) = split_quotient(quotient);
        assert!(quotient_block_index < self.len);
        set_bit(
            &mut self[quotient_block_index].occupieds,
            quotient_slot_index,
        );
        if *insert_pointer < quotient {
            *insert_pointer = quotient;
        }
        let (insert_block_index, insert_slot_index) = split_quotient(*insert_pointer);
        let (mut end_of_insert_block_index, mut end_of_insert_slot_index) =
            split_quotient(self.run_end_quotient(*insert_pointer));
        *insert_pointer += 1;
        self[insert_block_index].remainders[insert_slot_index as usize] = remainder;
        if count > 1 {
            let (count_block_index, count_slot_index) = split_quotient(*insert_pointer + 1);
            self[count_block_index].remainders[count_slot_index as usize] = count;
            (end_of_insert_block_index, end_of_insert_slot_index) =
                split_quotient(self.run_end_quotient(*insert_pointer));
            *insert_pointer += 1;
        }
        if quotient != next_quotient {
            set_bit(
                &mut self[end_of_insert_block_index].runends,
                end_of_insert_slot_index,
            );
        }
        for i in (quotient_block_index + 1)..=end_of_insert_block_index {
            self[i].offset = (end_of_insert_block_index - i) as u64 * SLOTS_PER_BLOCK as u64
                + end_of_insert_slot_index as u64;
        }
    }

    // returns new unique values, new occupied slots
    fn insert(&mut self, quotient: u64, remainder: u64, count: u64) -> (u64, u64) {
        assert!(count != 0);
        let (quotient_block_index, quotient_slot_index) 
            = split_quotient(quotient);
        assert!(quotient_block_index < self.len);
        let remainder = remainder as Remainder;
        let runstart_index = self.run_start_quotient(quotient);
        // Case 1: Can insert into empty slot
        if !self[quotient_block_index].has_metadata_bit_set(quotient_slot_index)
            && runstart_index == quotient
        {
            self[quotient_block_index].set_occupied(quotient_slot_index);
            self[quotient_block_index].set_remainder(quotient_slot_index, remainder);
            if count == 1 {
                self[quotient_block_index].set_runend(quotient_slot_index);
                return (1,1);
            }
            let count_quotient = quotient + 1;
            let first_free_slot = self.find_first_empty_slot(count_quotient);
            if first_free_slot != count_quotient {
                self.shift_right(count_quotient, first_free_slot);
            }
            self[block_index_of(count_quotient)].set_count(slot_index_of(count_quotient));
            self[block_index_of(count_quotient)].set_remainder(slot_index_of(count_quotient), count.try_into().unwrap_or(Remainder::MAX));
            self[block_index_of(count_quotient)].set_runend(slot_index_of(count_quotient));
            return (1, 2);
        } else if !self[quotient_block_index].is_occupied(quotient_slot_index) {
            // Case 2, need to shift in order to insert
            let first_free_slot = self.find_first_empty_slot(runstart_index);
            self.shift_right(runstart_index, first_free_slot);
            self[quotient_block_index].set_occupied(quotient_slot_index);
            let (insert_block_index, insert_slot_index) = split_quotient(runstart_index);
            self[insert_block_index].set_remainder(insert_slot_index, remainder);
            if count == 1 {
                self[insert_block_index].set_runend(insert_slot_index);
                return (1, 1);
            }
            let count_quotient = runstart_index + 1;
            let first_free_slot = self.find_first_empty_slot(count_quotient);
            
        } else {
            // case 3: have to find where to insert, may or may not already be in the run
            let mut pointer = quotient;
            let mut pointer_block = &mut self[block_index_of(pointer)];
            let mut pointer_slot_index = slot_index_of(pointer);
            while !pointer_block.is_runend(pointer_slot_index) {
                if !pointer_block.is_count(pointer_slot_index) &&
                    pointer_block.remainder(pointer_slot_index) == remainder
                {
                    let count_quotient = pointer + 1;
                    if self[block_index_of(count_quotient)].is_count(slot_index_of(count_quotient)) {
                        let new_count = (self[block_index_of(count_quotient)].remainder(slot_index_of(count_quotient)) as u64).checked_add(count).unwrap_or(u64::MAX).try_into().unwrap_or(Remainder::MAX);
                        self[block_index_of(count_quotient)].set_remainder(slot_index_of(count_quotient), new_count);
                        return (0, 0);
                    }
                    let next_empty_slot = self.find_first_empty_slot(count_quotient);
                    if next_empty_slot != count_quotient {
                        self.shift_right(count_quotient, next_empty_slot);
                    }
                    break;
                } else if !pointer_block.is_count(pointer_slot_index) &&
                    pointer_block.remainder(pointer_slot_index) > remainder
                {

                }
                pointer += 1;
                pointer_block = &mut self[block_index_of(pointer)];
                pointer_slot_index = slot_index_of(pointer);
            }

        }

        // if is_bit_set(self[block_index].occupieds, position)

        // set_bit(
        //     &mut self[block_index].occupieds,
        //     slot_index,
        // );

        (0, 0)
    }

    fn make_two_contigous_slots(&mut self, quotient: u64) {
        let (block_index, slot_index) = split_quotient(quotient);
        let first_free_slot = self.find_first_empty_slot(pointer);
        let second_free_slot = self.find_first_empty_slot(first_free_slot + 1);
        if first_free_slot == quotient && second_free_slot == quotient + 1 {
            return;
        }
        self.shift_right(quotient, first_free_slot);
        self.shift_right(quotient+1, second_free_slot);
    }

    fn shift_right(&mut self, left: u64, right: u64) {
        for index in ((left + 1)..=right).rev() {
            let (block_index, slot_index) = split_quotient(index);
            let (prev_block_index, prev_slot_index) = split_quotient(index - 1);
            if prev_block_index != block_index {
                self[block_index].offset += 1;
            }
            let mut bit_set = self[prev_block_index].is_occupied(prev_slot_index);
            self[block_index].set_occupied_to(slot_index, bit_set);
            bit_set = self[prev_block_index].is_runend(prev_slot_index);
            self[block_index].set_runend_to(slot_index, bit_set);
            bit_set = self[prev_block_index].is_count(prev_slot_index);
            self[block_index].set_count_to(slot_index, bit_set);
            self[block_index].remainders[slot_index as usize] =
                self[prev_block_index].remainders[prev_slot_index as usize];
        }
    }

    fn shift_left(&mut self, left: u64, right: u64) {
        todo!();
    }
}

// impl Blocks for U32Blocks {
//     type Remainder = Remainder;

//     fn bytes_needed(num_blocks: usize) -> usize {
//         num_blocks * std::mem::size_of::<Block>()
//     }

//     fn offset(&self, quotient: u64) -> Offset {
//         let (block_index, _) = Self::split_quotient(quotient);
//         self.offset_by_block(block_index)
//     }

//     #[inline(always)]
//     fn decode_counter(&self, quotient: &mut u64, remainder: &mut Self::Remainder, count: &mut u64) {
//         let (block_index, slot_index) = Self::split_quotient(*quotient);
//         *remainder = *self.slot_by_block(block_index, slot_index);
//         if self.is_runend(*quotient) || !self.is_count(*quotient + 1) {
//             *count = 1;
//         } else {
//             // Only works for u64
//             *count = *self.slot(*quotient + 1) as u64;
//             *quotient += 1;
//             // let mut qptr = *quotient + 1;
//             // let mut c: u64 = 0;
//             // while self.is_count(qptr) {
//             //     c <<= Self::Remainder::BITS;
//             //     c |= self.slot(qptr);
//             //     qptr += 1;
//             // }
//             // *quotient = qptr;
//             // *count = c;
//         }
//     }

//     fn madvise_dont_need(&self, current_quotient: u64) {
//         let ptr_start = self.ptr.as_ptr() as *mut c_void;
//         let aligned_ptr_start = unsafe { ptr_start.offset(ptr_start.align_offset(4096) as isize) };
//         let ptr_end =
//             unsafe { (self.slot(current_quotient) as *const Self::Remainder).offset(-4096) };
//         if ptr_end as usize > aligned_ptr_start as usize {
//             let len = ptr_end as usize - aligned_ptr_start as usize;
//             let madv_result = unsafe { libc::madvise(aligned_ptr_start, len, libc::MADV_DONTNEED) };
//             if madv_result != 0 {
//                 panic!("madvise failed: {}", madv_result);
//             }
//         }
//     }

//     fn advise_seq(&self) {
//         let ptr_start = self.ptr.as_ptr() as *mut c_void;
//         let aligned_ptr_start = unsafe { ptr_start.offset(ptr_start.align_offset(4096) as isize) };
//         let ptr_end = unsafe { (self.ptr.as_ptr() as *const Block).offset(self.len as isize) };
//         let len = ptr_end as usize - aligned_ptr_start as usize;
//         let madv_result = unsafe { libc::madvise(aligned_ptr_start, len, libc::MADV_SEQUENTIAL) };
//         if madv_result != 0 {
//             panic!("madvise failed: {}", madv_result);
//         }
//     }

//     fn advise_normal(&self) {
//         let ptr_start = self.ptr.as_ptr() as *mut c_void;
//         let aligned_ptr_start = unsafe { ptr_start.offset(ptr_start.align_offset(4096) as isize) };
//         let ptr_end = unsafe { (self.ptr.as_ptr() as *const Block).offset(self.len as isize) };
//         let len = ptr_end as usize - aligned_ptr_start as usize;
//         let madv_result = unsafe { libc::madvise(aligned_ptr_start, len, libc::MADV_RANDOM) };
//         if madv_result != 0 {
//             panic!("madvise failed: {}", madv_result);
//         }
//     }
// }
