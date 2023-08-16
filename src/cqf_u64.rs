use crate::ReversibleHasher;
type Remainder = u64;
const SLOTS_PER_BLOCK: usize = 64;
use crate::{bitmask, bitrank, bitselect, bitselectv, popcntv};
pub struct HashCount {
    pub hash: u64,
    pub count: u64,
}

mod blocks {
    use super::Remainder;
    use super::SLOTS_PER_BLOCK;
    use crate::utils::*;
    use bitintr::{Pdep, Popcnt, Tzcnt};
    use std::cell::SyncUnsafeCell;
    use std::ops::Deref;
    use std::ops::DerefMut;
    use std::ops::Index;
    use std::ops::IndexMut;
    use std::ptr::Unique;

    #[repr(C)]
    pub struct Block {
        occupieds: u64,
        runends: u64,
        counts: u64,
        remainders: [Remainder; SLOTS_PER_BLOCK],
        offset: u16,
    }

    impl Block {
        pub fn occupieds(&self) -> u64 {
            self.occupieds
        }

        pub fn runends(&self) -> u64 {
            self.runends
        }

        pub fn counts(&self) -> u64 {
            self.counts
        }

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

        pub fn has_metadata_bits_set(&self, slot: usize) -> bool {
            self.is_occupied(slot) && self.is_runend(slot) && self.is_count(slot)
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
            for i in 0..SLOTS_PER_BLOCK {
                self.remainders[i] = match Remainder::try_from(0) {
                    Ok(remainder) => remainder,
                    Err(_) => panic!("Remainder type must be able to be created from 0"),
                }; // maybe try_from.unwrap() with bitmask before
            }
        }
    }

    pub struct Blocks {
        ptr: Unique<Block>,
        len: usize,
        // inner: &[Block<Remainder>]
    }

    impl Deref for Blocks {
        type Target = [Block];

        fn deref(&self) -> &Self::Target {
            unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
        }
    }

    impl DerefMut for Blocks {
        fn deref_mut(&mut self) -> &mut [Block] {
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

    impl Blocks {
        pub fn new(ptr: Unique<Block>, len: usize) -> Self {
            Self { ptr, len }
        }

        pub fn offset(&self, quotient: u64) -> u16 {
            let (block_index, _) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].offset
        }

        pub fn set_offset(&mut self, quotient: u64, offset: u16) {
            let (block_index, _) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].offset = offset;
        }

        // pub fn get_block(&self, block_index: usize) -> &Block<Remainder> {
        //     &self[block_index]
        // }

        // pub fn get_block_mut(&mut self, block_index: usize) -> &mut Block<Remainder> {
        //     &mut self[block_index]
        // }

        pub fn get_slot(&self, quotient: u64) -> Remainder {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            *self[block_index].slot(slot_index)
        }

        pub fn set_slot(&mut self, quotient: u64, remainder: Remainder) {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            *self[block_index].slot_mut(slot_index) = remainder;
        }

        pub fn is_empty(&self, quotient: u64) -> bool {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            let block = &self[block_index];
            !block.is_occupied(slot_index)
                && !block.is_runend(slot_index)
                && !block.is_count(slot_index)
        }

        pub fn is_occupied(&self, quotient: u64) -> bool {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].is_occupied(slot_index)
        }

        pub fn is_runend(&self, quotient: u64) -> bool {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].is_runend(slot_index)
        }

        pub fn is_count(&self, quotient: u64) -> bool {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].is_count(slot_index)
        }

        pub fn set_occupied(&mut self, quotient: u64, bit: bool) {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].set_occupied(slot_index, bit)
        }

        pub fn set_runend(&mut self, quotient: u64, bit: bool) {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].set_runend(slot_index, bit)
        }

        pub fn set_count(&mut self, quotient: u64, bit: bool) {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].set_count(slot_index, bit)
        }

        pub fn flip_count(&mut self, quotient: u64) {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].flip_count(slot_index)
        }

        pub fn flip_occupied(&mut self, quotient: u64) {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].flip_occupied(slot_index)
        }

        pub fn flip_runend(&mut self, quotient: u64) {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].flip_runend(slot_index)
        }

        pub fn has_metadata_bits_set(&self, quotient: u64) -> bool {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].has_metadata_bits_set(slot_index)
        }

        fn block_slot_index_from_quotient(quotient: u64) -> (usize, usize) {
            let block_index = (quotient / SLOTS_PER_BLOCK as u64) as usize;
            let slot_index = (quotient % SLOTS_PER_BLOCK as u64) as usize;
            (block_index as usize, slot_index as usize)
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

        pub fn offset_lower_bound(&self, quotient: u64) -> u64 {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].offset_lower_bound(slot_index as u64)
        }

        pub fn run_end(&self, quotient: u64) -> u64 {
            let block_idx: u64 = (quotient / SLOTS_PER_BLOCK as u64);
            let intrablock_offset: u64 = (quotient % SLOTS_PER_BLOCK as u64);
            let blocks_offset: u64 = self[block_idx as usize].offset.into();
            let intrablock_rank: u64 =
                bitrank(self[block_idx as usize].occupieds, intrablock_offset);

            if intrablock_rank == 0 {
                if blocks_offset <= intrablock_offset {
                    return quotient;
                } else {
                    return 64 * block_idx as u64 + blocks_offset as u64 - 1;
                }
            }

            let mut runend_block_index: u64 = block_idx + blocks_offset / 64;
            let mut runend_ignore_bits: u64 = blocks_offset % 64;
            let mut runend_rank: u64 = intrablock_rank - 1;
            let mut runend_block_offset: u64 = bitselectv(
                self[runend_block_index as usize].runends,
                runend_ignore_bits,
                runend_rank,
            );

            if runend_block_offset == 64 {
                if blocks_offset == 0 && intrablock_rank == 0 {
                    return quotient;
                } else {
                    loop {
                        runend_rank -= popcntv(
                            self[runend_block_index as usize].runends,
                            runend_ignore_bits,
                        );
                        runend_block_index += 1;
                        runend_ignore_bits = 0;
                        runend_block_offset = bitselectv(
                            self[runend_block_index as usize].runends,
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
            if (runend_index as u64) < quotient {
                quotient
            } else {
                runend_index as u64
            }
        }

        pub fn decode_counter(&self, quotient: u64, remainder: &mut u64, count: &mut u64) -> u64 {
            let block_index: usize = (quotient / SLOTS_PER_BLOCK as u64) as usize;
            let slot_index: usize = (quotient % SLOTS_PER_BLOCK as u64) as usize;
            *remainder = *self[block_index].slot(slot_index);

            // if it's a runend or the next thing is not a count, there's only one
            if self.is_runend(quotient) || !self.is_count(quotient + 1) {
                *count = 1;
                return quotient;
            } else {
                // otherwise, whatever is in the next slot is the count
                *count = self.get_slot(quotient + 1);
                return quotient + 1;
            }
        }
    }
}

use crate::CqfError;
use crate::{Metadata, RuntimeData};
use blocks::{Block, Blocks};
use crossbeam::utils::CachePadded;
use libc::{
    c_void, madvise, mmap, munmap, MADV_RANDOM, MAP_ANONYMOUS, MAP_FAILED, MAP_HUGETLB,
    MAP_PRIVATE, MAP_SHARED, PROT_READ, PROT_WRITE,
};
use parking_lot::Mutex;
use std::cmp::min;
use std::fs::OpenOptions;
use std::hash::{self, BuildHasher, Hasher};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::ptr::{self, NonNull, Unique};
use std::sync::Arc;
use std::{
    fs::File,
    sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering},
};

pub struct CountingQuotientFilter<'a, Hasher: BuildHasher> {
    runtimedata: Box<RuntimeData<Hasher>>,
    metadata: &'a Metadata,
    blocks: Blocks,
}

/// lognslots should be atleast as big as quotient_bits, probably equal is best
impl<'a, Hasher: BuildHasher> CountingQuotientFilter<'a, Hasher> {
    fn valid_args(lognslots: u64, quotient_bits: u64, hash_bits: u64) -> bool {
        if quotient_bits > Remainder::BITS as u64 {
            return false;
        } else if quotient_bits == 0 {
            return false;
        } else if quotient_bits > lognslots {
            return false;
        }
        true
    }

    pub fn new(
        lognslots: u64,
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        hasher: Hasher,
    ) -> Result<Self, CqfError> {
        if !Self::valid_args(lognslots, quotient_bits, hash_bits) {
            return Err(CqfError::InvalidArguments);
        }
        let init_metadata = Metadata::new(
            lognslots,
            quotient_bits,
            Remainder::BITS as u64,
            hash_bits,
            std::mem::size_of::<Block>() as u64,
            invertable,
        );
        let buffer = unsafe {
            mmap(
                ptr::null_mut(),
                init_metadata.total_size_in_bytes as usize,
                PROT_READ | PROT_WRITE,
                MAP_ANONYMOUS | MAP_PRIVATE,
                -1,
                0,
            )
        };
        if buffer == MAP_FAILED {
            return Err(CqfError::MmapError);
        }
        let metadata = unsafe { &mut *(buffer as *mut Metadata) };
        let blocks_ptr =
            unsafe { buffer.offset(std::mem::size_of::<Metadata>() as isize) as *mut Block };
        let blocks = Blocks::new(
            Unique::new(blocks_ptr).unwrap(),
            init_metadata.num_blocks as usize,
        );
        *metadata = init_metadata;

        let cqf = CountingQuotientFilter {
            blocks,
            metadata,
            runtimedata: Box::new(RuntimeData {
                in_memory: true,
                hasher,
                file: None,
                max_occupied_slots: (metadata.real_num_slots as f64 * 0.95) as u64,
            }),
        };
        Ok(cqf)
    }

    pub fn insert(&mut self, item: u64, count: u64) -> Result<(), CqfError> {
        let hash = self.calc_hash(item);
        // self.insert_hash(hash, count, 0)
        self.insert_by_hash(hash, count)
    }

    // Result<QueryResult, CqfError>
    pub fn query(&self, item: u64) -> HashCount {
        let hash = self.calc_hash(item);
        HashCount {
            hash,
            count: self.query_by_hash(hash),
        }
    }

    // New insert function
    // pub fn insert_hash(
    //     &mut self,
    //     hash: u64,
    //     mut count: u64,
    //     thread: usize,
    // ) -> Result<(), CqfError> {
    //     if count == 0 { return Ok(()); } // nothing to do
    //     if self.num_occupied_slots() >= self.max_occupied_slots() {
    //         return Err(CqfError::Filled);
    //     }
    //     let (quotient, remainder) = self.quotient_remainder_from_hash(hash);
    //     let run_start = self.blocks.run_end(quotient - 1) + 1;
    //     // Can insert where it belongs
    //     if !self.blocks.has_metadata_bits_set(quotient) && false {
    //         self.blocks.flip_occupied(quotient);
    //         self.blocks.set_slot(quotient, remainder);
    //         // If count is one, we have no need for more slots
    //         if count == 1 {
    //             self.blocks.flip_runend(quotient);
    //             return Ok(());
    //         }
    //         {
    //             // How many slots required to store count
    //             let count_slots_required = (count / Remainder::MAX) + 1;
    //             let mut adjacent_free_slots = 0;
    //             for i in 1..=count_slots_required {
    //                 if (quotient+i) < self.metadata.real_num_slots && !self.blocks.has_metadata_bits_set(quotient + i) {
    //                     adjacent_free_slots += 1;
    //                 } else {
    //                     break;
    //                 }
    //             }
    //             if count_slots_required == adjacent_free_slots {
    //                 // We have enough adjacent free slots to store count
    //                 self.blocks.flip_runend(quotient + count_slots_required);
    //                 for i in 1..=count_slots_required {
    //                     let c = if i == count_slots_required {
    //                         count % Remainder::MAX
    //                     } else {
    //                         Remainder::MAX
    //                     };
    //                     self.blocks.set_slot(quotient + i, c);
    //                 }
    //                 return Ok(());
    //             }
    //         }
    //         count -= 1;
    //     }

    //     Ok(())
    // }

    fn block_slot_index_from_quotient(&self, quotient: u64) -> (usize, usize) {
        let block_index: usize = (quotient / SLOTS_PER_BLOCK as u64) as usize;
        let slot_index: usize = (quotient % SLOTS_PER_BLOCK as u64) as usize;
        (block_index as usize, slot_index as usize)
    }

    pub fn calc_hash(&self, item: u64) -> u64 {
        let mut hasher = self.runtimedata.hasher.build_hasher();
        hasher.write_u64(item);
        hasher.finish()
    }

    fn max_occupied_slots(&self) -> u64 {
        self.runtimedata.max_occupied_slots
    }

    fn num_occupied_slots(&self) -> u64 {
        self.metadata.num_occupied_slots.load(Ordering::Relaxed) as u64
    }

    fn invertable(&self) -> bool {
        self.metadata.invertable()
    }

    /// Returns total size in bytes of the filter
    pub fn total_size(&self) -> u64 {
        self.metadata.total_size_in_bytes
    }

    fn quotient_remainder_from_hash(&self, hash: u64) -> (u64, Remainder) {
        let quotient =
            (hash >> self.metadata.remainder_bits) & ((1 << self.metadata.quotient_bits) - 1);
        let mut remainder = hash & ((1 << self.metadata.remainder_bits) - 1);
        remainder &= 1 << self.metadata.remainder_bits;
        (quotient, remainder as Remainder)
        // match Remainder::try_from(remainder) {
        //     Ok(r) => (quotient, r),
        //     Err(_) => panic!("Invalid remainder"),
        // }
    }

    // pub fn print(&self) {
    //     // let mut run_index = 0;
    //     for i in (1<<self.metadata_blocks.metadata.logn_slots)/64..self.metadata_blocks.metadata.num_blocks {
    //         let block = self.get_block(i as usize);

    //         println!("Block {}, offset {}, occupied {}, runend {}, count {}", i, block.offset,
    //             block.occupieds.count_ones(), block.runends.count_ones(), block.counts.count_ones()
    //         );
    //         for j in 0..64 as usize {
    //             // if block.is_runend(j) && self.run_end((i * 64 + j as u64) as usize) >= (i * 64 + j as u64) as usize {
    //             //     run_index += 1;
    //             // }
    //             println!(
    //                 "Slot {} occupied: {} runend: {} count: {}, remainder: {}, run index {}",
    //                 j,
    //                 block.is_occupied(j),
    //                 block.is_runend(j),
    //                 block.is_count(j),
    //                 block.get_slot(j),
    //                 self.run_end((i * 64 + j as u64) as usize) % 64
    //             );
    //         }
    //         println!("");
    //     }
    // }

    // pub fn print_offsets(&self) {
    //     // let mut run_index = 0;
    //     for i in 0..self.metadata_blocks.metadata.num_blocks {
    //         let block = self.get_block(i as usize);

    //         println!("Block {}, offset {}", i, block.offset);
    //         // for j in 0..64 as usize {
    //         //     // if block.is_runend(j) && self.run_end((i * 64 + j as u64) as usize) >= (i * 64 + j as u64) as usize {
    //         //     //     run_index += 1;
    //         //     // }
    //         //     println!(
    //         //         "Slot {} occupied: {} runend: {} count: {}, remainder: {}, run index {}",
    //         //         j,
    //         //         block.is_occupied(j),
    //         //         block.is_runend(j),
    //         //         block.is_count(j),
    //         //         block.get_slot(j),
    //         //         self.run_end((i * 64 + j as u64) as usize) % 64
    //         //     );
    //         // }
    //         // println!("");
    //     }
    // }
}

impl<'a, Hasher: BuildHasher> CountingQuotientFilter<'a, Hasher> {
    pub fn insert_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError> {
        if count == 0 {
            return Ok(());
        } // nothing to do
        if self.num_occupied_slots() >= self.max_occupied_slots() {
            return Err(CqfError::Filled);
        }
        let (quotient, remainder) = self.quotient_remainder_from_hash(hash);
        let runend_index = self.blocks.run_end(quotient);
        if !self.blocks.has_metadata_bits_set(quotient) && runend_index == quotient {
            self.blocks.set_runend(quotient, true);
            self.blocks.set_slot(quotient, remainder);
            self.blocks.set_occupied(quotient, true);
            self.metadata
                .num_occupied_slots
                .fetch_add(1, Ordering::SeqCst);
            if count > 1 {
                self.insert_by_hash(hash, count - 1)?;
            }
        } else {
            let mut runstart_index = if quotient == 0 {
                0
            } else {
                self.blocks.run_end(quotient - 1) + 1
            };
            if !self.blocks.is_occupied(quotient) {
                self.insert_and_shift(0, quotient, remainder, count, runstart_index, 0);
            } else {
                let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
                let mut current_end: u64;
                current_end = self.blocks.decode_counter(
                    runstart_index,
                    &mut current_remainder,
                    &mut current_count,
                );
                while current_remainder < remainder && !self.blocks.is_runend(current_end as u64) {
                    runstart_index = (current_end + 1) as u64;
                    current_end = self.blocks.decode_counter(
                        runstart_index,
                        &mut current_remainder,
                        &mut current_count,
                    )
                }

                if current_remainder < remainder {
                    self.insert_and_shift(1, quotient, remainder, count, current_end + 1, 0);
                } else if current_remainder == remainder {
                    self.insert_and_shift(
                        if self.blocks.is_runend(current_end as u64) {
                            1
                        } else {
                            2
                        },
                        quotient,
                        remainder,
                        current_count + count,
                        runstart_index,
                        current_end - runstart_index as u64 + 1,
                    );
                } else {
                    self.insert_and_shift(2, quotient, remainder, count, runstart_index, 0);
                }
            }
            self.blocks.set_occupied(quotient, true);
        }
        Ok(())
    }

    pub fn build_hash(&self, quotient: u64, remainder: u64) -> u64 {
        ((quotient as u64) << self.metadata.remainder_bits) | remainder
    }

    fn find_first_empty_slot(&self, mut from: u64) -> u64 {
        loop {
            let t = self.blocks.offset_lower_bound(from);
            if t == 0 {
                break;
            }
            from += t;
        }
        return from;
    }

    fn shift_remainders(&mut self, insert_index: u64, empty_slot_index: u64, distance: u64) {
        for i in (insert_index..=empty_slot_index).rev() {
            self.blocks
                .set_slot((i + distance) as u64, self.blocks.get_slot(i as u64));
        }
    }

    fn shift_runends(&mut self, insert_index: u64, empty_slot_index: u64, distance: u64) {
        for i in (insert_index..=empty_slot_index).rev() {
            self.blocks
                .set_runend(i + distance, self.blocks.is_runend(i));
        }
    }

    fn shift_counts(&mut self, insert_index: u64, empty_slot_index: u64, distance: u64) {
        for i in (insert_index..=empty_slot_index).rev() {
            self.blocks.set_count(i + distance, self.blocks.is_count(i));
        }
    }

    fn insert_and_shift(
        &mut self,
        operation: u64,
        quotient: u64,
        remainder: u64,
        count: u64,
        insert_index: u64,
        noverwrites: u64,
    ) {
        let ninserts = if count == 1 { 1 } else { 2 } - noverwrites;
        if ninserts > 0 {
            match ninserts {
                1 => {
                    let empty = self.find_first_empty_slot(insert_index);
                    self.shift_remainders(insert_index, empty - 1, 1);
                    self.shift_runends(insert_index, empty - 1, 1);
                    self.shift_counts(insert_index, empty - 1, 1);
                    for i in (((quotient / 64) + 1)..).take_while(|i| *i <= empty / 64) {
                        if empty / 64 < i {
                            break;
                        }
                        self.blocks
                            .set_offset(quotient, self.blocks.offset(quotient) + 1);
                    }
                }
                2 => {
                    let first = self.find_first_empty_slot(insert_index);
                    let second = self.find_first_empty_slot(first + 1);
                    self.shift_remainders(first + 1, second - 1, 1);
                    self.shift_runends(first + 1, second - 1, 1);
                    self.shift_counts(first + 1, second - 1, 1);
                    self.shift_remainders(insert_index, first - 1, 2);
                    self.shift_runends(insert_index, first - 1, 2);
                    self.shift_counts(insert_index, first - 1, 2);

                    let mut npreceding_empties = 0;
                    for i in (((quotient / 64) + 1)..).take_while(|i| *i <= second / 64) {
                        if npreceding_empties == 0 && first / 64 < i {
                            npreceding_empties += 1;
                        }
                        if npreceding_empties == 1 && second / 64 < i {
                            break;
                        }
                        self.blocks.set_offset(
                            quotient,
                            self.blocks.offset(quotient) + ((ninserts - npreceding_empties) as u16),
                        );
                    }
                }
                _ => panic!("unexpected number of inserts!"),
            }

            match operation {
                0 => {
                    if count == 1 {
                        self.blocks.set_runend(insert_index, true);
                    } else {
                        self.blocks.set_runend(insert_index, false);
                        self.blocks.set_runend(insert_index + 1, true);
                    }
                }
                1 => {
                    if noverwrites == 0 {
                        self.blocks.set_runend(insert_index - 1, false);
                    }
                    if count == 1 {
                        self.blocks.set_runend(insert_index, true);
                    } else {
                        self.blocks.set_runend(insert_index, false);
                        self.blocks.set_runend(insert_index + 1, true);
                    }
                }
                2 => {
                    if count == 1 {
                        self.blocks.set_runend(insert_index, false);
                    } else {
                        self.blocks.set_runend(insert_index, false);
                        self.blocks.set_runend(insert_index + 1, false);
                    }
                }
                _ => panic!("invalid operation!"),
            }
        }

        self.blocks.set_slot(insert_index, remainder);
        if count != 1 {
            // if the count isn't one, put a count in the next slot
            self.blocks.set_count(insert_index + 1, true);
            self.blocks.set_slot(insert_index + 1, count);
        }
        self.metadata
            .num_occupied_slots
            .fetch_add(ninserts, Ordering::SeqCst);
    }

    pub fn query_by_hash(&self, hash: u64) -> u64 {
        let (quotient, remainder) = self.quotient_remainder_from_hash(hash);
        if !self.blocks.is_occupied(quotient) {
            return 0;
        }
        let mut runstart_index = if quotient == 0 {
            0
        } else {
            self.blocks.run_end(quotient - 1) + 1
        };
        if runstart_index < quotient {
            runstart_index = quotient;
        }
        let mut current_end: u64;
        let mut current_remainder: u64 = 0;
        let mut current_count: u64 = 0;
        loop {
            current_end = self.blocks.decode_counter(
                runstart_index,
                &mut current_remainder,
                &mut current_count,
            );
            if current_remainder == remainder {
                return current_count;
            }
            if self.blocks.is_runend(current_end) {
                break;
            }
            runstart_index = current_end + 1;
        }
        return 0;
    }

    pub fn set_count(&mut self, item: u64, count: u64) -> Result<(), CqfError> {
        if self.num_occupied_slots() >= self.max_occupied_slots() as u64 {
            return Err(CqfError::Filled);
        }
        let hash = self.calc_hash(item);
        match self.set_count_by_hash(hash, count) {
            Ok(_) => Ok(()),
            Err(_) => self.insert_by_hash(hash, count),
        }
    }

    pub fn set_count_by_hash(&mut self, hash: u64, count: u64) -> Result<(), ()> {
        let (quotient, remainder) = self.quotient_remainder_from_hash(hash);
        // let runend_index = self.run_end(quotient);
        let mut runstart_index = self.blocks.run_end(quotient - 1) + 1;
        let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
        let mut current_end: u64;
        current_end =
            self.blocks
                .decode_counter(runstart_index, &mut current_remainder, &mut current_count);
        while current_remainder < remainder && !self.blocks.is_runend(current_end) {
            runstart_index = current_end + 1;
            current_end = self.blocks.decode_counter(
                runstart_index,
                &mut current_remainder,
                &mut current_count,
            );
        }
        // println!("setting");
        if current_remainder == remainder {
            if self.blocks.is_count(runstart_index + 1) {
                self.blocks.set_slot(runstart_index + 1, count);
                return Ok(());
            }
            self.insert_and_shift(
                if self.blocks.is_runend(current_end) {
                    1
                } else {
                    2
                },
                quotient,
                remainder,
                count,
                runstart_index,
                current_end - runstart_index + 1,
            );
        } else {
            return Err(()); // error since we didn't find the remainder
        };

        Ok(())
    }
}

impl<'a, 'b, Hasher: BuildHasher + Default + Clone> CountingQuotientFilter<'a, Hasher> {

    /// Merges a and b into a in memory cqf
    pub fn merge(a: &Self, b: &Self) -> Result<CountingQuotientFilter<'a, Hasher>, CqfError> {
        let (larger, smaller) = if a.metadata.num_occupied_slots.load(Ordering::Relaxed)
            > b.metadata.num_occupied_slots.load(Ordering::Relaxed)
        {
            (a, b)
        } else {
            (b, a)
        };
        let mut new_cqf: CountingQuotientFilter<'a, Hasher>;
        if larger.metadata.num_occupied_slots.load(Ordering::Relaxed) as u64
            + smaller.metadata.num_occupied_slots.load(Ordering::Relaxed) as u64
            > larger.max_occupied_slots()
        {
            new_cqf = CountingQuotientFilter::new(
                larger.metadata.logn_slots + 1,
                larger.metadata.logn_slots + 1,
                larger.metadata.quotient_bits + larger.metadata.remainder_bits,
                larger.metadata.invertable(),
                larger.runtimedata.hasher.clone(),
            )?;
        } else {
            new_cqf = CountingQuotientFilter::new(
                larger.metadata.logn_slots,
                larger.metadata.quotient_bits,
                larger.metadata.remainder_bits,
                larger.metadata.invertable(),
                larger.runtimedata.hasher.clone(),
            )?;
        }

        Self::merge_into(a, b, &mut new_cqf);
        // not sure if this works
        return Ok(new_cqf);

        Err(CqfError::FileError)
    }

    fn merge_into(a: &Self, b: &Self, new_cqf: &mut Self) {
        let mut iter_a = a.into_iter();
        let mut iter_b = b.into_iter();
        // let mut current_a = iter_a.next();
        // let mut current_b = iter_b.next();
        let mut next_a = iter_a.next();
        let mut next_b = iter_b.next();
        let mut a_val: HashCount;
        let mut b_val: HashCount;
        let mut merged_current_quotient = 0u64;
        if next_a.is_some() && next_b.is_some() {
            a_val = next_a.unwrap();
            b_val = next_b.unwrap();
            next_a = iter_a.next();
            next_b = iter_b.next();

            
            loop {
                let (a_quotient, a_remainder) = new_cqf.quotient_remainder_from_hash(a_val.hash);
                let (b_quotient, b_remainder) = new_cqf.quotient_remainder_from_hash(b_val.hash);
                // bring merged quotient index up to the quotient that we're inserting
                if merged_current_quotient < min(a_quotient, b_quotient) {
                    merged_current_quotient = min(a_quotient, b_quotient);
                }
                if a_quotient == b_quotient {
                    new_cqf.blocks.set_occupied(a_quotient, true);
                    if a_remainder == b_remainder {
                        let count = a_val.count + b_val.count;
                        new_cqf.blocks.set_count(a_quotient+1, true);
                        new_cqf.blocks.set_slot(a_quotient, a_remainder);
                        new_cqf.blocks.set_slot(a_quotient+1, count);
                        new_cqf.metadata.num_occupied_slots.fetch_add(2, Ordering::Relaxed);
                        merged_current_quotient += 2;
                        // check runend
                        let inserted_quotient = a_quotient;
                        // current_a = iter_a.next();
                        // current_b = iter_b.next();
                        if next_a.is_none() && next_b.is_none() {
                            new_cqf.blocks.set_runend(merged_current_quotient-1, true);
                            break;
                        } else if next_a.is_none() {
                            b_val = next_b.unwrap();
                            next_b = iter_b.next();
                            let next_quotient = new_cqf.quotient_remainder_from_hash(b_val.hash).0;
                            if next_quotient != inserted_quotient {
                                new_cqf.blocks.set_runend(merged_current_quotient-1, true);
                            }
                            continue;
                        } else if next_b.is_none() {
                            a_val = next_a.unwrap();
                            next_a = iter_a.next();
                            let next_quotient = new_cqf.quotient_remainder_from_hash(a_val.hash).0;
                            if next_quotient != inserted_quotient {
                                new_cqf.blocks.set_runend(merged_current_quotient-1, true);
                            }
                            continue;
                        } else {
                            a_val = next_a.unwrap();
                            b_val = next_b.unwrap();
                            next_a = iter_a.next();
                            next_b = iter_b.next();
                            let next_quotient_a = new_cqf.quotient_remainder_from_hash(a_val.hash).0;
                            let next_quotient_b = new_cqf.quotient_remainder_from_hash(b_val.hash).0;
                            if inserted_quotient != min(next_quotient_a, next_quotient_b) {
                                new_cqf.blocks.set_runend(merged_current_quotient-1, true);
                            }
                            continue;
                        }
                        // continue;
                    } else if a_remainder < b_remainder {
                        new_cqf.blocks.set_slot(merged_current_quotient, a_remainder);
                        if a_val.count != 1 {
                            new_cqf.blocks.set_count(merged_current_quotient+1, true);
                            new_cqf.blocks.set_slot(merged_current_quotient+1, a_val.count);
                            merged_current_quotient += 2;
                            new_cqf.metadata.num_occupied_slots.fetch_add(2, Ordering::Relaxed);
                        } else {
                            merged_current_quotient += 1;
                            new_cqf.metadata.num_occupied_slots.fetch_add(1, Ordering::Relaxed);
                        }
                        // check runend - not need because we know one quotient is equal
                        if next_a.is_none() {
                            break;
                        }
                        a_val = next_a.unwrap();
                        next_a = iter_a.next();
                    } else { // b_remainder < a_remainder
                        new_cqf.blocks.set_slot(merged_current_quotient, b_remainder);
                        if b_val.count != 1 {
                            new_cqf.blocks.set_count(merged_current_quotient+1, true);
                            new_cqf.blocks.set_slot(merged_current_quotient+1, b_val.count);
                            merged_current_quotient += 2;
                            new_cqf.metadata.num_occupied_slots.fetch_add(2, Ordering::Relaxed);
                        } else {
                            merged_current_quotient += 1;
                            new_cqf.metadata.num_occupied_slots.fetch_add(1, Ordering::Relaxed);
                        }
                        // check runend - not need because we know one quotient is equal
                        if next_b.is_none() {
                            break;
                        }
                        b_val = next_b.unwrap();
                        next_b = iter_b.next();
                    }   
                } else if a_quotient < b_quotient {
                    // can maybe turn this into while loop to improve speed
                    new_cqf.blocks.set_occupied(a_quotient, true);
                    new_cqf.blocks.set_slot(merged_current_quotient, a_remainder);
                    if a_val.count != 1 {
                        new_cqf.blocks.set_count(merged_current_quotient+1, true);
                        new_cqf.blocks.set_slot(merged_current_quotient+1, a_val.count);
                        merged_current_quotient += 2;
                        new_cqf.metadata.num_occupied_slots.fetch_add(2, Ordering::Relaxed);
                    } else {
                        merged_current_quotient += 1;
                        new_cqf.metadata.num_occupied_slots.fetch_add(1, Ordering::Relaxed);
                    }
                    // check runend
                    let inserted_quotient = a_quotient;
                    if next_a.is_none() {
                        break;
                    }
                    a_val = next_a.unwrap();
                    next_a = iter_a.next();

                    let next_quotient = new_cqf.quotient_remainder_from_hash(a_val.hash).0;
                    if next_quotient != inserted_quotient {
                        new_cqf.blocks.set_runend(merged_current_quotient-1, true);
                    }
                } else { // b_quotient < a_quotient
                    new_cqf.blocks.set_occupied(b_quotient, true);
                    new_cqf.blocks.set_slot(merged_current_quotient, b_remainder);
                    if b_val.count != 1 {
                        new_cqf.blocks.set_count(merged_current_quotient+1, true);
                        new_cqf.blocks.set_slot(merged_current_quotient+1, b_val.count);
                        merged_current_quotient += 2;
                        new_cqf.metadata.num_occupied_slots.fetch_add(2, Ordering::Relaxed);
                    } else {
                        merged_current_quotient += 1;
                        new_cqf.metadata.num_occupied_slots.fetch_add(1, Ordering::Relaxed);
                    }
                    // check runend
                    let inserted_quotient = b_quotient;
                    if next_b.is_none() {
                        break;
                    }
                    b_val = next_b.unwrap();
                    next_b = iter_b.next();
                    let next_quotient = new_cqf.quotient_remainder_from_hash(b_val.hash).0;
                    if next_quotient != inserted_quotient {
                        new_cqf.blocks.set_runend(merged_current_quotient-1, true);
                    }
                }
            }
        } else if next_b.is_none() {
            a_val = next_a.unwrap();
            b_val = HashCount {hash: 0, count: 0};
            next_a = iter_a.next();
        } else if next_a.is_none() {
            b_val = next_b.unwrap();
            a_val = HashCount {hash: 0, count: 0};
            next_b = iter_b.next();
        } else {
            // both empty
            return;          
        }

        while next_a.is_none() && next_b.is_some() {
            let (b_quotient, b_remainder) = new_cqf.quotient_remainder_from_hash(b_val.hash);
            new_cqf.blocks.set_occupied(b_quotient, true);
            new_cqf.blocks.set_slot(merged_current_quotient, b_remainder);
            if b_val.count != 1 {
                new_cqf.blocks.set_count(merged_current_quotient+1, true);
                new_cqf.blocks.set_slot(merged_current_quotient+1, b_val.count);
                merged_current_quotient += 2;
                new_cqf.metadata.num_occupied_slots.fetch_add(2, Ordering::Relaxed);
            } else {
                merged_current_quotient += 1;
                new_cqf.metadata.num_occupied_slots.fetch_add(1, Ordering::Relaxed);
            }
            // check runend
            let inserted_quotient = b_quotient;
            if next_b.is_none() {
                break;
            }
            b_val = next_b.unwrap();
            next_b = iter_b.next();
            let next_quotient = new_cqf.quotient_remainder_from_hash(b_val.hash).0;
            if next_quotient != inserted_quotient {
                new_cqf.blocks.set_runend(merged_current_quotient-1, true);
            }
        }

        while next_a.is_some() && next_b.is_none() {
            let (a_quotient, a_remainder) = new_cqf.quotient_remainder_from_hash(a_val.hash);
            new_cqf.blocks.set_occupied(a_quotient, true);
            new_cqf.blocks.set_slot(merged_current_quotient, a_remainder);
            if a_val.count != 1 {
                new_cqf.blocks.set_count(merged_current_quotient+1, true);
                new_cqf.blocks.set_slot(merged_current_quotient+1, a_val.count);
                merged_current_quotient += 2;
                new_cqf.metadata.num_occupied_slots.fetch_add(2, Ordering::Relaxed);
            } else {
                merged_current_quotient += 1;
                new_cqf.metadata.num_occupied_slots.fetch_add(1, Ordering::Relaxed);
            }
            // check runend
            let inserted_quotient = a_quotient;
            if next_a.is_none() {
                break;
            }
            a_val = next_a.unwrap();
            next_a = iter_a.next();
            let next_quotient = new_cqf.quotient_remainder_from_hash(a_val.hash).0;
            if next_quotient != inserted_quotient {
                new_cqf.blocks.set_runend(merged_current_quotient-1, true);
            }
        }
    }
}

impl<'a, Hasher: BuildHasher> IntoIterator for &'a CountingQuotientFilter<'a, Hasher> {
    type Item = HashCount;
    type IntoIter = CQFIterator<'a, Hasher>;

    fn into_iter(self) -> Self::IntoIter {
        let mut position: u64 = 0;
        if !self.blocks.is_occupied(0) {
            let mut block_index: u64 = 0;
            let mut idx = bitselect(self.blocks[0].occupieds(), 0);
            if idx == 64 {
                while idx == 64 && block_index < (self.metadata.num_blocks - 1) {
                    block_index += 1;
                    idx = bitselect(self.blocks[block_index as usize].occupieds(), 0);
                }
            }
            position = block_index * 64 + idx;
        }

        CQFIterator {
            qf: self,
            position: if position == 0 {
                0
            } else {
                self.blocks.run_end(position - 1) + 1
            },
            end: self.metadata.real_num_slots,
            run: position,
            first: true,
        }
    }
}

// pub struct CQFIterator<'a> {
//     qf: &'a CountingQuotientFilter,
//     position: usize,
//     end: usize,
//     run: usize,
//     first: bool,
// }

impl<'a, Hasher: BuildHasher> CQFIterator<'a, Hasher> {
    fn move_position(&mut self) -> bool {
        if self.position >= self.qf.metadata.real_num_slots {
            return false;
        } else {
            let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
            self.position = self.qf.blocks.decode_counter(
                self.position,
                &mut current_remainder,
                &mut current_count,
            );
            if !self.qf.blocks.is_runend(self.position) {
                self.position += 1;
                if self.position >= self.qf.metadata.real_num_slots {
                    return false;
                }
                return true;
            } else {
                let mut block_idx = self.run / 64;
                let mut rank = bitrank(
                    self.qf.blocks[block_idx as usize].occupieds(),
                    self.run % 64,
                );
                let mut next_run = bitselect(self.qf.blocks[block_idx as usize].occupieds(), rank);

                if next_run == 64 {
                    rank = 0;
                    while next_run == 64 && block_idx < (self.qf.metadata.num_blocks - 1) {
                        block_idx += 1;
                        next_run = bitselect(self.qf.blocks[block_idx as usize].occupieds(), rank);
                    }
                }

                if block_idx == self.qf.metadata.num_blocks {
                    self.run = self.qf.metadata.real_num_slots;
                    self.position = self.qf.metadata.real_num_slots;
                    return false;
                }

                self.run = block_idx * 64 + next_run;
                self.position += 1;
                if self.position < self.run {
                    self.position = self.run;
                }

                if self.position >= self.qf.metadata.real_num_slots {
                    return false;
                }

                return true;
            }
        }
    }
}

pub struct CQFIterator<'a, Hasher: BuildHasher> {
    qf: &'a CountingQuotientFilter<'a, Hasher>,
    position: u64,
    end: u64,
    run: u64,
    first: bool,
}

impl<'a, Hasher: BuildHasher> Iterator for CQFIterator<'a, Hasher> {
    type Item = HashCount;

    fn next(&mut self) -> Option<Self::Item> {
        if self.first {
            self.first = false;
            let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
            self.qf.blocks.decode_counter(
                self.position,
                &mut current_remainder,
                &mut current_count,
            );
            let hash = self.qf.build_hash(self.run, current_remainder);
            return Some(HashCount {
                hash,
                count: current_count,
            });
        }
        let can_move = self.move_position();
        if !can_move {
            return None;
        }
        if self.position >= self.end {
            println!("position: {}, end: {}", self.position, self.end);
            return None;
        }
        let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
        self.qf
            .blocks
            .decode_counter(self.position, &mut current_remainder, &mut current_count);
        let hash = self.qf.build_hash(self.run, current_remainder);
        Some(HashCount {
            hash,
            count: current_count,
        })
    }
}
