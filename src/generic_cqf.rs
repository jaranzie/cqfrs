type Remainder = u32;
// type Remainder = u64;
const SLOTS_PER_BLOCK: usize = 64;
use crate::{bitrank, bitselect, bitmask, bitselectv, popcntv};
pub struct HashCount {
    pub hash: u64,
    pub count: u64,
}

mod blocks {
    type Offset = u16;

    use super::Remainder;
    use super::SLOTS_PER_BLOCK;
    use crate::utils::*;
    use bitintr::{Pdep, Popcnt, Tzcnt};
    use std::mem::align_of;
    use std::ops::Deref;
    use std::ops::DerefMut;
    use std::ptr::Unique;

    #[repr(C)]
    pub struct BlockMetadata {
        // pub offset: u16,
        pub occupieds: u64,
        pub runends: u64
    }
    pub struct Block {
        counts: u64,
        remainders: [Remainder; SLOTS_PER_BLOCK],
    }

    pub struct Blocks {
        ptr_metadata: Unique<BlockMetadata>,
        ptr: Unique<Offset>,
        ptr_blocks: Unique<Block>,
        len: usize,
    }

    impl Deref for Blocks {
        type Target = [Block];

        fn deref(&self) -> &Self::Target {
            unsafe { std::slice::from_raw_parts(self.ptr_blocks.as_ptr(), self.len) }
        }
    }

    impl DerefMut for Blocks {
        fn deref_mut(&mut self) -> &mut [Block] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr_blocks.as_ptr(), self.len) }
        }
    }

    impl Blocks {
        // ptr is a pointer to the start of an mmaped region
        pub fn new(ptr: *mut u8, len: isize) -> Self {
            let ptr_metadata = ptr as *mut BlockMetadata;
            let mut ptr_blocks = unsafe { ptr_metadata.offset(len) as *mut Block };
            ptr_blocks = unsafe { 
                ptr_blocks.add(ptr_blocks.align_offset(align_of::<Block>()))
            };
            
            let ptr_blocks = unsafe { ptr_metadata.offset(len) as *mut Block };



            Self { ptr, len }
        }

        pub fn offsets(&self) -> &[Offset] {
            unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
        }

        pub fn offsets_mut(&mut self) -> &mut [Offset] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
        }

        pub fn blocks_metadata(&self) -> &[BlockMetadata] {
            unsafe { std::slice::from_raw_parts(self.ptr_metadata.as_ptr(), self.len) }
        }

        pub fn blocks_metadata_mut(&mut self) -> &mut [BlockMetadata] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr_metadata.as_ptr(), self.len) }
        }

        pub fn blocks(&self) -> &[Block] {
            unsafe { std::slice::from_raw_parts(self.ptr_blocks.as_ptr(), self.len) }
        }

        pub fn blocks_mut(&mut self) -> &mut [Block] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr_blocks.as_ptr(), self.len) }
        }

        pub fn offset(&self, quotient: u64) -> u16 {
            let (block_index, _) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].offset
        }

        pub fn set_offset(&mut self, quotient: u64, offset: u16) {
            let (block_index, _) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].offset = offset;
        }

        pub fn get_block(&self, block_index: usize) -> &Block {
            &self[block_index]
        }

        pub fn get_block_mut(&mut self, block_index: usize) -> &mut Block {
            &mut self[block_index]
        }

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

        pub fn offset_lower_bound(&self, quotient: u64) -> u64 {
            let (block_index, slot_index) = Self::block_slot_index_from_quotient(quotient);
            self[block_index].offset_lower_bound(slot_index as u64)
        }

        pub fn clear(&mut self) {
            for i in 0..self.len {
                self[i].clear();
            }
        }

        pub fn run_end(&self, quotient: u64) -> u64 {
            let block_idx: u64 = quotient / SLOTS_PER_BLOCK as u64;
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
use libc::{
    c_void, madvise, mmap, munmap, MADV_RANDOM, MADV_SEQUENTIAL, MAP_ANONYMOUS, MAP_FAILED,
    MAP_HUGETLB, MAP_PRIVATE, MAP_SHARED, PROT_READ, PROT_WRITE,
};
use parking_lot::Mutex;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::hash::{self, BuildHasher, Hasher};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull, Unique};
use std::{
    fs::File,
    sync::atomic::Ordering,
};

pub struct CountingQuotientFilter<Hasher: BuildHasher> {
    runtimedata: Box<RuntimeData<Hasher>>,
    metadata: std::mem::ManuallyDrop<Box<Metadata>>,
    blocks: Blocks,
}

/// lognslots should be atleast as big as quotient_bits, probably equal is best
impl<Hasher: BuildHasher> CountingQuotientFilter<Hasher> {
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

    fn make_metadata_blocks(
        lognslots: u64,
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        file: Option<&Path>,
        new: bool,
    ) -> Result<(ManuallyDrop<Box<Metadata>>, Blocks), CqfError> {
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
        let mmap_flags;
        let fd: i32;
        let prot_flags = PROT_READ | PROT_WRITE;
        let mut f: File;
        match file {
            Some(fpath) => {
                mmap_flags = MAP_SHARED;
                if new {
                    f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create_new(true)
                        .open(fpath)
                        .map_err(|_| CqfError::FileError)?;
                    f.set_len(init_metadata.total_size_in_bytes)
                        .map_err(|_| CqfError::FileError)?;
                    fd = f.as_raw_fd();
                } else {
                    f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(fpath)
                        .map_err(|_| CqfError::FileError)?;
                    fd = f.as_raw_fd();
                }
            }
            None => {
                mmap_flags = MAP_ANONYMOUS | MAP_PRIVATE;
                fd = -1;
            }
        };
        println!("fd: {}", fd);
        println!("total_size_in_bytes: {}", init_metadata.total_size_in_bytes);
        let buffer = unsafe {
            mmap(
                ptr::null_mut(),
                init_metadata.total_size_in_bytes as usize,
                prot_flags,
                mmap_flags,
                fd,
                0,
            )
        };
        if buffer == MAP_FAILED {
            println!("buffer: {:p}", buffer);
            return Err(CqfError::MmapError);
        }
        let metadata = unsafe { (buffer as *mut Metadata) };
        let mut metadata = std::mem::ManuallyDrop::new(unsafe { Box::from_raw(metadata) });
        if new {
            **metadata = init_metadata;
        }
        let blocks_ptr =
            unsafe { buffer.offset(std::mem::size_of::<Metadata>() as isize) as *mut Block };
        let blocks = Blocks::new(
            Unique::new(blocks_ptr).unwrap(),
            metadata.num_blocks as usize,
        );
        Ok((metadata, blocks))
    }

    pub fn new(
        lognslots: u64,
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        hasher: Hasher,
    ) -> Result<Self, CqfError> {
        let (metadata, blocks) = Self::make_metadata_blocks(
            lognslots,
            quotient_bits,
            hash_bits,
            invertable,
            None,
            true,
        )?;
        let real_num_slots = metadata.real_num_slots;
        let cqf = CountingQuotientFilter {
            blocks,
            metadata,
            runtimedata: Box::new(RuntimeData {
                in_memory: true,
                hasher,
                file: None,
                max_occupied_slots: (real_num_slots as f64 * 0.95) as u64,
            }),
        };
        Ok(cqf)
    }

    pub fn new_file(
        lognslots: u64,
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        hasher: Hasher,
        file: PathBuf,
    ) -> Result<Self, CqfError> {
        if file.exists() {
            return Err(CqfError::FileError);
        }
        let (metadata, blocks) = Self::make_metadata_blocks(
            lognslots,
            quotient_bits,
            hash_bits,
            invertable,
            Some(&file),
            true,
        )?;
        let real_num_slots = metadata.real_num_slots;
        let cqf = CountingQuotientFilter {
            blocks,
            metadata,
            runtimedata: Box::new(RuntimeData {
                in_memory: true,
                hasher,
                file: Some(file),
                max_occupied_slots: (real_num_slots as f64 * 0.95) as u64,
            }),
        };
        Ok(cqf)
    }

    pub fn open_file(hasher: Hasher, file: PathBuf) -> Result<Self, CqfError> {
        if !file.exists() {
            return Err(CqfError::FileError);
        }
        let (metadata, blocks) = Self::make_metadata_blocks(0, 0, 0, false, Some(&file), false)?;
        let real_num_slots = metadata.real_num_slots;
        let cqf = CountingQuotientFilter {
            blocks,
            metadata,
            runtimedata: Box::new(RuntimeData {
                in_memory: true,
                hasher,
                file: Some(file),
                max_occupied_slots: (real_num_slots as f64 * 0.95) as u64,
            }),
        };
        Ok(cqf)
    }

    pub fn advise_random(&self) {
        let metadata_pointer = &**self.metadata;
        unsafe {
            madvise(
                metadata_pointer as *const Metadata as *mut c_void,
                self.metadata.total_size_in_bytes as usize,
                MADV_RANDOM,
            )
        };
    }

    pub fn advise_seq(&self) {
        let metadata_pointer = &**self.metadata;
        unsafe {
            madvise(
                metadata_pointer as *const Metadata as *mut c_void,
                self.metadata.total_size_in_bytes as usize,
                MADV_SEQUENTIAL,
            )
        };
    }

    pub fn insert(&mut self, item: u64, count: u64) -> Result<(), CqfError> {
        // println!("insert {item} {count}");
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

    pub fn max_occupied_slots(&self) -> u64 {
        self.runtimedata.max_occupied_slots
    }

    pub fn num_occupied_slots(&self) -> u64 {
        self.metadata.num_occupied_slots.load(Ordering::Relaxed) as u64
    }

    fn invertable(&self) -> bool {
        self.metadata.invertable()
    }

    /// Returns total size in bytes of the filter
    pub fn total_size(&self) -> u64 {
        self.metadata.total_size_in_bytes
    }

    pub fn quotient_remainder_from_hash(&self, hash: u64) -> (u64, Remainder) {
        let quotient =
            (hash >> self.metadata.remainder_bits) & ((1 << self.metadata.quotient_bits) - 1);
        let mut remainder = hash & ((1 << self.metadata.remainder_bits) - 1);
        // remainder &= 1 << self.metadata.remainder_bits;
        (quotient, remainder as Remainder)
        // match Remainder::try_from(remainder) {
        //     Ok(r) => (quotient, r),
        //     Err(_) => panic!("Invalid remainder"),
        // }
    }

    pub fn print(&self) {
        // let mut run_index = 0;
        for i in 0..self.metadata.num_blocks {
            let block = &self.blocks[i as usize];

            println!(
                "Block {}, offset {}, occupied {}, runend {}, count {}",
                i,
                block.offset(),
                block.occupieds().count_ones(),
                block.runends().count_ones(),
                block.counts().count_ones()
            );
            for j in 0..64 as usize {
                // if block.is_runend(j) && self.run_end((i * 64 + j as u64) as usize) >= (i * 64 + j as u64) as usize {
                //     run_index += 1;
                // }
                println!(
                    "Slot {} occupied: {} runend: {} count: {}, remainder: {}, run index ",
                    j,
                    block.is_occupied(j),
                    block.is_runend(j),
                    block.is_count(j),
                    block.get_slot(j),
                    // self.blocks.run_end((i * 64 + j as u64)) % 64
                );
            }
            println!("");
        }
    }

    pub fn print_offsets(&self) {
        // let mut run_index = 0;
        for i in 0..self.metadata.num_blocks {
            // let block = self.blocks[i as usize];

            println!("Block {}, offset {}", i, self.blocks.offset(i * 64));
            // for j in 0..64 as usize {
            //     // if block.is_runend(j) && self.run_end((i * 64 + j as u64) as usize) >= (i * 64 + j as u64) as usize {
            //     //     run_index += 1;
            //     // }
            //     println!(
            //         "Slot {} occupied: {} runend: {} count: {}, remainder: {}, run index {}",
            //         j,
            //         block.is_occupied(j),
            //         block.is_runend(j),
            //         block.is_count(j),
            //         block.get_slot(j),
            //         self.run_end((i * 64 + j as u64) as usize) % 64
            //     );
            // }
            // println!("");
        }
    }
}

impl<Hasher: BuildHasher> CountingQuotientFilter<Hasher> {
    pub fn insert_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError> {
        // println!("insert_by_hash {hash} {count}");
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
            // println!("offset lower bound {}", t);
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
    // offset_lower
    fn insert_and_shift(
        &mut self,
        operation: u64,
        quotient: u64,
        remainder: u64,
        count: u64,
        insert_index: u64,
        noverwrites: u64,
    ) {
        // println!("insert_and_shift {operation} {quotient} {remainder} {count} {insert_index} {noverwrites}");
        // if quotient == 2352 {
        //     println!("quotient idx {}", quotient);
        // }
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
                        // println!("setting offset for block");
                        self.blocks
                            .set_offset(i * 64, self.blocks.offset(i * 64) + 1);
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
                            i * 64,
                            self.blocks.offset(i * 64) + ((ninserts - npreceding_empties) as u16),
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

impl<Hasher: BuildHasher + Default + Clone> CountingQuotientFilter<Hasher> {
    /// Merges a and b into a in memory cqf
    pub fn merge(a: &Self, b: &Self) -> Result<CountingQuotientFilter<Hasher>, CqfError> {
        let (larger, smaller) = if a.max_occupied_slots() > b.max_occupied_slots() {
            (a, b)
        } else {
            (b, a)
        };
        let mut new_cqf: CountingQuotientFilter<Hasher>;
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
                larger.metadata.quotient_bits + larger.metadata.remainder_bits,
                larger.metadata.invertable(),
                larger.runtimedata.hasher.clone(),
            )?;
        }

        Self::merge_into(a, b, &mut new_cqf);
        // not sure if this works
        return Ok(new_cqf);
        Err(CqfError::FileError)
    }

    pub fn merge_file(
        a: &Self,
        b: &Self,
        path: PathBuf,
    ) -> Result<CountingQuotientFilter<Hasher>, CqfError> {
        if path.exists() {
            std::fs::remove_file(&path).map_err(|_| CqfError::FileError)?;
        }
        let (larger, smaller) = if a.max_occupied_slots() > b.max_occupied_slots() {
            (a, b)
        } else {
            (b, a)
        };
        let mut new_cqf: CountingQuotientFilter<Hasher>;
        if larger.metadata.num_occupied_slots.load(Ordering::Relaxed) as u64
            + smaller.metadata.num_occupied_slots.load(Ordering::Relaxed) as u64
            > larger.max_occupied_slots()
        {
            new_cqf = CountingQuotientFilter::new_file(
                larger.metadata.logn_slots + 1,
                larger.metadata.logn_slots + 1,
                larger.metadata.quotient_bits + larger.metadata.remainder_bits,
                larger.metadata.invertable(),
                larger.runtimedata.hasher.clone(),
                path,
            )?;
        } else {
            new_cqf = CountingQuotientFilter::new_file(
                larger.metadata.logn_slots,
                larger.metadata.quotient_bits,
                larger.metadata.quotient_bits + larger.metadata.remainder_bits,
                larger.metadata.invertable(),
                larger.runtimedata.hasher.clone(),
                path,
            )?;
        }

        Self::merge_into(a, b, &mut new_cqf);
        // not sure if this works
        return Ok(new_cqf);
        Err(CqfError::FileError)
    }

    // Returns current_quotient-1 if both are None
    fn next_quotient(
        &self,
        a: &Option<HashCount>,
        b: &Option<HashCount>,
        current_quotient: u64,
    ) -> u64 {
        match (a, b) {
            (Some(a_val), Some(b_val)) => {
                let a_quotient = self.quotient_remainder_from_hash(a_val.hash).0;
                let b_quotient = self.quotient_remainder_from_hash(b_val.hash).0;
                if a_quotient < b_quotient {
                    a_quotient
                } else {
                    b_quotient
                }
            }
            (Some(a_val), None) => self.quotient_remainder_from_hash(a_val.hash).0,
            (None, Some(b_val)) => self.quotient_remainder_from_hash(b_val.hash).0,
            (None, None) => current_quotient - 1,
        }
    }

    pub fn clear(&mut self) {
        self.blocks.clear();
        self.metadata.num_occupied_slots.store(0, Ordering::Relaxed);
    }

    pub fn resize(&mut self) -> Result<(), CqfError> {
        let mut new_cqf: CountingQuotientFilter<Hasher>;
        if self.runtimedata.file.is_some() {
            new_cqf = CountingQuotientFilter::new_file(
                self.metadata.logn_slots + 1,
                self.metadata.quotient_bits + 1,
                self.metadata.quotient_bits + self.metadata.remainder_bits,
                self.metadata.invertable(),
                self.runtimedata.hasher.clone(),
                self.runtimedata.file.as_ref().unwrap().clone(),
            )?;
        } else {
            new_cqf = CountingQuotientFilter::new(
                self.metadata.logn_slots + 1,
                self.metadata.quotient_bits + 1,
                self.metadata.quotient_bits + self.metadata.remainder_bits,
                self.metadata.invertable(),
                self.runtimedata.hasher.clone(),
            )?;
        }
        let mut merged_cqf_current_quotient = 0u64;
        let mut old_iter = self.into_iter();
        let mut current_old = old_iter.next();
        // finish inserts
        while current_old.is_some() {
            let insert_quotient: u64;
            let insert_remainder: u64;
            let insert_count: u64;
            let next_quotient: u64;
            {
                let (r_quotient, r_remainder);
                let r_count;
                {
                    let a_val = current_old.as_ref().unwrap();
                    (r_quotient, r_remainder) = new_cqf.quotient_remainder_from_hash(a_val.hash);
                    r_count = a_val.count;
                }
                insert_count = r_count;
                insert_quotient = r_quotient;
                insert_remainder = r_remainder;
                current_old = old_iter.next();
                next_quotient = new_cqf.next_quotient(&current_old, &None, insert_quotient);
            }
            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient,
                insert_remainder,
                insert_count,
            );
        }
        *self = new_cqf;
        Ok(())
    }

    fn merge_into(a: &Self, b: &Self, new_cqf: &mut Self) {
        let mut iter_a = a.into_iter();
        let mut iter_b = b.into_iter();

        let mut current_a = iter_a.next();
        let mut current_b = iter_b.next();

        let mut merged_cqf_current_quotient = 0u64;
        while current_a.is_some() && current_b.is_some() {
            let insert_quotient: u64;
            let insert_remainder: u64;
            let insert_count: u64;
            let next_quotient: u64;
            {
                let (a_quotient, a_remainder);
                let (b_quotient, b_remainder);
                let a_count;
                let b_count;
                {
                    let a_val = current_a.as_ref().unwrap();
                    let b_val = current_b.as_ref().unwrap();
                    (a_quotient, a_remainder) = new_cqf.quotient_remainder_from_hash(a_val.hash);
                    (b_quotient, b_remainder) = new_cqf.quotient_remainder_from_hash(b_val.hash);
                    a_count = a_val.count;
                    b_count = b_val.count;
                }
                if a_quotient == b_quotient {
                    insert_quotient = a_quotient;
                    if a_remainder == b_remainder {
                        insert_count = a_count + b_count;
                        insert_remainder = a_remainder;
                        current_a = iter_a.next();
                        current_b = iter_b.next();
                    } else if a_remainder < b_remainder {
                        insert_count = a_count;
                        insert_remainder = a_remainder;
                        current_a = iter_a.next();
                    } else {
                        insert_count = b_count;
                        insert_remainder = b_remainder;
                        current_b = iter_b.next();
                    }
                } else if a_quotient < b_quotient {
                    insert_count = a_count;
                    insert_quotient = a_quotient;
                    insert_remainder = a_remainder;
                    current_a = iter_a.next();
                    // current_b = Some(b_val);
                } else {
                    insert_count = b_count;
                    insert_quotient = b_quotient;
                    insert_remainder = b_remainder;
                    current_b = iter_b.next();
                }
                next_quotient = new_cqf.next_quotient(&current_a, &current_b, insert_quotient);
            }
            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient,
                insert_remainder,
                insert_count,
            );
        }
        let (mut current_remaining, mut remaining_iter) = if current_a.is_some() {
            (current_a, iter_a)
        } else {
            (current_b, iter_b)
        };
        // finish inserts
        while current_remaining.is_some() {
            let insert_quotient: u64;
            let insert_remainder: u64;
            let insert_count: u64;
            let next_quotient: u64;
            {
                let (r_quotient, r_remainder);
                let r_count;
                {
                    let a_val = current_remaining.as_ref().unwrap();
                    (r_quotient, r_remainder) = new_cqf.quotient_remainder_from_hash(a_val.hash);
                    r_count = a_val.count;
                }
                insert_count = r_count;
                insert_quotient = r_quotient;
                insert_remainder = r_remainder;
                current_remaining = remaining_iter.next();
                next_quotient = new_cqf.next_quotient(&current_remaining, &None, insert_quotient);
            }
            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient,
                insert_remainder,
                insert_count,
            );
        }
    }

    pub fn merge_insert(
        &mut self,
        current_quotient: &mut u64,
        new_quotient: u64,
        next_quotient: u64,
        new_remainder: Remainder,
        count: u64,
    ) {
        self.blocks.set_occupied(new_quotient, true);

        if *current_quotient < new_quotient {
            *current_quotient = new_quotient;
        }
        // else if *current_quotient > new_quotient {

        // }

        self.blocks.set_slot(*current_quotient, new_remainder);
        if count != 1 {
            self.blocks.set_count(*current_quotient + 1, true);
            self.blocks.set_slot(*current_quotient + 1, count);
            self.metadata
                .num_occupied_slots
                .fetch_add(2, Ordering::Relaxed);
            *current_quotient += 2;
        } else {
            self.metadata
                .num_occupied_slots
                .fetch_add(1, Ordering::Relaxed);
            *current_quotient += 1;
        }

        if next_quotient != new_quotient {
            self.blocks.set_runend(*current_quotient - 1, true);
        }

        let quotient_block_idx = new_quotient / SLOTS_PER_BLOCK as u64;
        let end_of_insert = *current_quotient - 1;
        // The block we're inserting into
        let insert_block_idx = (end_of_insert) / SLOTS_PER_BLOCK as u64;
        for i in (quotient_block_idx + 1)..insert_block_idx {
            // println!("setting offset for block {} eoi {}", i, end_of_insert % SLOTS_PER_BLOCK as u64);
            // new_cqf.blocks.set_offset(i * SLOTS_PER_BLOCK as u64, ((end_of_insert % SLOTS_PER_BLOCK as u64)+1) as u16);
            self.blocks
                .set_offset(i * SLOTS_PER_BLOCK as u64, (SLOTS_PER_BLOCK) as u16);
        }
        if quotient_block_idx + 1 <= insert_block_idx {
            self.blocks.set_offset(
                insert_block_idx * SLOTS_PER_BLOCK as u64,
                ((end_of_insert % SLOTS_PER_BLOCK as u64) + 1) as u16,
            );
        }
    }
}

impl<'a, Hasher: BuildHasher> IntoIterator for &'a CountingQuotientFilter<Hasher> {
    type Item = HashCount;
    type IntoIter = CQFIterator<'a, Hasher>;

    // fn into_iter(self) -> Self::IntoIter {
    //     let mut position: u64 = 0;
    //     if !self.blocks.is_occupied(0) {
    //         let mut block_index: u64 = 0;
    //         let mut idx = bitselect(self.blocks[0].occupieds(), 0);
    //         if idx == 64 {
    //             while idx == 64 && block_index < (self.metadata.num_blocks - 1) {
    //                 block_index += 1;
    //                 idx = bitselect(self.blocks[block_index as usize].occupieds(), 0);
    //             }
    //         }
    //         position = block_index * 64 + idx;
    //     }

    //     CQFIterator {
    //         qf: self,
    //         position: if position == 0 {
    //             0
    //         } else {
    //             self.blocks.run_end(position - 1) + 1
    //         },
    //         end: self.metadata.real_num_slots,
    //         run: position,
    //         first: true,
    //     }
    // }

    fn into_iter(self) -> Self::IntoIter {
        self.advise_seq();
        // println!("{}", self.metadata.num_occupied_slots.load(Ordering::Relaxed));

        let mut position = 0;
        if self.num_occupied_slots() == 0 {
            return CQFIterator {
                qf: self,
                position: 0,
                end: 0,
                run: 0,
                first: true,
                // id: 0,
            };
        } else if !self.blocks.is_occupied(0) {
            let mut block_index: usize = 0;
            // let mut idx = bitselect(self.get_block(0).occupieds, 0);
            let mut idx = bitselect(self.blocks[0].occupieds(), 0);
            if idx == 64 {
                while idx == 64 && block_index < (self.metadata.num_blocks - 1) as usize {
                    block_index += 1;
                    // idx = bitselect(self.get_block(block_index).occupieds, 0);
                    idx = bitselect(self.blocks[block_index].occupieds(), 0);
                }
            }
            position = block_index * 64 + idx as usize;
        }

        CQFIterator {
            qf: self,
            position: if position == 0 {
                0
            } else {
                self.blocks.run_end((position - 1) as u64) + 1
            },
            end: self.metadata.real_num_slots - 1,
            run: position as u64,
            first: true,
            // id: 0,
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
    // fn move_position(&mut self) -> bool {
    //     if self.position >= self.qf.metadata.real_num_slots {
    //         return false;
    //     } else {
    //         let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
    //         self.position = self.qf.blocks.decode_counter(
    //             self.position,
    //             &mut current_remainder,
    //             &mut current_count,
    //         );
    //         if !self.qf.blocks.is_runend(self.position) {
    //             self.position += 1;
    //             if self.position >= self.qf.metadata.real_num_slots {
    //                 return false;
    //             }
    //             return true;
    //         } else {
    //             let mut block_idx = self.run / 64;
    //             let mut rank = bitrank(
    //                 self.qf.blocks[block_idx as usize].occupieds(),
    //                 self.run % 64,
    //             );
    //             let mut next_run = bitselect(self.qf.blocks[block_idx as usize].occupieds(), rank);

    //             if next_run == 64 {
    //                 rank = 0;
    //                 while next_run == 64 && block_idx < (self.qf.metadata.num_blocks - 1) {
    //                     block_idx += 1;
    //                     next_run = bitselect(self.qf.blocks[block_idx as usize].occupieds(), rank);
    //                 }
    //             }

    //             if block_idx == self.qf.metadata.num_blocks {
    //                 self.run = self.qf.metadata.real_num_slots;
    //                 self.position = self.qf.metadata.real_num_slots;
    //                 return false;
    //             }

    //             self.run = block_idx * 64 + next_run;
    //             self.position += 1;
    //             if self.position < self.run {
    //                 self.position = self.run;
    //             }

    //             if self.position >= self.qf.metadata.real_num_slots {
    //                 return false;
    //             }

    //             return true;
    //         }
    //     }
    // }

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
                // let mut rank = bitrank(self.qf.get_block(block_idx).occupieds, self.run % 64);
                // let mut next_run = bitselect(self.qf.get_block(block_idx).occupieds, rank);
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

                // advise dont need old run
                // let old_run = self.run;

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
    qf: &'a CountingQuotientFilter<Hasher>,
    position: u64,
    end: u64,
    run: u64,
    first: bool,
}

impl<'a, Hasher: BuildHasher> Iterator for CQFIterator<'a, Hasher> {
    type Item = HashCount;

    // fn next(&mut self) -> Option<Self::Item> {
    //     if self.first {
    //         self.first = false;
    //         let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
    //         self.qf.blocks.decode_counter(
    //             self.position,
    //             &mut current_remainder,
    //             &mut current_count,
    //         );
    //         let hash = self.qf.build_hash(self.run, current_remainder);
    //         return Some(HashCount {
    //             hash,
    //             count: current_count,
    //         });
    //     }
    //     let can_move = self.move_position();
    //     if !can_move {
    //         return None;
    //     }
    //     if self.position >= self.end {
    //         println!("position: {}, end: {}", self.position, self.end);
    //         return None;
    //     }
    //     let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
    //     self.qf
    //         .blocks
    //         .decode_counter(self.position, &mut current_remainder, &mut current_count);
    //     let hash = self.qf.build_hash(self.run, current_remainder);
    //     Some(HashCount {
    //         hash,
    //         count: current_count,
    //     })
    // }
    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.end {
            return None;
        }
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
            // println!("position: {}, end: {} id: {}", self.position, self.end, self.id);
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

impl<Hasher: BuildHasher + Clone + Default> CountingQuotientFilter<Hasher> {
    fn check_cqf_merge_compatibility(a: &Self, b: &Self) -> bool {
        if a.metadata.quotient_bits != b.metadata.quotient_bits {
            return false;
        }
        if a.metadata.remainder_bits != b.metadata.remainder_bits {
            return false;
        }
        if a.metadata.invertable() != b.metadata.invertable() {
            return false;
        }
        return true;
    }
    /// Fn is (a quotient, aremainder, &mut a_count, b quotient, bremainder, &mut b_count) -> bool True if items should not be inserted
    pub fn merge_file_cb<T: CqfMergeCallback>(
        s: &T,
        a: &Self,
        b: &Self,
        path: PathBuf,
    ) -> Result<CountingQuotientFilter<Hasher>, CqfError> {
        if Self::check_cqf_merge_compatibility(a, b) {
            return Err(CqfError::InvalidArguments);
        } else if path.exists() {
            std::fs::remove_file(&path).map_err(|_| CqfError::FileError)?;
        }
        let total_occupied_slots = a.num_occupied_slots() + b.num_occupied_slots();
        let required_bits = total_occupied_slots.next_power_of_two().trailing_zeros() as u64 + 1;
        if required_bits == 0 {
            return Err(CqfError::InvalidArguments);
        }
        let mut new_cqf: Self = CountingQuotientFilter::new_file(
            required_bits,
            required_bits,
            a.metadata.quotient_bits + a.metadata.remainder_bits,
            a.metadata.invertable(),
            a.runtimedata.hasher.clone(),
            path,
        )?;
        Self::merge_into_cb(s, a, b, &mut new_cqf);
        return Ok(new_cqf);
    }

    /// Fn is (&mut newcqf, &mut next insert index, a quotient, aremainder, a_count, b quotient, bremainder, b_count, &mut) -> bool True if items should not be inserted
    fn merge_into_cb<T: CqfMergeCallback>(
        s: &T,
        a: &Self,
        b: &Self,
        new_cqf: &mut Self,
    ) {
        let mut iter_a = a.into_iter();
        let mut iter_b = b.into_iter();
        let mut current_a = iter_a.next();
        let mut current_b = iter_b.next();
        let mut merged_cqf_current_quotient = 0u64;
        while current_a.is_some() && current_b.is_some() {
            let insert_quotient: u64;
            let insert_remainder: u64;
            let insert_count: u64;
            let next_quotient: u64;
            {
                let (a_quotient, a_remainder);
                let (b_quotient, b_remainder);
                let mut a_count;
                let mut b_count;
                {
                    let a_val = current_a.as_ref().unwrap();
                    let b_val = current_b.as_ref().unwrap();
                    (a_quotient, a_remainder) = new_cqf.quotient_remainder_from_hash(a_val.hash);
                    (b_quotient, b_remainder) = new_cqf.quotient_remainder_from_hash(b_val.hash);
                    a_count = a_val.count;
                    b_count = b_val.count;
                }
                s.merge_cb(
                    new_cqf,
                    a_quotient,
                    a_remainder,
                    &mut a_count,
                    b_quotient,
                    b_remainder,
                    &mut b_count,
                );
                if a_quotient == b_quotient && a_remainder == b_remainder {
                    insert_count = a_count + b_count;
                    insert_quotient = a_quotient;
                    insert_remainder = a_remainder;
                    current_a = iter_a.next();
                    current_b = iter_b.next();
                } else if a_quotient < b_quotient || (a_quotient == b_quotient && a_remainder < b_remainder) {
                    insert_count = a_count;
                    insert_quotient = a_quotient;
                    insert_remainder = a_remainder;
                    current_a = iter_a.next();
                    // current_b = Some(b_val);
                } else {
                    insert_count = b_count;
                    insert_quotient = b_quotient;
                    insert_remainder = b_remainder;
                    current_b = iter_b.next();
                }
                next_quotient = new_cqf.next_quotient(&current_a, &current_b, insert_quotient);
            }
            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient,
                insert_remainder,
                insert_count,
            );
        }
        let is_a = current_a.is_some();
        let (mut current_remaining, mut remaining_iter) = if current_a.is_some() {
            (current_a, iter_a)
        } else {
            (current_b, iter_b)
        };
        // finish inserts
        while current_remaining.is_some() {
            let insert_quotient: u64;
            let insert_remainder: u64;
            let mut insert_count: u64;
            let next_quotient: u64;
            {
                let (r_quotient, r_remainder);
                let r_count;
                {
                    let a_val = current_remaining.as_ref().unwrap();
                    (r_quotient, r_remainder) = new_cqf.quotient_remainder_from_hash(a_val.hash);
                    r_count = a_val.count;
                }
                insert_count = r_count;
                insert_quotient = r_quotient;
                insert_remainder = r_remainder;
                current_remaining = remaining_iter.next();
                next_quotient = new_cqf.next_quotient(&current_remaining, &None, insert_quotient);
            }
            if is_a {
                s.merge_cb(
                    new_cqf,
                    insert_quotient,
                    insert_remainder,
                    &mut insert_count,
                    u64::MAX,
                    u64::MAX,
                    &mut 0,
                );
            }
            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient,
                insert_remainder,
                insert_count,
            );
        }
    }
}

pub trait CqfMergeCallback {
    fn merge_cb<T: BuildHasher>(
        &self,
        new_cqf: &mut CountingQuotientFilter<T>,
        a_quotient: u64,
        a_remainder: u64,
        a_count: &mut u64,
        b_quotient: u64,
        b_remainder: u64,
        b_count: &mut u64,
    );
}

impl<Hasher: BuildHasher> Drop for CountingQuotientFilter<Hasher> {
    fn drop(&mut self) {
        let metadata_pointer = &**self.metadata;
        unsafe {
            munmap(
                metadata_pointer as *const Metadata as *mut c_void,
                self.metadata.total_size_in_bytes as usize,
            )
        };
    }
}
