#![feature(sync_unsafe_cell)]
mod blocks;
mod partitioned_counter;

use blocks::{Block, Blocks};
use crossbeam::utils::CachePadded;
use parking_lot::Mutex;
// use partitioned_counter::PartitionedCounter;
use libc::{
    c_void, madvise, mmap, munmap, MADV_RANDOM, MAP_ANONYMOUS, MAP_FAILED, MAP_HUGETLB,
    MAP_PRIVATE, MAP_SHARED, PROT_READ, PROT_WRITE,
};
use rayon::iter::plumbing::{Consumer, UnindexedConsumer, UnindexedProducer};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::prelude::*;
use std::alloc::{self, Layout};
use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::{
    fs::File,
    sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering},
};
use xxhash_rust::xxh3::xxh3_64;

const QF_BLOCK_OFFSET_BITS: usize = 6;
const QF_SLOTS_PER_BLOCK: usize = 1usize << QF_BLOCK_OFFSET_BITS;
// const QF_METADATA_WORDS_PER_BLOCK: usize = ((QF_SLOTS_PER_BLOCK + 63) / 64) as usize;
// const THRESHOLD: i32 = 100;
// const NUM_COUNTERS: u32 = 1;
const CLUSTER_SIZE: usize = 1 << 14;
const NUM_SLOTS_TO_LOCK: u64 = 1 << 16;


const BITS_PER_SLOT: u64 = 64;
type BlockT = u64;
type Quotient = u64;
type Remainder = u64;
type SlotIndex = u64;
type BlockIndex = u64;

pub enum HashMode {
    Fast = 0,
    Invertible = 1,
    // QFHashNone = 2,
}

struct RuntimeData {
    file: Option<File>,
    auto_resize: bool,
    num_locks: u64,
    metadata_lock: Mutex<()>,
    locks: Vec<CachePadded<Mutex<()>>>,
    // pub pc_num_elements: PartitionedCounter,
    // pub pc_num_distinct_elements: PartitionedCounter,
    // pub pc_num_occupied_slots: PartitionedCounter,
}
unsafe impl Sync for RuntimeData {}
unsafe impl Send for RuntimeData {}

#[repr(C)]
struct Metadata {
    total_size_in_bytes: u64,
    logn_slots: u64,
    real_num_slots: u64,
    num_blocks: u64,
    quotient_bits: u64,
    remainder_bits: u64,
    num_occupied_slots: AtomicI64,
    hash_mode: u32,
    // Unsure if needed
    // pub bits_per_slot: u64,
    // pub num_elements: AtomicI64,
    // pub num_distinct_elts: AtomicI64,
}

pub struct CountingQuotientFilter {
    runtimedata: Box<RuntimeData>,
    metadata: *mut Metadata,
    blocks: *mut Blocks,
}
unsafe impl Sync for CountingQuotientFilter {}

fn valid_args(lognslots: u64, quotient_bits: u64) -> bool {
    if quotient_bits >= BITS_PER_SLOT || quotient_bits <= 0 || quotient_bits > lognslots {
        return false;
    }
    true
}

fn calulate_num_locks(num_blocks: u64) -> u64 {
    let num_locks = (num_blocks as f64 / NUM_SLOTS_TO_LOCK as f64).ceil() as u64;
    num_locks
}

fn create_locks(num_blocks: u64) -> Vec<CachePadded<Mutex<()>>> {
    let num_locks = (num_blocks as f64 / NUM_SLOTS_TO_LOCK as f64).ceil() as u64;
    let mut locks = Vec::with_capacity(num_locks as usize);
    for _ in 0..num_locks {
        locks.push(CachePadded::new(Mutex::new(())));
    }
    locks
}

pub enum CqfError {
    FileError,
    MmapError,
    InvalidArgs,
    InvalidFile,
    InvalidSize,
}

impl Metadata {
    fn new(lognslots: u64, quotient_bits: u64, hash_mode: HashMode) -> Self {
        let num_slots: u64 = 1 << lognslots;
        let real_num_slots: u64 = (num_slots as f64 + 10 as f64 * (num_slots as f64).sqrt()) as u64;
        let num_blocks =
            (real_num_slots + QF_SLOTS_PER_BLOCK as u64 - 1) / QF_SLOTS_PER_BLOCK as u64;
        let remainder_bits = BITS_PER_SLOT as u64 - quotient_bits;
        let total_size_blocks: u64 = num_blocks * std::mem::size_of::<Block>() as u64;
        let total_bytes: u64 = total_size_blocks + std::mem::size_of::<Metadata>() as u64;

        Self {
            total_size_in_bytes: total_bytes,
            logn_slots: lognslots,
            real_num_slots,
            num_blocks,
            quotient_bits,
            remainder_bits,
            num_occupied_slots: AtomicI64::new(0),
            hash_mode: hash_mode as u32,
            // metadata_blocks.metadata.num_elements = AtomicI64::new(0);
            // metadata_blocks.metadata.num_distinct_elts = AtomicI64::new(0);
        }
    }
}
// callback
// setbit
// merge_together
// clear
// contains_key
// capacity
// usuage
// try_insert
// serialize
// deserialize
// serialize_raw
// grow
// shrink
// insert inner, clear, insert with thread id ,
/// lognslots should be atleast as big as quotient_bits, probably equal is best
impl CountingQuotientFilter {
    pub fn new(lognslots: u64, quotient_bits: u64, hash_mode: HashMode) -> Result<Self, CqfError> {
        if !valid_args(lognslots, quotient_bits) {
            return Err(CqfError::InvalidArgs);
        }
        let init_metadata = Metadata::new(lognslots, quotient_bits, hash_mode);
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
        let blocks = 
            unsafe { &mut *(buffer.add(std::mem::size_of::<Metadata>()) as *mut Blocks) };
        *metadata = init_metadata;
        let mut locks: Vec<CachePadded<Mutex<()>>> = create_locks(metadata.num_blocks);

        // let nelts_ptr = &metadata_blocks.metadata.num_elements as *const AtomicI64;
        // let ndistinct_elts_ptr = &metadata_blocks.metadata.num_distinct_elts as *const AtomicI64;
        let noccupied_slots_ptr = &metadata.num_occupied_slots as *const AtomicI64;
        let cqf = CountingQuotientFilter {
            metadata,
            blocks,
            runtimedata: Box::new(RuntimeData {
                file: None,
                auto_resize: false,
                num_locks: locks.len() as u64,
                metadata_lock: Mutex::new(()),
                locks,
            }),
        };
        Ok(cqf)
    }

    // pub fn serialize_raw(&self, path: PathBuf) -> Result<(), ()> {
    //     use std::io::{BufWriter, Write};
    //     let file = OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .open(path)
    //         .unwrap();
    //     let mut buffered_writer = BufWriter::new(file);
    //     let size = self.metadata().total_size_in_bytes;
    //     let buf = unsafe { std::slice::from_raw_parts(self.metadata as *const u8, size as usize) };
    //     match buffered_writer.write_all(buf) {
    //         Ok(_) => Ok(()),
    //         Err(_) => Err(()),
    //     }
    // }

    // pub fn new_file(
    //     lognslots: u64,
    //     quotient_bits: u64,
    //     hash_mode: HashMode,
    //     file: PathBuf,
    // ) -> Result<Self, ()> {
    //     if quotient_bits > BITS_PER_SLOT {
    //         return Err(());
    //     } else if quotient_bits == 0 {
    //         return Err(());
    //     } else if quotient_bits > lognslots {
    //         return Err(());
    //     }
    //     let num_slots: u64 = 1 << lognslots;
    //     let real_num_slots: u64 = (num_slots as f64 + 10 as f64 * (num_slots as f64).sqrt()) as u64;
    //     let num_blocks =
    //         (real_num_slots + QF_SLOTS_PER_BLOCK as u64 - 1) / QF_SLOTS_PER_BLOCK as u64;
    //     let remainder_bits = BITS_PER_SLOT as u64 - quotient_bits;

    //     let total_size_blocks: u64 = num_blocks * std::mem::size_of::<Block>() as u64;
    //     let total_bytes: u64 = total_size_blocks + std::mem::size_of::<Metadata>() as u64;

    //     let f = match OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .open(file)
    //     {
    //         Ok(f) => f,
    //         Err(_) => return Err(()),
    //     };

    //     match f.set_len(total_bytes as u64) {
    //         Ok(_) => (),
    //         Err(_) => return Err(()),
    //     };

    //     let mm = unsafe {
    //         mmap(
    //             ptr::null_mut(),
    //             total_bytes as usize,
    //             PROT_READ | PROT_WRITE,
    //             MAP_SHARED,
    //             f.as_raw_fd(),
    //             0,
    //         )
    //     };
    //     if mm == MAP_FAILED {
    //         return Err(());
    //     }
    //     let metadata = unsafe { &mut *(mm as *mut Metadata) };
    //     let blocks = unsafe { &mut *(mm.add(std::mem::size_of::<Metadata>()) as *mut Blocks) };

    //     metadata.logn_slots = lognslots;
    //     metadata.num_blocks = num_blocks;
    //     metadata.remainder_bits = remainder_bits;
    //     metadata.quotient_bits = quotient_bits;
    //     metadata.real_num_slots = real_num_slots;
    //     metadata.total_size_in_bytes = total_bytes;
    //     metadata.hash_mode = hash_mode as u32;
    //     metadata.num_occupied_slots = AtomicI64::new(0);

    //     let num_locks = (real_num_slots / NUM_SLOTS_TO_LOCK)
    //         + if real_num_slots % NUM_SLOTS_TO_LOCK == 0 {
    //             1
    //         } else {
    //             0
    //         };

    //     let mut locks: Vec<CachePadded<Mutex<()>>> = Vec::with_capacity(num_locks as usize);
    //     for _ in 0..num_locks {
    //         locks.push(CachePadded::new(Mutex::new(())));
    //     }

    //     // let nelts_ptr = &metadata_blocks.metadata.num_elements as *const AtomicI64;
    //     // let ndistinct_elts_ptr = &metadata_blocks.metadata.num_distinct_elts as *const AtomicI64;
    //     let noccupied_slots_ptr = &metadata.num_occupied_slots as *const AtomicI64;

    //     let cqf = CountingQuotientFilter {
    //         metadata,
    //         blocks,
    //         runtimedata: Box::new(RuntimeData {
    //             file: Some(f),
    //             auto_resize: false,
    //             // pc_num_elements: PartitionedCounter::new(nelts_ptr, NUM_COUNTERS, THRESHOLD),
    //             // pc_num_distinct_elements: PartitionedCounter::new(
    //             //     ndistinct_elts_ptr,
    //             //     NUM_COUNTERS,
    //             //     THRESHOLD,
    //             // ),
    //             // pc_num_occupied_slots: PartitionedCounter::new(
    //             //     noccupied_slots_ptr,
    //             //     NUM_COUNTERS,
    //             //     THRESHOLD,
    //             // ),
    //             num_locks,
    //             metadata_lock: Mutex::new(()),
    //             locks,
    //         }),
    //     };
    //     Ok(cqf)
    // }

    // fn metadata(&self) -> &Metadata {
    //     unsafe { &*self.metadata }
    // }

    // fn metadata_mut(&mut self) -> &mut Metadata {
    //     unsafe { &mut *self.metadata }
    // }

    // fn blocks(&self) -> &Blocks {
    //     unsafe { &*self.blocks }
    // }

    // fn blocks_mut(&mut self) -> &mut Blocks {
    //     unsafe { &mut *self.blocks }
    // }

    // pub fn open_file(file: PathBuf) -> Result<Self, ()> {
    //     let f = match OpenOptions::new().read(true).write(true).open(file) {
    //         Ok(f) => f,
    //         Err(_) => return Err(()),
    //     };

    //     // get file size
    //     let metadata = match f.metadata() {
    //         Ok(m) => m,
    //         Err(_) => return Err(()),
    //     };
    //     let total_bytes = metadata.len();

    //     let mm = unsafe {
    //         mmap(
    //             ptr::null_mut(),
    //             total_bytes as usize,
    //             PROT_READ | PROT_WRITE,
    //             MAP_SHARED,
    //             f.as_raw_fd(),
    //             0,
    //         )
    //     };

    //     if mm == MAP_FAILED {
    //         return Err(());
    //     }

    //     let metadata = unsafe { &mut *(mm as *mut Metadata) };
    //     let blocks = unsafe { &mut *(mm.add(std::mem::size_of::<Metadata>()) as *mut Blocks) };

    //     let num_locks = metadata.real_num_slots / NUM_SLOTS_TO_LOCK + 2;

    //     let mut locks: Vec<CachePadded<Mutex<()>>> = Vec::with_capacity(num_locks as usize);
    //     for _ in 0..num_locks {
    //         locks.push(CachePadded::new(Mutex::new(())));
    //     }

    //     // let nelts_ptr = &metadata_blocks.metadata.num_elements as *const AtomicI64;
    //     // let ndistinct_elts_ptr = &metadata_blocks.metadata.num_distinct_elts as *const AtomicI64;
    //     let noccupied_slots_ptr = &metadata.num_occupied_slots as *const AtomicI64;

    //     let cqf = CountingQuotientFilter {
    //         metadata,
    //         blocks,
    //         runtimedata: Box::new(RuntimeData {
    //             file: Some(f),
    //             auto_resize: false,
    //             // pc_num_elements: PartitionedCounter::new(nelts_ptr, NUM_COUNTERS, THRESHOLD),
    //             // pc_num_distinct_elements: PartitionedCounter::new(
    //             //     ndistinct_elts_ptr,
    //             //     NUM_COUNTERS,
    //             //     THRESHOLD,
    //             // ),
    //             // pc_num_occupied_slots: PartitionedCounter::new(
    //             //     noccupied_slots_ptr,
    //             //     NUM_COUNTERS,
    //             //     THRESHOLD,
    //             // ),
    //             num_locks,
    //             metadata_lock: Mutex::new(()),
    //             locks,
    //         }),
    //     };
    //     Ok(cqf)
    // }

    // pub fn merge_into(&self, other: Self) -> Result<(), ()> {
    //     if other.get_num_occupied_slots() + self.get_num_occupied_slots()
    //         >= (self.get_num_slots() as f64 * 0.95) as u64
    //     {
    //         return Err(());
    //     }

    //     for item in other.into_iter() {
    //         self.insert(item.hash, item.count).expect("Failed to merge");
    //     }

    //     Ok(())
    // }

    // pub fn insert(&self, item: u64, count: u64) -> Result<(), ()> {
    //     if self.get_num_occupied_slots() >= (self.get_num_slots() as f64 * 0.95) as u64 {
    //         return Err(());
    //     }
    //     let metadata = self.metadata();

    //     let hash = calc_hash(item, metadata.hash_mode);

    //     let (quotient, remainder) = self.quotient_remainder_from_hash(hash);
    //     let block_index = quotient / 64;
    //     let slot_index = quotient % 64;
    //     let blocks_per_lock = metadata.num_blocks / (self.runtimedata.num_locks);
    //     let lock_index = block_index / blocks_per_lock as usize;
    //     let mut lock = self.runtimedata.locks[lock_index].lock();
    //     let mut lock2 = self.runtimedata.locks[lock_index + 1].lock();

    //     self.insert_by_hash(hash, count);
    //     Ok(())
    // }

    // pub fn get_num_occupied_slots(&self) -> u64 {
    //     // self.runtimedata.pc_num_occupied_slots.sync();
    //     self.metadata().num_occupied_slots.load(Ordering::Relaxed) as u64

    //     // unsafe {*(&self.metadata_blocks
    //     //     .metadata
    //     //     .noccupied_slots as *const AtomicI64 as *const u64)}
    // }

    // // pub fn get_num_distinct_key_value_pairs(&self) -> u64 {
    // //     // self.runtimedata.pc_num_distinct_elements.sync();
    // //     self.metadata_blocks
    // //         .metadata
    // //         .num_distinct_elts
    // //         .load(Ordering::Relaxed) as u64
    // // }

    // // pub fn get_bits_per_slot(&self) -> u64 {
    // //     self.metadata_blocks.metadata.bits_per_slot
    // // }

    // pub fn get_num_slots(&self) -> u64 {
    //     1 << self.metadata().logn_slots
    // }

    // pub fn get_hash_mode(&self) -> HashMode {
    //     match self.metadata().hash_mode {
    //         0 => HashMode::Fast,
    //         1 => HashMode::Invertible,
    //         // 2 => HashMode::QFHashNone,
    //         _ => panic!("Invalid hash mode"),
    //     }
    // }

    // /// Returns total size of non-runtime data in bytes
    // pub fn total_size(&self) -> u64 {
    //     self.metadata().total_size_in_bytes
    // }

    // fn quotient_remainder_from_hash(&self, hash: u64) -> (usize, u64) {
    //     let quotient =
    //         (hash >> self.metadata().remainder_bits) & ((1 << self.metadata().quotient_bits) - 1);
    //     let remainder = hash & ((1 << self.metadata().remainder_bits) - 1);
    //     (quotient as usize, remainder)
    // }

    // pub fn insert_by_hash(&self, hash: u64, mut count: u64) {
    //     let blocks = self.blocks();
    //     let (quotient, remainder) = self.calc_qr(hash);
    //     let run_start_index = blocks.find_run_start(quotient);
    //     if !blocks.has_metadata_bits_set(quotient) && run_start_index == quotient {
    //         blocks.flip_occupied(quotient);
    //         blocks.set_remainder(quotient, remainder);
    //         if count == 1 {
    //             blocks.flip_runend(quotient);
    //             self.metadata()
    //                 .num_occupied_slots
    //                 .fetch_add(1, Ordering::SeqCst);
    //             return;
    //         } else if !blocks.has_metadata_bits_set(quotient + 1) {
    //             blocks.flip_count(quotient + 1);
    //             blocks.flip_runend(quotient + 1);
    //             blocks.set_remainder(quotient + 1, count - 1);
    //             self.metadata()
    //                 .num_occupied_slots
    //                 .fetch_add(2, Ordering::SeqCst);
    //             return;
    //         } else {
    //             count -= 1;
    //         }
    //         // self.metadata_blocks
    //         // .metadata
    //         // .num_distinct_elts
    //         // .fetch_add(1, Ordering::SeqCst);
    //         // self.metadata_blocks
    //         //     .metadata
    //         //     .num_elements
    //         //     .fetch_add(1, Ordering::SeqCst);
    //         // self.runtimedata.pc_num_occupied_slots.add(1);
    //         // self.runtimedata.pc_num_distinct_elements.add(1);
    //         // self.runtimedata.pc_num_elements.add(1);
    //     }
    //     if !blocks.is_occupied(quotient) {
    //         self.insert_and_shift(0, quotient, remainder, count, run_start_index, 0);
    //     } else {
    //         let quotient_of_remainder =
    //             blocks.find_quotient_of_remainder(run_start_index, remainder);
    //         let current_remainder = blocks.get_remainder(quotient_of_remainder);
    //         let current_count = blocks.get_remainder(quotient_of_remainder + 1);
    //         if current_remainder < remainder {
    //             self.insert_and_shift(1, quotient, remainder, count, quotient_of_remainder, 0);
    //         } else if current_remainder == remainder {
    //             if blocks.is_count(quotient_of_remainder + 1) {
    //                 let new_count = current_count + count;
    //                 blocks.set_remainder(quotient_of_remainder + 1, new_count);
    //                 return;
    //             }
    //             self.insert_and_shift(
    //                 if blocks.is_runend(quotient_of_remainder) {
    //                     1
    //                 } else {
    //                     2
    //                 },
    //                 quotient,
    //                 remainder,
    //                 current_count + count,
    //                 run_start_index,
    //                 quotient_of_remainder - run_start_index + 1,
    //             );
    //         } else {
    //             self.insert_and_shift(2, quotient, remainder, count, quotient_of_remainder, 0);
    //         }
    //     }
    //     blocks.set_occupied(quotient, true);
    // }

    // fn decode_counter(&self, index: usize, remainder: &mut u64, count: &mut u64) -> usize {
    //     *remainder = self.get_remainder(index);
    //     // if it's a runend or the next thing is not a count, there's only one
    //     if self.is_runend(index) || !self.is_count(index + 1) {
    //         *count = 1;
    //         return index;
    //     } else {
    //         // otherwise, whatever is in the next slot is the count
    //         *count = self.get_remainder(index + 1);
    //         return index + 1;
    //     }
    // }

    // fn find_first_empty_slot(&self, mut from: usize) -> usize {
    //     loop {
    //         let t = self.offset_lower_bound(from);
    //         if t == 0 {
    //             break;
    //         }
    //         from += t as usize;
    //     }
    //     return from;
    // }

    // fn offset_lower_bound(&self, index: usize) -> u64 {
    //     let block_idx = index / 64;
    //     let slot = index as u64 % 64;
    //     self.get_block(block_idx).offset_lower_bound(slot)
    // }

    // fn shift_remainders(&self, insert_index: usize, empty_slot_index: usize, distance: usize) {
    //     for i in (insert_index..=empty_slot_index).rev() {
    //         self.blocks()
    //             .set_remainder((i + distance) as u64, self.blocks().get_remainder(i as u64));
    //     }
    // }

    // fn shift_runends(&self, insert_index: usize, empty_slot_index: usize, distance: usize) {
    //     for i in (insert_index..=empty_slot_index).rev() {
    //         self.blocks()
    //             .set_runend((i + distance) as u64, self.blocks().is_runend(i as u64));
    //     }
    // }

    // fn shift_counts(&self, insert_index: usize, empty_slot_index: usize, distance: usize) {
    //     for i in (insert_index..=empty_slot_index).rev() {
    //         self.blocks()
    //             .set_count((i + distance) as u64, self.blocks().is_count(i as u64));
    //     }
    // }

    // fn insert_and_shift(
    //     &self,
    //     operation: u64,
    //     quotient: u64,
    //     remainder: u64,
    //     count: u64,
    //     insert_index: u64,
    //     noverwrites: u64,
    // ) {
    //     let blocks = self.blocks();
    //     let ninserts = if count == 1 { 1 } else { 2 } - noverwrites;
    //     if ninserts > 0 {
    //         match ninserts {
    //             1 => {
    //                 let empty = self.find_first_empty_slot(insert_index as usize);
    //                 self.shift_remainders(insert_index as usize, empty - 1, 1);
    //                 self.shift_runends(insert_index as usize, empty - 1, 1);
    //                 self.shift_counts(insert_index as usize, empty - 1, 1);
    //                 for i in
    //                     (((quotient as usize / 64) + 1)..).take_while(|i: &usize| *i <= empty / 64)
    //                 {
    //                     if empty / 64 < i as usize {
    //                         break;
    //                     }

    //                     let old_offset = self.blocks().get_block(i as u64).get_offset();
    //                     self.blocks().get_block_mut(i as u64).offset += 1;
    //                     // println!(
    //                     //     "1 offset for block {} was {}, is now {}, original blocks {}",
    //                     //     i,
    //                     //     old_offset,
    //                     //     self.get_block(i).offset,
    //                     //     (quotient / 64)
    //                     // );
    //                 }
    //                 // println!("1 count: {}", count);
    //             }
    //             2 => {
    //                 let first = self.find_first_empty_slot(insert_index as usize);
    //                 let second = self.find_first_empty_slot(first + 1);
    //                 self.shift_remainders(first + 1, second - 1, 1);
    //                 self.shift_runends(first + 1, second - 1, 1);
    //                 self.shift_counts(first + 1, second - 1, 1);
    //                 self.shift_remainders(insert_index as usize, first - 1, 2);
    //                 self.shift_runends(insert_index as usize, first - 1, 2);
    //                 self.shift_counts(insert_index as usize, first - 1, 2);

    //                 let mut npreceding_empties = 0;
    //                 for i in
    //                     (((quotient as usize / 64) + 1)..).take_while(|i: &usize| *i <= second / 64)
    //                 {
    //                     if npreceding_empties == 0 && first / 64 < i {
    //                         npreceding_empties += 1;
    //                     }
    //                     if npreceding_empties == 1 && second / 64 < i {
    //                         break;
    //                     }
    //                     let old_offset = self.get_block(i).offset;
    //                     self.get_block_mut(i).offset += (ninserts - npreceding_empties) as u16;
    //                     // println!(
    //                     //     "2 offset for block {} was {}, is now {}, original blocks {}",
    //                     //     i,
    //                     //     old_offset,
    //                     //     self.get_block(i).offset,
    //                     //     (quotient / 64)
    //                     // );
    //                 }
    //             }
    //             _ => panic!("unexpected number of inserts!"),
    //         }

    //         match operation {
    //             0 => {
    //                 if count == 1 {
    //                     blocks.set_runend(insert_index, true);
    //                 } else {
    //                     blocks.set_runend(insert_index, false);
    //                     blocks.set_runend(insert_index + 1, true);
    //                 }
    //             }
    //             1 => {
    //                 if noverwrites == 0 {
    //                     blocks.set_runend(insert_index - 1, false);
    //                 }
    //                 if count == 1 {
    //                     blocks.set_runend(insert_index, true);
    //                 } else {
    //                     blocks.set_runend(insert_index, false);
    //                     blocks.set_runend(insert_index + 1, true);
    //                 }
    //             }
    //             2 => {
    //                 if count == 1 {
    //                     blocks.set_runend(insert_index, false);
    //                 } else {
    //                     blocks.set_runend(insert_index, false);
    //                     blocks.set_runend(insert_index + 1, false);
    //                 }
    //             }
    //             _ => panic!("invalid operation!"),
    //         }
    //     }

    //     blocks.set_remainder(insert_index, remainder);
    //     if count != 1 {
    //         // if the count isn't one, put a count in the next slot
    //         blocks.set_count(insert_index + 1, true);
    //         blocks.set_remainder(insert_index + 1, count);
    //     }
    //     self.metadata()
    //         .num_occupied_slots
    //         .fetch_add(ninserts as i64, Ordering::SeqCst);
    // }

    // pub fn query(&self, item: u64) -> u64 {
    //     self.query_by_hash(self.calc_hash(item))
    // }

    // pub fn query_by_hash(&self, hash: u64) -> u64 {
    //     let blocks = self.blocks();
    //     let (quotient, remainder) = self.calc_qr(hash);
    //     if !blocks.is_occupied(quotient) {
    //         return 0;
    //     }
    //     let mut runstart_index = if quotient == 0 {
    //         0
    //     } else {
    //         blocks.find_run_end(quotient - 1) + 1
    //     };
    //     if runstart_index < quotient {
    //         runstart_index = quotient;
    //     }
    //     let mut current_end: usize;
    //     let mut current_remainder: u64 = 0;
    //     let mut current_count: u64 = 0;
    //     loop {
    //         current_end =
    //             self.decode_counter(runstart_index, &mut current_remainder, &mut current_count);
    //         if current_remainder == remainder {
    //             return current_count;
    //         }
    //         if self.is_runend(current_end) {
    //             break;
    //         }
    //         runstart_index = current_end + 1;
    //     }
    //     return 0;
    // }

    // fn calc_hash(&self, item: u64) -> u64 {
    //     match self.get_hash_mode() {
    //         HashMode::Invertible => {
    //             let mut key = item;
    //             key = (!key).wrapping_add(key << 21); // key = (key << 21) - key - 1;
    //             key = key ^ (key >> 24);
    //             key = (key.wrapping_add(key << 3)).wrapping_add(key << 8); // key * 265
    //             key = key ^ (key >> 14);
    //             key = (key.wrapping_add(key << 2)).wrapping_add(key << 4); // key * 21
    //             key = key ^ (key >> 28);
    //             key = key.wrapping_add(key << 31);
    //             key
    //         }
    //         HashMode::Fast => xxh3_64(&item.to_le_bytes()),
    //     }
    // }

    // pub fn invert_hash(&self, item: u64) -> Option<u64> {
    //     match self.get_hash_mode() {
    //         HashMode::Invertible => {
    //             let mut tmp: u64;
    //             let mut key = item;

    //             // Invert key = key + (key << 31)
    //             tmp = key.wrapping_sub(key << 31);
    //             key = key.wrapping_sub(tmp << 31);

    //             // Invert key = key ^ (key >> 28)
    //             tmp = key ^ key >> 28;
    //             key = key ^ tmp >> 28;

    //             // Invert key *= 21
    //             key = key.wrapping_mul(14933078535860113213);

    //             // Invert key = key ^ (key >> 14)
    //             tmp = key ^ key >> 14;
    //             tmp = key ^ tmp >> 14;
    //             tmp = key ^ tmp >> 14;
    //             key = key ^ tmp >> 14;

    //             // Invert key *= 265
    //             key = key.wrapping_mul(15244667743933553977);

    //             // Invert key = key ^ (key >> 24)
    //             tmp = key ^ key >> 24;
    //             key = key ^ tmp >> 24;

    //             // Invert key = (~key) + (key << 21)
    //             tmp = !key;
    //             tmp = !(key.wrapping_sub(tmp << 21));
    //             tmp = !(key.wrapping_sub(tmp << 21));
    //             key = !(key.wrapping_sub(tmp << 21));

    //             Some(key)
    //         }
    //         HashMode::Fast => None,
    //     }
    // }

    // fn calc_qr(&self, hash: u64) -> (u64, u64) {
    //     let quotient =
    //         (hash >> self.metadata().remainder_bits) & ((1 << self.metadata().quotient_bits) - 1);
    //     let remainder = hash & ((1 << self.metadata().remainder_bits) - 1);
    //     (quotient, remainder)
    // }

    // pub fn build_hash(&self, quotient: usize, remainder: u64) -> u64 {
    //     ((quotient as u64) << self.metadata().remainder_bits) | remainder
    // }

    // // fn is_occupied(&self, index: usize) -> bool {
    // //     let block_idx = index / 64;
    // //     let slot = index % 64;
    // //     self.get_block(block_idx).is_occupied(slot as u64)
    // // }

    // // fn set_occupied(&self, index: usize, val: bool) {
    // //     let block_idx = index / 64;
    // //     let slot = index % 64;
    // //     self.get_block_mut(block_idx).set_occupied(slot as u64, val)
    // // }

    // // fn is_runend(&self, index: usize) -> bool {
    // //     let block_idx = index / 64;
    // //     let slot = index as SlotIndex % 64;
    // //     self.get_block(block_idx).is_runend(slot)
    // // }

    // // fn set_runend(&self, index: usize, val: bool) {
    // //     let block_idx = index / 64;
    // //     let slot = index as SlotIndex % 64;
    // //     self.get_block_mut(block_idx).set_runend(slot, val)
    // // }

    // // fn is_count(&self, index: usize) -> bool {
    // //     let block_idx = index / 64;
    // //     let slot = index as SlotIndex % 64;
    // //     self.get_block(block_idx).is_count(slot)
    // // }

    // // fn set_count(&self, index: usize, val: bool) {
    // //     let block_idx = index / 64;
    // //     let slot = index as SlotIndex % 64;
    // //     self.get_block_mut(block_idx).set_count(slot, val)
    // // }

    // // fn get_remainder(&self, index: usize) -> u64 {
    // //     let block_idx = index / 64;
    // //     let slot = index as SlotIndex % 64;
    // //     self.get_block(block_idx).get_slot(slot)
    // // }

    // // fn set_remainder(&self, index: usize, val: u64) {
    // //     let block_idx = index / 64;
    // //     let slot = index as SlotIndex % 64;
    // //     self.get_block_mut(block_idx).set_slot(slot, val)
    // // }

    // fn might_be_empty(&self, index: usize) -> bool {
    //     let block_idx = index / 64;
    //     let slot = index as SlotIndex % 64;
    //     !self.get_block(block_idx).is_occupied(slot) && !self.get_block(block_idx).is_runend(slot)
    // }

    // fn run_end(&self, quotient: usize) -> usize {
    //     let block_idx: usize = quotient / 64;
    //     let slot_index: usize = quotient % 64;
    //     let blocks_offset: usize = self.get_block(block_idx).offset.into();
    //     let intrablock_rank: usize = bitrank(self.get_block(block_idx).occupieds, slot_index);

    //     if intrablock_rank == 0 {
    //         if blocks_offset <= slot_index {
    //             return quotient;
    //         } else {
    //             return quotient + blocks_offset - 1;
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

    // // fn qf_init(qf: &mut Self, mut slots: u64, key_bits: u64, value_bits: u64, hash: QfHashMode, seed: u32, buffer: &mut [u8]) {

    // //     assert!(slots.count_ones() == 1);

    // //     let num_slots: u64;
    // //     let xnslots: u64;
    // //     let nblocks: u64;
    // //     let mut key_remainder_bits: u64;
    // //     let bits_per_slot: u64;
    // //     let size: u64;
    // //     let total_bytes: u64;

    // //     num_slots = slots;
    // //     xnslots = (slots as f64 + 10 as f64 * (slots as f64).sqrt()) as u64;
    // //     nblocks = (xnslots + QF_SLOTS_PER_BLOCK as u64 - 1) / QF_SLOTS_PER_BLOCK as u64;
    // //     key_remainder_bits = key_bits;
    // //     while slots > 1 && key_remainder_bits > 0 {
    // //         slots >>= 1;
    // //         key_remainder_bits -= 1;
    // //     }
    // //     assert!(key_remainder_bits >= 2);

    // //     bits_per_slot = key_remainder_bits + value_bits;
    // //     assert!(QF_BITS_PER_SLOT == 0 || QF_BITS_PER_SLOT as u64 == qf.metadata_blocks.metadata.bits_per_slot);
    // //     assert!(bits_per_slot > 1);

    // // }

    // fn get_block(&self, block_idx: usize) -> &Block {
    //     if block_idx >= self.metadata().num_blocks as usize {
    //         panic!(
    //             "Tried getting block at idx {}, we only have {} blocks",
    //             block_idx,
    //             self.metadata().num_blocks
    //         )
    //     }
    //     let t = unsafe { &(*self.blocks().0.get_unchecked(block_idx).get()) };
    //     return t;
    // }

    // // pub fn print(&self) {
    // //     // let mut run_index = 0;
    // //     for i in 0..self.metadata().num_blocks {
    // //         let block = self.get_block(i as usize);

    // //         println!("Block {}, offset {}, occupied {}, runend {}, count {}", i, block.offset,
    // //             block.occupieds.count_ones(), block.runends.count_ones(), block.counts.count_ones()
    // //         );
    // //         for j in 0..64 as usize {
    // //             // if block.is_runend(j) && self.run_end((i * 64 + j as u64) as usize) >= (i * 64 + j as u64) as usize {
    // //             //     run_index += 1;
    // //             // }
    // //             println!(
    // //                 "Slot {} occupied: {} runend: {} count: {}, remainder: {}, run index {}",
    // //                 j,
    // //                 block.is_occupied(j),
    // //                 block.is_runend(j),
    // //                 block.is_count(j),
    // //                 block.get_slot(j),
    // //                 self.run_end((i * 64 + j as u64) as usize) % 64
    // //             );
    // //         }
    // //         println!("");
    // //     }
    // // }

    // /// Locks must be acquried prior to calling function
    // fn get_block_mut(&self, block_idx: usize) -> &mut Block {
    //     if block_idx >= self.metadata().num_blocks as usize {
    //         panic!(
    //             "Tried getting block at idx {}, we only have {} blocks",
    //             block_idx,
    //             self.metadata().num_blocks
    //         )
    //     }
    //     let t = unsafe { &mut *self.blocks().0.get_unchecked(block_idx).get() };
    //     return t;
    // }
}

impl Drop for CountingQuotientFilter {
    fn drop(&mut self) {
        let size = self.metadata().total_size_in_bytes;
        if self.runtimedata.file.is_some() {
            unsafe {
                munmap(self.metadata as *const _ as *mut c_void, size as usize);
            }
            let f = self.runtimedata.file.take().unwrap();
            drop(f);
        } else {
            let layout = Layout::from_size_align(size as usize, 8).unwrap();
            unsafe { alloc::dealloc(self.metadata as *const _ as *mut u8, layout) };
        }
    }
}

fn calc_hash(item: u64, mode: u32) -> u64 {
    match mode {
        1 => {
            let mut key = item;
            key = (!key).wrapping_add(key << 21); // key = (key << 21) - key - 1;
            key = key ^ (key >> 24);
            key = (key.wrapping_add(key << 3)).wrapping_add(key << 8); // key * 265
            key = key ^ (key >> 14);
            key = (key.wrapping_add(key << 2)).wrapping_add(key << 4); // key * 21
            key = key ^ (key >> 28);
            key = key.wrapping_add(key << 31);
            key
        }
        0 => xxh3_64(&item.to_le_bytes()),
        _ => panic!(),
    }
}

pub fn invert_hash(item: u64, mode: u32) -> Option<u64> {
    match mode {
        1 => {
            let mut tmp: u64;
            let mut key = item;

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

            Some(key)
        }
        _ => None,
    }
}

use bitintr::{Pdep, Tzcnt};

fn bitrank(val: u64, pos: u64) -> u64 {
    // if pos == 63 {
    //     (val & u64::MAX).count_ones() as usize
    // } else {
    //     (val & ((2 << pos) - 1)).count_ones() as usize
    // }
    (val & ((2 << pos) - 1)).count_ones() as u64
}

fn popcntv(val: u64, ignore: u64) -> u64 {
    if ignore % 64 != 0 {
        (val & !(bitmask(ignore as u64 % 64))).count_ones() as u64
    } else {
        val.count_ones() as u64
    }
}

/// Return trailing zeros of rank'th set bit
/// returns 64 if rank is larger than number of set bits
/// used to get index of val in bitvector
fn bitselect(val: u64, rank: u64) -> u64 {
    (1 << rank as u64).pdep(val).tzcnt() as u64
}

fn bitselectv(val: u64, ignore: u64, rank: u64) -> u64 {
    bitselect(val & !(bitmask(ignore as u64 % 64)), rank)
}

fn bitmask(nbits: u64) -> u64 {
    // if nbits == 64 {
    //     u64::MAX
    // } else {
    //     (1 << nbits) - 1
    // }
    (1 << nbits) - 1
}

impl<'a> IntoIterator for &'a CountingQuotientFilter {
    type Item = FilterItem;
    type IntoIter = CQFIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut position = 0;
        if !self.is_occupied(0) {
            let mut block_index: usize = 0;
            let mut idx = bitselect(self.get_block(0).occupieds, 0);
            if idx == 64 {
                while idx == 64 && block_index < (self.metadata().num_blocks - 1) as usize {
                    block_index += 1;
                    idx = bitselect(self.get_block(block_index).occupieds, 0);
                }
            }
            position = block_index * 64 + idx;
        }

        CQFIterator {
            qf: self,
            position: if position == 0 {
                0
            } else {
                self.run_end(position - 1) + 1
            },
            end: self.metadata().real_num_slots as usize,
            run: position as usize,
            first: true,
        }
    }
}

impl<'a> CQFIterator<'a> {
    fn move_position(&mut self) -> bool {
        if self.position >= self.qf.metadata().real_num_slots as usize {
            return false;
        } else {
            let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
            self.position =
                self.qf
                    .decode_counter(self.position, &mut current_remainder, &mut current_count);
            if !self.qf.is_runend(self.position) {
                self.position += 1;
                if self.position >= self.qf.metadata().real_num_slots as usize {
                    return false;
                }
                return true;
            } else {
                let mut block_idx = self.run / 64;
                let mut rank = bitrank(self.qf.get_block(block_idx).occupieds, self.run % 64);
                let mut next_run = bitselect(self.qf.get_block(block_idx).occupieds, rank);

                if next_run == 64 {
                    rank = 0;
                    while next_run == 64 && block_idx < (self.qf.metadata().num_blocks - 1) as usize
                    {
                        block_idx += 1;
                        next_run = bitselect(self.qf.get_block(block_idx).occupieds, rank);
                    }
                }

                if block_idx == self.qf.metadata().num_blocks as usize {
                    self.run = self.qf.metadata().real_num_slots as usize;
                    self.position = self.qf.metadata().real_num_slots as usize;
                    return false;
                }

                self.run = block_idx * 64 + next_run;
                self.position += 1;
                if self.position < self.run {
                    self.position = self.run;
                }

                if self.position >= self.qf.metadata().real_num_slots as usize {
                    return false;
                }

                return true;
            }
        }
    }
}

impl<'a> IntoParallelIterator for &'a CountingQuotientFilter {
    type Item = FilterItem;
    type Iter = CQFIterator<'a>;

    fn into_par_iter(self) -> Self::Iter {
        return (&self).into_iter();
    }
}

pub struct CQFIterator<'a> {
    qf: &'a CountingQuotientFilter,
    position: usize,
    end: usize,
    run: usize,
    first: bool,
}

pub struct FilterItem {
    pub hash: u64,
    pub item: Option<u64>,
    pub count: u64,
}

impl<'a> Iterator for CQFIterator<'a> {
    type Item = FilterItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.first {
            self.first = false;
            let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
            self.qf
                .decode_counter(self.position, &mut current_remainder, &mut current_count);
            let hash = self.qf.build_hash(self.run, current_remainder);
            return Some(FilterItem {
                hash,
                item: self.qf.invert_hash(hash),
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
            .decode_counter(self.position, &mut current_remainder, &mut current_count);
        let hash = self.qf.build_hash(self.run, current_remainder);
        Some(FilterItem {
            hash,
            item: self.qf.invert_hash(hash),
            count: current_count,
        })
    }
}

// pub struct CQFParallelIterator<'a> {
//     qf: &'a CountingQuotientFilter,
//     position: usize,
//     run: usize,
//     first: bool,
//     end: usize,
// }

impl ParallelIterator for CQFIterator<'_> {
    type Item = FilterItem;

    fn drive_unindexed<C>(self, consumer: C) -> <C as Consumer<Self::Item>>::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        rayon::iter::plumbing::bridge_unindexed(self, consumer)
    }
}

impl UnindexedProducer for CQFIterator<'_> {
    type Item = FilterItem;

    fn fold_with<F>(self, folder: F) -> F
    where
        F: rayon::iter::plumbing::Folder<Self::Item>,
    {
        folder.consume_iter(self)
    }

    fn split(self) -> (Self, Option<Self>) {
        let mid = self.position + (self.end - self.position) / 2;
        // if mid == self.position {
        //     return (self, None);
        // }

        // Dont split if it's 1/8th of the size of the cqf
        if self.end - self.position <= self.qf.metadata().real_num_slots as usize / 2 {
            return (self, None);
        }

        let mut position = self.qf.run_end(mid) + 1;

        // //////////////////////////////////////

        if !self.qf.is_occupied(position) {
            let mut block_index: usize = position / 64;
            let mut idx = bitselect(self.qf.get_block(block_index).occupieds, 0);
            if idx == 64 {
                while idx == 64 && block_index < (self.qf.metadata().num_blocks - 1) as usize {
                    block_index += 1;
                    idx = bitselect(self.qf.get_block(block_index).occupieds, 0);
                }
            }
            position = block_index * 64 + idx;
        }

        println!(
            "occupied position {} {} {}",
            position,
            self.qf.is_occupied(position),
            self.qf.is_runend(position - 1)
        );
        // //////////////////////////////////////
        println!("left {}, {}, {}", self.position, position, self.end);
        let mut right = CQFIterator {
            qf: self.qf,
            position: position,
            run: self.run,
            first: self.first,
            end: self.end,
        };
        let mut left = CQFIterator {
            qf: self.qf,
            position: self.position,
            run: self.run,
            first: true,
            end: position - 1,
        };
        // left.move_position();
        // right.move_position();
        // (left, None)
        (left, Some(right))
    }
}

// call runend at length / 2, left and right

// fn into_iter(self) -> Self::IntoIter {
//     let mut position = 0;
//     if !self.is_occupied(0) {
//         let mut block_index: usize = 0;
//         let mut idx = bitselect(self.get_block(0).occupieds, 0);
//         if idx == 64 {
//             while idx == 64 && block_index < (self.metadata().num_blocks - 1) as usize {
//                 block_index += 1;
//                 idx = bitselect(self.get_block(block_index).occupieds, 0);
//             }
//         }
//         position = block_index * 64 + idx;
//     }

//     CQFIterator {
//         qf: self,
//         position: if position == 0 {
//             0
//         } else {
//             self.run_end(position - 1) + 1
//         },
//         end: self.metadata().real_num_slots as usize,
//         run: position as usize,
//         first: true,
//     }
// }
