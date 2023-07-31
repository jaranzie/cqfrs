// use parking_lot::Mutex;
// use std::cell::UnsafeCell;
// use std::sync::Arc;
// use std::{
//     fs::File,
//     sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering},
// };
// use partitioned_counter::PartitionedCounter;
// use blocks::Block;
// use std::alloc::{self, Layout};

// const MAGIC_NUMBER: u64 = 1018874902021329732;
// const QF_BLOCK_OFFSET_BITS: usize = 6;
// const QF_SLOTS_PER_BLOCK: usize = 1usize << QF_BLOCK_OFFSET_BITS;
// const QF_METADATA_WORDS_PER_BLOCK: usize = ((QF_SLOTS_PER_BLOCK + 63) / 64) as usize;
// const THRESHOLD: i32 = 100;
// const NUM_COUNTERS: u32 = 8;
// const NUM_SLOTS_TO_LOCK: u64 = 1 << 16;
// // const QF_BITS_PER_SLOT: usize = 0; custom (find way to implement)
// // type BlockT = u8;
// // const QF_BITS_PER_SLOT: usize = 0;
// // type BlockT = u8;
// // const QF_BITS_PER_SLOT: usize = 16;
// // type BlockT = u16;
// // const QF_BITS_PER_SLOT: usize = 32;
// // type BlockT = u32;
// const QF_BITS_PER_SLOT: u64 = 64;
// type BlockT = u64;

// pub enum QfHashMode {
//     QfHashDefault = 0,
//     QfHashInvertible = 1,
//     QFHashNone = 2,
// }

// pub struct RuntimeData {
//     pub file: Option<File>,
//     pub auto_resize: bool,
//     pub pc_num_elements: PartitionedCounter,
//     pub pc_num_distinct_elements: PartitionedCounter,
//     pub pc_num_occupied_slots: PartitionedCounter,
//     pub num_locks: u64,
//     pub metadata_lock: Mutex<()>,
//     pub locks: Vec<Mutex<()>>,
// }

// #[repr(C)]
// pub struct Metadata {
//     pub total_size_in_bytes: u64,
//     pub logn_slots: u64,
//     pub real_num_slots: u64,
//     pub num_blocks: u64,
//     pub quotient_bits: u64,
//     pub remainder_bits: u64,
//     // Unsure if needed
//     pub bits_per_slot: u64,
//     pub range: u128,
//     pub nelts: Arc<AtomicI64>,
//     pub ndistinct_elts: Arc<AtomicI64>,
//     pub noccupied_slots: Arc<AtomicI64>,
//     pub magic_endian_number: u64,
//     pub seed: u32,
//     pub hash_mode: u32,
//     pub reserved: u32,
// }

// // mod blocks {
//     // use super::BlockT;
//     // use super::{bitmask, bitrank, bitselectv, popcntv};
//     // use bitintr::{Pdep, Popcnt, Tzcnt};
//     // use super::QF_SLOTS_PER_BLOCK;

//     #[repr(C)]
//     pub struct Block {
//         pub offset: u16,
//         // occupieds: [u64; QF_METADATA_WORDS_PER_BLOCK],
//         // runends: [u64; QF_METADATA_WORDS_PER_BLOCK],
//         // counts: [u64; QF_METADATA_WORDS_PER_BLOCK],
//         occupieds: u64,
//         runends: u64,
//         counts: u64,
//         remainders: [BlockT; QF_SLOTS_PER_BLOCK],
//     }

//     impl Block {
//         #[inline]
//         pub fn flip_occupied(&mut self, slot: usize) {
//             self.occupieds ^= 1 << slot;
//         }

//         #[inline]
//         pub fn is_occupied(&self, slot: usize) -> bool {
//             ((self.occupieds >> slot) & 1) != 0
//         }

//         pub fn set_occupied(&mut self, slot: usize, bit: bool) {
//             if bit {
//                 self.occupieds |= 1 << slot;
//             } else {
//                 self.occupieds &= !(1 << slot);
//             }
//         }

//         #[inline]
//         pub fn flip_runend(&mut self, slot: usize) {
//             self.runends ^= 1 << slot;
//         }

//         #[inline]
//         pub fn is_runend(&self, slot: usize) -> bool {
//             ((self.runends >> slot) & 1) != 0
//         }

//         pub fn set_runend(&mut self, slot: usize, bit: bool) {
//             if bit {
//                 self.runends |= 1 << slot;
//             } else {
//                 self.runends &= !(1 << slot);
//             }
//         }

//         #[inline]
//         pub fn flip_count(&mut self, slot: usize) {
//             self.counts ^= 1 << slot;
//         }

//         #[inline]
//         pub fn is_count(&self, slot: usize) -> bool {
//             ((self.counts >> slot) & 1) != 0
//         }

//         pub fn set_count(&mut self, slot: usize, bit: bool) {
//             if bit {
//                 self.counts |= 1 << slot;
//             } else {
//                 self.counts &= !(1 << slot);
//             }
//         }

//         pub fn set_slot(&mut self, slot: usize, remainder: u64) {
//             self.remainders[slot] = remainder;
//         }

//         pub fn get_slot(&self, slot: usize) -> u64 {
//             self.remainders[slot]
//         }

//         fn offset_lower_bound(&self, slot: u64) -> u64 {
//             let occupieds = self.occupieds & bitmask(slot + 1);
//             let offset_64: u64 = self.offset.into();
//             if offset_64 <= slot {
//                 let runends = (self.runends & bitmask(slot)) >> offset_64;
//                 return (occupieds.count_ones() - runends.count_ones()) as u64;
//             }
//             return offset_64 - slot + occupieds.count_ones() as u64;
//         }
//     }
// // }

// pub struct CountingQuotientFilter {
//     pub runtimedata: Box<RuntimeData>,
//     pub metadata_blocks: Box<MetadataBlocks>,
// }

// #[repr(C)]
// pub struct MetadataBlocks {
//     pub metadata: Metadata,
//     pub blocks: [UnsafeCell<Block>; 1],
// }

// pub enum InsertFlags {
//     Test,
// }

// // struct Blocks([UnsafeCell<Block>; 1]);

// /// lognslots should be atleast as big as quotient_bits, probably equal is best
// impl CountingQuotientFilter {
//     pub fn new(
//         lognslots: u64,
//         quotient_bits: u64,
//         hash: QfHashMode,
//         seed: u32,
//     ) -> Result<Self, ()> {
//         if lognslots.count_ones() != 1 {
//             return Err(());
//         } else if quotient_bits > QF_BITS_PER_SLOT {
//             return Err(());
//         } else if quotient_bits == 0 {
//             return Err(());
//         } else if quotient_bits > lognslots {
//             return Err(());
//         }

//         let num_slots: u64 = 1 << lognslots;
//         let real_num_slots: u64 = (num_slots as f64 + 10 as f64 * (num_slots as f64).sqrt()) as u64;
//         let num_blocks =
//             (real_num_slots + QF_SLOTS_PER_BLOCK as u64 - 1) / QF_SLOTS_PER_BLOCK as u64;
//         let remainder_bits = QF_BITS_PER_SLOT as u64 - quotient_bits;

//         let total_size_blocks: u64 = num_blocks * std::mem::size_of::<Block>() as u64;
//         let total_bytes: u64 = total_size_blocks + std::mem::size_of::<Metadata>() as u64;

//         let layout = Layout::from_size_align(total_bytes as usize, 8).unwrap();
//         let buffer = unsafe { alloc::alloc(layout) };
//         if buffer.is_null() {
//             // Memory allocation error
//             return Err(());
//         }
//         let mut metadata_blocks = unsafe { Box::from_raw(buffer as *mut MetadataBlocks) };
//         metadata_blocks.metadata.seed = seed;
//         metadata_blocks.metadata.logn_slots = lognslots;
//         metadata_blocks.metadata.num_blocks = num_blocks;
//         metadata_blocks.metadata.remainder_bits = remainder_bits;
//         metadata_blocks.metadata.quotient_bits = quotient_bits;
//         metadata_blocks.metadata.real_num_slots = real_num_slots;
//         metadata_blocks.metadata.total_size_in_bytes = total_bytes;

//         metadata_blocks.metadata.hash_mode = hash as u32;
//         metadata_blocks.metadata.magic_endian_number = MAGIC_NUMBER;
//         metadata_blocks.metadata.reserved = 0;
//         metadata_blocks.metadata.range = (num_slots << 0) as u128;

//         let nelts = Arc::new(AtomicI64::new(0));
//         metadata_blocks.metadata.nelts = nelts.clone();
//         let ndistinct_elts = Arc::new(AtomicI64::new(0));
//         metadata_blocks.metadata.ndistinct_elts = ndistinct_elts.clone();
//         let noccupied_slots = Arc::new(AtomicI64::new(0));
//         metadata_blocks.metadata.noccupied_slots = noccupied_slots.clone();

//         let num_locks = metadata_blocks.metadata.real_num_slots / NUM_SLOTS_TO_LOCK + 2;

//         let mut locks = Vec::with_capacity(num_locks as usize);
//         for _ in 0..num_locks {
//             locks.push(Mutex::new(()));
//         }

//         let mut cqf = CountingQuotientFilter {
//             metadata_blocks,
//             runtimedata: Box::new(RuntimeData {
//                 file: None,
//                 auto_resize: false,
//                 pc_num_elements: PartitionedCounter::new(nelts.clone(), NUM_COUNTERS, THRESHOLD),
//                 pc_num_distinct_elements: PartitionedCounter::new(
//                     ndistinct_elts.clone(),
//                     NUM_COUNTERS,
//                     THRESHOLD,
//                 ),
//                 pc_num_occupied_slots: PartitionedCounter::new(
//                     noccupied_slots.clone(),
//                     NUM_COUNTERS,
//                     THRESHOLD,
//                 ),
//                 num_locks: 0,
//                 metadata_lock: Mutex::new(()),
//                 locks,
//             }),
//         };
//         Ok(cqf)
//     }

//     pub fn insert(&self, item: u64, count: u64, flags: InsertFlags) -> Result<(), ()> {
//         if self.get_num_occupied_slots() >= (self.get_num_slots() as f64 * 0.95) as u64 {
//             // if self.runtimedata.auto_resize
//             return Err(());
//         }

//         let hash = calc_hash(item, self.metadata_blocks.metadata.hash_mode);
//         self.insert_by_hash(hash, count)
//     }

//     pub fn get_num_occupied_slots(&self) -> u64 {
//         self.runtimedata.pc_num_occupied_slots.sync();
//         self.metadata_blocks
//             .metadata
//             .noccupied_slots
//             .load(Ordering::Relaxed) as u64
//     }

//     pub fn get_num_distinct_key_value_pairs(&self) -> u64 {
//         self.runtimedata.pc_num_distinct_elements.sync();
//         self.metadata_blocks
//             .metadata
//             .ndistinct_elts
//             .load(Ordering::Relaxed) as u64
//     }

//     pub fn get_bits_per_slot(&self) -> u64 {
//         self.metadata_blocks.metadata.bits_per_slot
//     }

//     pub fn get_num_slots(&self) -> u64 {
//         1 << self.metadata_blocks.metadata.logn_slots
//     }

//     // pub fn get_num_key_remainder_bits(&self) -> u64 {
//     //     self.metadata_blocks.metadata.key_remainder_bits
//     // }

//     // pub fn get_num_value_bits(&self) -> u64 {
//     //     self.metadata_blocks.metadata.value_bits
//     // }

//     // pub fn get_num_key_bits(&self) -> u64 {
//     //     self.metadata_blocks.metadata.key_bits
//     // }

//     pub fn get_hash_seed(&self) -> u32 {
//         self.metadata_blocks.metadata.seed
//     }

//     pub fn get_hash_mode(&self) -> QfHashMode {
//         match self.metadata_blocks.metadata.hash_mode {
//             0 => QfHashMode::QfHashDefault,
//             1 => QfHashMode::QfHashInvertible,
//             2 => QfHashMode::QFHashNone,
//             _ => panic!("Invalid hash mode"),
//         }
//     }

//     pub fn get_hash_range(&self) -> u128 {
//         self.metadata_blocks.metadata.range
//     }

//     pub fn is_auto_resize(&self) -> bool {
//         self.runtimedata.auto_resize
//     }

//     /// Returns total size in bytes of the filter
//     pub fn total_size(&self) -> u64 {
//         self.metadata_blocks.metadata.total_size_in_bytes
//     }

//     pub fn get_sum_of_counts(&self) -> u64 {
//         self.runtimedata.pc_num_elements.sync();
//         self.metadata_blocks.metadata.nelts.load(Ordering::Relaxed) as u64
//     }

//     fn quotient_remainder_from_hash(&self, hash: u64) -> (usize, u64) {
//         let quotient = (hash >> self.metadata_blocks.metadata.remainder_bits)
//             & ((1 << self.metadata_blocks.metadata.quotient_bits) - 1);
//         let remainder = hash & ((1 << self.metadata_blocks.metadata.remainder_bits) - 1);
//         (quotient as usize, remainder)
//     }

//     fn might_be_empty(&self, index: usize) -> bool {
//         let block_idx = index / 64;
//         let slot = index % 64;
//         !self.get_block(block_idx).is_occupied(slot) && !self.get_block(block_idx).is_runend(slot)
//     }

//     fn is_occupied(&self, index: usize) -> bool {
//         let block_idx = index / 64;
//         let slot = index % 64;
//         self.get_block(block_idx).is_occupied(slot)
//     }

//     fn insert_by_hash(&self, hash: u64, count: u64) -> Result<(), ()> {
//         let (quotient, remainder) = self.quotient_remainder_from_hash(hash);
//         let block_index = quotient / 64;
//         let slot_index = quotient % 64;
//         let blocks_per_lock = self.metadata_blocks.metadata.num_blocks / self.runtimedata.num_locks;
//         let lock_index = block_index / blocks_per_lock as usize;
//         let mut lock = self.runtimedata.locks[lock_index].lock();
//         let mut lock2 = self.runtimedata.locks[next_lock_index].lock();

//         let block = self.get_block_mut(block_index);

//         let runend_index = self.run_end(quotient);

//         if self.might_be_empty(quotient) && runend_index == quotient {
//             {
//                 block.set_runend(quotient % 64, true);
//                 block.set_occupied(quotient % 64, true);
//                 block.set_slot(quotient % 64, remainder);
//             }
//             self.runtimedata.pc_num_occupied_slots.add(1);
//             self.runtimedata.pc_num_distinct_elements.add(1);
//             self.runtimedata.pc_num_elements.add(1);
//             if count > 1 {
//                 self.insert_by_hash(hash, count - 1)?;
//             }
//         } else {
//             let mut runstart_index = if quotient == 0 {
//                 0
//             } else {
//                 self.run_end(quotient - 1) + 1
//             };
//             if !self.is_occupied(quotient) {
//                 self.insert_and_shift(0, quotient, remainder, count, runstart_index, 0);
//             } else {
//                 let (mut current_remainder, mut current_count): (u64, u64) = (0, 0);
//                 let mut current_end: usize;
//                 current_end =
//                     self.decode_counter(runstart_index, &mut current_remainder, &mut current_count);
//                 while current_remainder < remainder && !self.is_runend(current_end) {
//                     runstart_index = current_end + 1;
//                     current_end = self.decode_counter(
//                         runstart_index,
//                         &mut current_remainder,
//                         &mut current_count,
//                     )
//                 }

//                 if current_remainder < remainder {
//                     self.insert_and_shift(1, quotient, remainder, count, current_end + 1, 0);
//                 } else if current_remainder == remainder {
//                     self.insert_and_shift(
//                         if self.is_runend(current_end) { 1 } else { 2 },
//                         quotient,
//                         remainder,
//                         current_count + count,
//                         runstart_index,
//                         current_end - runstart_index + 1,
//                     );
//                 } else {
//                     self.insert_and_shift(2, quotient, remainder, count, runstart_index, 0);
//                 }
//             }
//             self.set_occupied(quotient, true);
//         }
//         Ok(())
//     }

//     // fn qf_init(qf: &mut Self, mut slots: u64, key_bits: u64, value_bits: u64, hash: QfHashMode, seed: u32, buffer: &mut [u8]) {

//     //     assert!(slots.count_ones() == 1);

//     //     let num_slots: u64;
//     //     let xnslots: u64;
//     //     let nblocks: u64;
//     //     let mut key_remainder_bits: u64;
//     //     let bits_per_slot: u64;
//     //     let size: u64;
//     //     let total_bytes: u64;

//     //     num_slots = slots;
//     //     xnslots = (slots as f64 + 10 as f64 * (slots as f64).sqrt()) as u64;
//     //     nblocks = (xnslots + QF_SLOTS_PER_BLOCK as u64 - 1) / QF_SLOTS_PER_BLOCK as u64;
//     //     key_remainder_bits = key_bits;
//     //     while slots > 1 && key_remainder_bits > 0 {
//     //         slots >>= 1;
//     //         key_remainder_bits -= 1;
//     //     }
//     //     assert!(key_remainder_bits >= 2);

//     //     bits_per_slot = key_remainder_bits + value_bits;
//     //     assert!(QF_BITS_PER_SLOT == 0 || QF_BITS_PER_SLOT as u64 == qf.metadata_blocks.metadata.bits_per_slot);
//     //     assert!(bits_per_slot > 1);

//     // }

//     fn get_block(&self, block_idx: usize) -> &Block {
//         if block_idx >= self.metadata_blocks.metadata.nblocks as usize {
//             panic!(
//                 "Tried getting block at idx {}, we only have {} blocks",
//                 block_idx, self.metadata_blocks.metadata.nblocks
//             )
//         }
//         let t = unsafe { &*self.metadata_blocks.blocks[block_idx].get() };
//         return t;
//     }

//     /// Locks must be acquried prior to calling function
//     fn get_block_mut(&self, block_idx: usize) -> &mut Block {
//         if block_idx >= self.metadata_blocks.metadata.num_blocks as usize {
//             panic!(
//                 "Tried getting block at idx {}, we only have {} blocks",
//                 block_idx, self.metadata_blocks.metadata.num_blocks
//             )
//         }
//         let t = unsafe { &mut *self.metadata_blocks.blocks[block_idx].get() };
//         return t;
//     }

//     fn run_end(&self, quotient: usize) -> usize {
//         let block_idx: usize = quotient / 64;
//         let intrablock_offset: usize = quotient % 64;
//         let blocks_offset: usize = self.get_block(block_idx).offset.into();
//         let intrablock_rank: usize =
//             bitrank(self.get_block(block_idx).occupieds, intrablock_offset);

//         if intrablock_rank == 0 {
//             if blocks_offset <= intrablock_offset {
//                 return quotient;
//             } else {
//                 return 64 * block_idx + blocks_offset - 1;
//             }
//         }

//         let mut runend_block_index: usize = block_idx + blocks_offset / 64;
//         let mut runend_ignore_bits: usize = blocks_offset % 64;
//         let mut runend_rank: usize = intrablock_rank - 1;
//         let mut runend_block_offset: usize = bitselectv(
//             self.get_block(runend_block_index).runends,
//             runend_ignore_bits,
//             runend_rank,
//         );

//         if runend_block_offset == 64 {
//             if blocks_offset == 0 && intrablock_rank == 0 {
//                 return quotient;
//             } else {
//                 loop {
//                     runend_rank -= popcntv(
//                         self.get_block(runend_block_index).runends,
//                         runend_ignore_bits,
//                     );
//                     runend_block_index += 1;
//                     runend_ignore_bits = 0;
//                     runend_block_offset = bitselectv(
//                         self.get_block(runend_block_index).runends,
//                         runend_ignore_bits,
//                         runend_rank,
//                     );
//                     if runend_block_offset != 64 {
//                         break;
//                     }
//                 }
//             }
//         }

//         let runend_index = 64 * runend_block_index + runend_block_offset;
//         if runend_index < quotient {
//             quotient
//         } else {
//             runend_index
//         }
//     }
// }

// fn calc_hash(item: u64, mode: u32) -> u64 {
//     match mode {
//         1 => {
//             let mut key = item;
//             key = (!key).wrapping_add(key << 21); // key = (key << 21) - key - 1;
//             key = key ^ (key >> 24);
//             key = (key.wrapping_add(key << 3)).wrapping_add(key << 8); // key * 265
//             key = key ^ (key >> 14);
//             key = (key.wrapping_add(key << 2)).wrapping_add(key << 4); // key * 21
//             key = key ^ (key >> 28);
//             key = key.wrapping_add(key << 31);
//             key
//         } // 0 => xxh3_64(&item.to_le_bytes()),
//     }
// }

// pub fn invert_hash(item: u64, mode: u32) -> Option<u64> {
//     match mode {
//         1 => {
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
//         } // 0 => None,
//     }
// }
// fn bitrank(val: u64, pos: usize) -> usize {
//     if pos == 63 {
//         (val & u64::MAX).popcnt() as usize
//     } else {
//         (val & ((2 << pos) - 1)).popcnt() as usize
//     }
// }

// fn popcntv(val: u64, ignore: usize) -> usize {
//     if ignore % 64 != 0 {
//         (val & !(bitmask(ignore as u64 % 64))).popcnt() as usize
//     } else {
//         val.popcnt() as usize
//     }
// }

// fn bitselect(val: u64, rank: usize) -> usize {
//     (1 << rank as u64).pdep(val).tzcnt() as usize
// }

// fn bitselectv(val: u64, ignore: usize, rank: usize) -> usize {
//     bitselect(val & !(bitmask(ignore as u64 % 64)), rank)
// }

// fn bitmask(nbits: u64) -> u64 {
//     if nbits == 64 {
//         u64::MAX
//     } else {
//         (1 << nbits) - 1
//     }
// }
