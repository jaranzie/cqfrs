#![feature(sync_unsafe_cell)]
#![feature(unchecked_math)]
#![feature(ptr_internals)]
#![feature(core_intrinsics)]
mod blocks;
const SLOTS_PER_BLOCK: usize = 64;
// mod old_blocks;
// mod cqf_u64;
// mod generic_cqf;
// // mod cqf_u16;
// mod partitioned_counter;
// mod reversible_hasher;
mod cqf;
mod utils;

// pub use reversible_hasher::*;
// use std::hash::BuildHasher;
// use std::ops::{Deref, DerefMut};
// use std::path::PathBuf;
// use std::sync::atomic::AtomicU64;
// pub use utils::*;

// // pub use generic_cqf::{*};
// pub use cqf_u64::{*};
// // pub use cqf_u64::CQFIterator;
// // pub use cqf_u64::CountingQuotientFilter;
// // pub use cqf_u64::CqfMergeCallback;
// // pub use cqf_u64::HashCount;
// // pub use cqf_u64::ZippedCqfIterator;

// // const QF_BLOCK_OFFSET_BITS: usize = 6;

// struct RuntimeData<Hasher: BuildHasher> {
//     pub file: Option<PathBuf>,
//     pub hasher: Hasher,
//     pub max_occupied_slots: u64,
// }

// struct Metadata {
//     pub total_size_in_bytes: u64,
//     pub num_real_slots: u64,
//     pub num_occupied_slots: AtomicU64,
//     pub num_blocks: u64,
//     pub quotient_bits: u64,
//     pub remainder_bits: u64,
//     pub hash_mode: u64,
// }

// struct MetadataWrapper {
//     inner: std::ptr::Unique<Metadata>,
// }

// impl MetadataWrapper {
//     pub fn new(metadata: *mut Metadata) -> Self {
//         let inner = unsafe { std::ptr::Unique::new_unchecked(metadata) };
//         MetadataWrapper { inner }
//     }
// }

// impl Deref for MetadataWrapper {
//     type Target = Metadata;
//     fn deref(&self) -> &Self::Target {
//         unsafe { self.inner.as_ref() }
//     }
// }

// impl DerefMut for MetadataWrapper {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         unsafe { self.inner.as_mut() }
//     }
// }

// impl Metadata {
//     fn new(quotient_bits: u64, hash_bits: u64, blocks_total_size: u64, invertable: bool) -> Self {
//         let num_slots: u64 = 1 << quotient_bits;
//         let num_real_slots: u64 = (num_slots as f64 + 10 as f64 * (num_slots as f64).sqrt()) as u64;
//         let num_blocks = (num_real_slots + SLOTS_PER_BLOCK as u64 - 1) / SLOTS_PER_BLOCK as u64;
//         let remainder_bits = hash_bits - quotient_bits;
//         let total_bytes: u64 = blocks_total_size + std::mem::size_of::<Metadata>() as u64;
//         let hash_mode = if invertable { 1 } else { 0 };

//         Self {
//             total_size_in_bytes: total_bytes,
//             num_real_slots,
//             num_blocks,
//             quotient_bits,
//             remainder_bits,
//             num_occupied_slots: AtomicU64::new(0),
//             hash_mode,
//         }
//     }

//     fn invertable(&self) -> bool {
//         self.hash_mode == 1
//     }
// }

// #[derive(Debug)]
// pub enum CqfError {
//     FileError,
//     MmapError,
//     InvalidArguments,
//     InvalidFile,
//     InvalidSize,
//     Filled,
// }
