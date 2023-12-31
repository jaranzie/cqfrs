// use crate::SLOTS_PER_BLOCK;
// use std::{
//     fs::File,
//     hash::{BuildHasher, Hash},
// };

// struct MetadataWrapper(std::ptr::Unique<Metadata>);

// impl MetadataWrapper {
//     pub fn new(metadata: *mut Metadata) -> Self {
//         let inner = unsafe { std::ptr::Unique::new_unchecked(metadata) };
//         Self(inner)
//     }
// }

// #[repr(C)]
// #[derive(Debug, Copy, Clone)]
// struct Metadata {
//     pub total_size_bytes: u64,
//     pub num_real_slots: u64,
//     pub num_unique_keys: u64,
//     pub num_used_slots: u64,
//     pub num_blocks: u64,
//     pub quotient_bits: u64,
//     pub remainder_bits: u64,
//     // 0 for non-invertable, 1 for invertable
//     pub invertable: u64,
// }

// impl std::ops::Deref for MetadataWrapper {
//     type Target = Metadata;
//     fn deref(&self) -> &Self::Target {
//         unsafe { self.0.as_ref() }
//     }
// }

// impl std::ops::DerefMut for MetadataWrapper {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         unsafe { self.0.as_mut() }
//     }
// }

// impl Metadata {
//     fn new(quotient_bits: u64, hash_bits: u64, invertable: bool) -> Self {
//         let num_slots: u64 = 1u64 << quotient_bits;
//         let num_real_slots = (num_slots as f64 + 10 as f64 * (num_slots as f64).sqrt()) as u64;
//         let num_blocks = (num_real_slots + SLOTS_PER_BLOCK as u64 - 1) / SLOTS_PER_BLOCK as u64;
//         let remainder_bits = hash_bits - quotient_bits;
//         let invertable = if invertable { 1 } else { 0 };
//         let total_size_bytes = std::mem::size_of::<Metadata>() as u64;
//         Self {
//             total_size_bytes,
//             num_real_slots,
//             num_used_slots: 0,
//             num_unique_keys: 0,
//             num_blocks,
//             quotient_bits,
//             remainder_bits,
//             invertable,
//         }
//     }

//     fn add_size(&mut self, size: u64) {
//         self.total_size_bytes += size;
//     }

//     fn invertable(&self) -> bool {
//         self.invertable == 1
//     }
// }

// struct RuntimeData<H: BuildHasher> {
//     pub file: Option<File>,
//     pub hasher: H,
//     pub max_occupied_slots: u64,
// }

// impl<H: BuildHasher> RuntimeData<H> {
//     fn new(file: Option<File>, hasher: H, num_real_slots: u64) -> Self {
//         Self {
//             file,
//             hasher,
//             max_occupied_slots: ((num_real_slots as f64) * 0.95) as u64,
//         }
//     }
// }

// #[derive(Debug)]
// pub enum CqfError {
//     InvalidArguments,
//     FileError,
//     MmapError,
//     InvalidFile,
//     InvalidSize,
//     Filled,
// }

// pub trait CountingQuotientFilter: IntoIterator + Sized {
//     type Hasher: BuildHasher;
//     type Remainder: Copy + Clone + Default + std::fmt::Debug + Into<u64>;

//     fn new(
//         quotient_bits: u64,
//         hash_bits: u64,
//         invertable: bool,
//         hasher: Self::Hasher,
//     ) -> Result<Self, CqfError>;

//     fn new_file(
//         quotient_bits: u64,
//         hash_bits: u64,
//         invertable: bool,
//         hasher: Self::Hasher,
//         file: File,
//     ) -> Result<Self, CqfError>;

//     fn open_file(hasher: Self::Hasher, file: File) -> Result<Self, CqfError>;

//     // Returns the new count of the item or an error
//     fn insert<Item: Hash>(&mut self, item: Item, count: u64) -> Result<(), CqfError> {
//         let hash = self.calc_hash(item);
//         self.insert_by_hash(hash, count)
//     }

//     // Returns the count and hash of item
//     fn query<Item: Hash>(&self, item: Item) -> (u64, u64) {
//         let hash = self.calc_hash(item);
//         (self.query_by_hash(hash), hash)
//     }

//     fn set_count<Item: Hash>(&mut self, item: Item, count: u64) -> Result<(), CqfError> {
//         if self.occupied_slots() >= self.max_occupied_slots() as u64 {
//             return Err(CqfError::Filled);
//         }
//         let hash = self.calc_hash(item);
//         // self.set_count_by_hash(hash, count)
//         match self.set_count_by_hash(hash, count) {
//             Ok(_) => Ok(()),
//             Err(_) => self.insert_by_hash(hash, count),
//         }
//     }

//     fn quotient_bits(&self) -> u64;

//     fn remainder_bits(&self) -> u64;

//     // fn set_count_cb<Item: Hash, F: FnMut(u64) -> u64>(&mut self, item: Item, count: u64, cb: F) -> Result<u64, CqfError>;

//     // fn iter(&self) -> Self::CqfIterator;

//     fn occupied_slots(&self) -> u64;

//     fn size_bytes(&self) -> u64;

//     fn invertable(&self) -> bool;

//     fn insert_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError>;

//     fn query_by_hash(&self, hash: u64) -> u64;

//     fn set_count_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError>;

//     fn max_occupied_slots(&self) -> u64;

//     fn quotient_remainder_from_hash(&self, hash: u64) -> (u64, Self::Remainder);

//     fn calc_hash<Item: Hash>(&self, item: Item) -> u64;

//     fn merge_insert(
//         &mut self,
//         current_quotient: &mut u64,
//         new_quotient: u64,
//         next_quotient: u64,
//         new_remainder: u64,
//         count: u64,
//     );

//     fn build_hash(&self, quotient: u64, remainder: u64) -> u64;

//     fn is_file(&self) -> bool;

//     fn serialize_to_bytes(&self) -> &[u8];
// }

// // fn set_count_by_hash_cb<F: FnMut(u64) -> u64>(&mut self, hash: u64, count: u64, cb: F) -> Result<u64, CqfError>;
// // fn check_compatibility(a: Self, b: Self) -> bool;

// // fn merge<IterTypeA: CqfIteratorImpl, IterTypeB: CqfIteratorImpl, MergeIntoT: Cqf>(a: IterType, b: IterType) -> Result<Self, CqfError>;

// //     fn merge_cb<IterType: CqfIteratorImpl, T: CqfMergeClosure>(a: IterType, b: IterType, cb: &mut T) -> Result<Self, CqfError>;

// //     fn merge_file_cb<IterType: CqfIteratorImpl, T: CqfMergeClosure>(a: IterType, b: IterType, file: File, cb: &mut T) -> Result<Self, CqfError>;
// // trait CountingQuotientFilterInternal: CountingQuotientFilter {

// // }

// mod u64_cqf;
// pub use u64_cqf::*;
// mod u32_cqf;
// pub use u32_cqf::*;

// pub trait CqfIteratorImpl: Iterator<Item = (u64, u64)> {}

// pub trait CqfMergeClosure: Sized {
//     fn merge_cb<CqfT: CountingQuotientFilter>(
//         &mut self,
//         new_cqf: &mut CqfT,
//         a_quotient: u64,
//         a_remainder: u64,
//         a_count: Option<&mut u64>,
//         b_quotient: u64,
//         b_remainder: u64,
//         b_count: Option<&mut u64>,
//     );
// }

// pub struct CqfMerge();

// impl CqfMerge {
//     pub fn merge<T: CountingQuotientFilter>(
//         mut iter_a: impl CqfIteratorImpl,
//         mut iter_b: impl CqfIteratorImpl,
//         new_cqf: &mut T,
//     ) {
//         let mut current_a = iter_a.next();
//         let mut current_b = iter_b.next();
//         let mut merged_cqf_current_quotient = 0u64;
//         while current_a.is_some() && current_b.is_some() {
//             let insert_quotient: u64;
//             let insert_remainder: u64;
//             let insert_count: u64;
//             let next_quotient_: u64;
//             {
//                 let (a_quotient, a_remainder): (u64, u64);
//                 let (b_quotient, b_remainder): (u64, u64);
//                 let a_count;
//                 let b_count;
//                 {
//                     let a_val = current_a.as_ref().unwrap();
//                     let b_val = current_b.as_ref().unwrap();
//                     let av = new_cqf.quotient_remainder_from_hash(a_val.1);
//                     (a_quotient, a_remainder) = (av.0, av.1.into());
//                     let bv = new_cqf.quotient_remainder_from_hash(b_val.1);
//                     (b_quotient, b_remainder) = (bv.0, bv.1.into());
//                     a_count = a_val.0;
//                     b_count = b_val.0;
//                 }
//                 let a_remainder: u64 = a_remainder.into();
//                 let b_remainder: u64 = b_remainder.into();
//                 if a_quotient == b_quotient && a_remainder == b_remainder {
//                     insert_count = a_count + b_count;
//                     insert_quotient = a_quotient;
//                     insert_remainder = a_remainder;
//                     current_a = iter_a.next();
//                     current_b = iter_b.next();
//                 } else if a_quotient < b_quotient
//                     || (a_quotient == b_quotient && a_remainder < b_remainder)
//                 {
//                     insert_count = a_count;
//                     insert_quotient = a_quotient;
//                     insert_remainder = a_remainder;
//                     current_a = iter_a.next();
//                     // current_b = Some(b_val);
//                 } else {
//                     insert_count = b_count;
//                     insert_quotient = b_quotient;
//                     insert_remainder = b_remainder;
//                     current_b = iter_b.next();
//                 }
//                 next_quotient_ =
//                     Self::next_quotient(new_cqf, &current_a, &current_b, insert_quotient);
//             }
//             new_cqf.merge_insert(
//                 &mut merged_cqf_current_quotient,
//                 insert_quotient,
//                 next_quotient_,
//                 insert_remainder,
//                 insert_count,
//             );
//         }
//         while current_a.is_some() {
//             let insert_quotient: u64;
//             let insert_remainder: u64;
//             let insert_count: u64;
//             let next_quotient_: u64;
//             {
//                 let av = new_cqf.quotient_remainder_from_hash(current_a.as_ref().unwrap().1);
//                 (insert_quotient, insert_remainder) = (av.0, av.1.into());
//                 insert_count = current_a.as_ref().unwrap().0;
//                 current_a = iter_a.next();
//             }
//             next_quotient_ = Self::next_quotient(new_cqf, &current_a, &None, insert_quotient);
//             new_cqf.merge_insert(
//                 &mut merged_cqf_current_quotient,
//                 insert_quotient,
//                 next_quotient_,
//                 insert_remainder,
//                 insert_count,
//             );
//         }
//         while current_b.is_some() {
//             let insert_quotient: u64;
//             let insert_remainder: u64;
//             let insert_count: u64;
//             let next_quotient_: u64;
//             {
//                 let av = new_cqf.quotient_remainder_from_hash(current_b.as_ref().unwrap().1);
//                 (insert_quotient, insert_remainder) = (av.0, av.1.into());
//                 insert_count = current_b.as_ref().unwrap().0;
//                 current_b = iter_b.next();
//             }
//             next_quotient_ = Self::next_quotient(new_cqf, &current_b, &None, insert_quotient);
//             new_cqf.merge_insert(
//                 &mut merged_cqf_current_quotient,
//                 insert_quotient,
//                 next_quotient_,
//                 insert_remainder,
//                 insert_count,
//             );
//         }
//     }

//     pub fn merge_by<T: CountingQuotientFilter>(
//         mut iter_a: impl CqfIteratorImpl,
//         mut iter_b: impl CqfIteratorImpl,
//         new_cqf: &mut T,
//         closure: &mut impl CqfMergeClosure,
//     ) {
//         let mut current_a = iter_a.next();
//         let mut current_b = iter_b.next();
//         let mut merged_cqf_current_quotient = 0u64;
//         while current_a.is_some() && current_b.is_some() {
//             let insert_quotient: u64;
//             let insert_remainder: u64;
//             let insert_count: u64;
//             let next_quotient_: u64;
//             {
//                 let (a_quotient, a_remainder): (u64, u64);
//                 let (b_quotient, b_remainder): (u64, u64);
//                 let mut a_count;
//                 let mut b_count;
//                 {
//                     let a_val = current_a.as_ref().unwrap();
//                     let b_val = current_b.as_ref().unwrap();
//                     let av = new_cqf.quotient_remainder_from_hash(a_val.1);
//                     (a_quotient, a_remainder) = (av.0, av.1.into());
//                     let bv = new_cqf.quotient_remainder_from_hash(b_val.1);
//                     (b_quotient, b_remainder) = (bv.0, bv.1.into());
//                     a_count = a_val.0;
//                     b_count = b_val.0;
//                 }
//                 let a_remainder: u64 = a_remainder.into();
//                 let b_remainder: u64 = b_remainder.into();
//                 closure.merge_cb(
//                     new_cqf,
//                     a_quotient,
//                     a_remainder,
//                     Some(&mut a_count),
//                     b_quotient,
//                     b_remainder,
//                     Some(&mut b_count),
//                 );
//                 if a_quotient == b_quotient && a_remainder == b_remainder {
//                     insert_count = a_count + b_count;
//                     insert_quotient = a_quotient;
//                     insert_remainder = a_remainder;
//                     current_a = iter_a.next();
//                     current_b = iter_b.next();
//                 } else if a_quotient < b_quotient
//                     || (a_quotient == b_quotient && a_remainder < b_remainder)
//                 {
//                     insert_count = a_count;
//                     insert_quotient = a_quotient;
//                     insert_remainder = a_remainder;
//                     current_a = iter_a.next();
//                     // current_b = Some(b_val);
//                 } else {
//                     insert_count = b_count;
//                     insert_quotient = b_quotient;
//                     insert_remainder = b_remainder;
//                     current_b = iter_b.next();
//                 }
//                 next_quotient_ =
//                     Self::next_quotient(new_cqf, &current_a, &current_b, insert_quotient);
//             }
//             new_cqf.merge_insert(
//                 &mut merged_cqf_current_quotient,
//                 insert_quotient,
//                 next_quotient_,
//                 insert_remainder,
//                 insert_count,
//             );
//         }
//         while current_a.is_some() {
//             let insert_quotient: u64;
//             let insert_remainder: u64;
//             let mut insert_count: u64;
//             let next_quotient_: u64;
//             {
//                 let av = new_cqf.quotient_remainder_from_hash(current_a.as_ref().unwrap().1);
//                 (insert_quotient, insert_remainder) = (av.0, av.1.into());
//                 insert_count = current_a.as_ref().unwrap().0;
//                 current_a = iter_a.next();
//             }
//             next_quotient_ = Self::next_quotient(new_cqf, &current_a, &None, insert_quotient);
//             closure.merge_cb(
//                 new_cqf,
//                 insert_quotient,
//                 insert_remainder,
//                 Some(&mut insert_count),
//                 u64::MAX,
//                 u64::MAX,
//                 None,
//             );
//             new_cqf.merge_insert(
//                 &mut merged_cqf_current_quotient,
//                 insert_quotient,
//                 next_quotient_,
//                 insert_remainder,
//                 insert_count,
//             );
//         }
//         while current_b.is_some() {
//             let insert_quotient: u64;
//             let insert_remainder: u64;
//             let mut insert_count: u64;
//             let next_quotient_: u64;
//             {
//                 let av = new_cqf.quotient_remainder_from_hash(current_b.as_ref().unwrap().1);
//                 (insert_quotient, insert_remainder) = (av.0, av.1.into());
//                 insert_count = current_b.as_ref().unwrap().0;
//                 current_b = iter_b.next();
//             }
//             next_quotient_ = Self::next_quotient(new_cqf, &current_b, &None, insert_quotient);
//             closure.merge_cb(
//                 new_cqf,
//                 u64::MAX,
//                 u64::MAX,
//                 None,
//                 insert_quotient,
//                 insert_remainder,
//                 Some(&mut insert_count),
//             );
//             new_cqf.merge_insert(
//                 &mut merged_cqf_current_quotient,
//                 insert_quotient,
//                 next_quotient_,
//                 insert_remainder,
//                 insert_count,
//             );
//         }
//     }

//     fn next_quotient(
//         new_cqf: &impl CountingQuotientFilter,
//         a: &Option<(u64, u64)>,
//         b: &Option<(u64, u64)>,
//         current_quotient: u64,
//     ) -> u64 {
//         match (a, b) {
//             (Some(a_val), Some(b_val)) => {
//                 let a_quotient = new_cqf.quotient_remainder_from_hash(a_val.1).0;
//                 let b_quotient = new_cqf.quotient_remainder_from_hash(b_val.1).0;
//                 if a_quotient < b_quotient {
//                     a_quotient
//                 } else {
//                     b_quotient
//                 }
//             }
//             (Some(a_val), None) => new_cqf.quotient_remainder_from_hash(a_val.1).0,
//             (None, Some(b_val)) => new_cqf.quotient_remainder_from_hash(b_val.1).0,
//             (None, None) => current_quotient - 1,
//         }
//     }
// }

// pub struct ZippedCqfIter<A: CqfIteratorImpl, B: CqfIteratorImpl> {
//     iter_a: A,
//     iter_b: B,
//     current_a: Option<(u64, u64)>,
//     current_b: Option<(u64, u64)>,
// }

// impl<A: CqfIteratorImpl, B: CqfIteratorImpl> ZippedCqfIter<A, B> {
//     pub fn new(mut iter_a: A, mut iter_b: B) -> Self {
//         let current_a = iter_a.next();
//         let current_b = iter_b.next();
//         Self {
//             iter_a,
//             iter_b,
//             current_a,
//             current_b,
//         }
//     }
// }

// impl<A: CqfIteratorImpl, B: CqfIteratorImpl> Iterator for ZippedCqfIter<A, B> {
//     type Item = (Option<(u64, u64)>, Option<(u64, u64)>);
//     fn next(&mut self) -> Option<Self::Item> {
//         match (self.current_a, self.current_b) {
//             (None, None) => None,
//             (Some(a_val), None) => {
//                 self.current_a = self.iter_a.next();
//                 Some((Some(a_val), None))
//             }
//             (None, Some(b_val)) => {
//                 self.current_b = self.iter_b.next();
//                 Some((None, Some(b_val)))
//             }
//             (Some(a_val), Some(b_val)) => {
//                 let a_hash = a_val.1;
//                 let b_hash = b_val.1;
//                 if a_hash < b_hash {
//                     self.current_a = self.iter_a.next();
//                     Some((Some(a_val), None))
//                 } else if a_hash > b_hash {
//                     self.current_b = self.iter_b.next();
//                     Some((None, Some(b_val)))
//                 } else {
//                     self.current_a = self.iter_a.next();
//                     self.current_b = self.iter_b.next();
//                     Some((Some(a_val), Some(b_val)))
//                 }
//             }
//         }
//     }
// }
