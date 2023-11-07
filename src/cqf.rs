use std::{
    fs::File,
    hash::{BuildHasher, Hash},
};
use crate::SLOTS_PER_BLOCK;

struct MetadataWrapper(std::ptr::Unique<Metadata>);

impl MetadataWrapper {
    pub fn new(metadata: *mut Metadata) -> Self {
        let inner = unsafe { std::ptr::Unique::new_unchecked(metadata) };
        Self(inner)
    }
}

#[repr(C)]
struct Metadata {
    pub total_size_bytes: u64,
    pub num_real_slots: u64,
    pub num_occupied_slots: u64,
    pub num_blocks: u64,
    pub quotient_bits: u64,
    pub remainder_bits: u64,
    pub invertable: u64,
}

impl std::ops::Deref for MetadataWrapper {
    type Target = Metadata;
    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl std::ops::DerefMut for MetadataWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Metadata {
    fn new(quotient_bits: u64, hash_bits: u64, invertable: bool) -> Self {
        let num_slots: u64 = 1 << quotient_bits;
        let num_real_slots = (num_slots as f64 + 10 as f64 * (num_slots as f64).sqrt()) as u64;
        let num_blocks = (num_real_slots + SLOTS_PER_BLOCK as u64 - 1) / SLOTS_PER_BLOCK as u64;
        let remainder_bits = hash_bits - quotient_bits;
        let invertable = if invertable { 1 } else { 0 };
        let total_size_bytes = std::mem::size_of::<Metadata>() as u64;
        Self {
            total_size_bytes,
            num_real_slots,
            num_occupied_slots: 0,
            num_blocks,
            quotient_bits,
            remainder_bits,
            invertable,
        }
    }

    fn add_size(&mut self, size: u64) {
        self.total_size_bytes += size;
    }

    fn invertable(&self) -> bool {
        self.invertable == 1
    }
}

struct RuntimeData<H: BuildHasher> {
    pub file: Option<File>,
    pub hasher: H,
    pub max_occupied_slots: u64,
}

impl<H: BuildHasher> RuntimeData<H> {
    fn new(file: Option<File>, hasher: H, num_real_slots: u64) -> Self {
        Self {
            file,
            hasher,
            max_occupied_slots: ((num_real_slots as f64) * 0.95) as u64,
        }
    }
}


#[derive(Debug)]
pub enum CqfError {
    InvalidArguments,
    FileError,
    MmapError,
    InvalidFile,
    InvalidSize,
    Filled,
}

pub trait CountingQuotientFilter: IntoIterator + Sized {
    type Hasher: BuildHasher;
    type Remainder: Copy + Clone + Default + std::fmt::Debug;

    fn new(
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        hasher: Self::Hasher,
    ) -> Result<Self, CqfError>;

    fn new_file(
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        hasher: Self::Hasher,
        file: File,
    ) -> Result<Self, CqfError>;

    fn open_file(hasher: Self::Hasher, file: File) -> Result<Self, CqfError>;

    // Returns the new count of the item or an error
    fn insert<Item: Hash>(&mut self, item: Item, count: u64) -> Result<(), CqfError> {
        let hash = self.calc_hash(item);
        self.insert_by_hash(hash, count)
    }

    // Returns the count and hash of item
    fn query<Item: Hash>(&self, item: Item) -> (u64, u64) {
        let hash = self.calc_hash(item);
        (self.query_by_hash(hash), hash)
    }

    fn set_count<Item: Hash>(&mut self, item: Item, count: u64) -> Result<(), CqfError> {
        if self.occupied_slots() >= self.max_occupied_slots() as u64 {
            return Err(CqfError::Filled);
        }
        let hash = self.calc_hash(item);
        // self.set_count_by_hash(hash, count)
        match self.set_count_by_hash(hash, count) {
            Ok(_) => Ok(()),
            Err(_) => self.insert_by_hash(hash, count),
        }
    }

    fn quotient_bits(&self) -> u64;

    fn remainder_bits(&self) -> u64;

    // fn set_count_cb<Item: Hash, F: FnMut(u64) -> u64>(&mut self, item: Item, count: u64, cb: F) -> Result<u64, CqfError>;

    // fn iter(&self) -> Self::CqfIterator;

    fn occupied_slots(&self) -> u64;

    fn size_bytes(&self) -> u64;

    fn invertable(&self) -> bool;

    fn insert_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError>;

    fn query_by_hash(&self, hash: u64) -> u64;

    fn set_count_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError>;

    fn max_occupied_slots(&self) -> u64;

    fn quotient_remainder_from_hash(&self, hash: u64) -> (u64, Self::Remainder);

    fn calc_hash<Item: Hash>(&self, item: Item) -> u64;

    fn merge_insert(
        &mut self,
        current_quotient: &mut u64,
        new_quotient: u64,
        next_quotient: u64,
        new_remainder: u64,
        count: u64,
    );

    fn build_hash(&self, quotient: u64, remainder: u64) -> u64;

    fn is_file(&self) -> bool;
    
}

// fn set_count_by_hash_cb<F: FnMut(u64) -> u64>(&mut self, hash: u64, count: u64, cb: F) -> Result<u64, CqfError>;
// fn check_compatibility(a: Self, b: Self) -> bool;

// fn merge<IterTypeA: CqfIteratorImpl, IterTypeB: CqfIteratorImpl, MergeIntoT: Cqf>(a: IterType, b: IterType) -> Result<Self, CqfError>;

//     fn merge_cb<IterType: CqfIteratorImpl, T: CqfMergeClosure>(a: IterType, b: IterType, cb: &mut T) -> Result<Self, CqfError>;

//     fn merge_file_cb<IterType: CqfIteratorImpl, T: CqfMergeClosure>(a: IterType, b: IterType, file: File, cb: &mut T) -> Result<Self, CqfError>;
// trait CountingQuotientFilterInternal: CountingQuotientFilter {

// }


pub trait CqfIteratorImpl: Iterator {}

pub trait CqfMergeClosure: Sized {
    fn merge_cb<'a, CqfT: CountingQuotientFilter>(
        &mut self,
        new_cqf: &mut CqfT,
        a_quotient: u64,
        a_remainder: u64,
        a_count: &mut u64,
        b_quotient: u64,
        b_remainder: u64,
        b_count: &mut u64,
    );
}



mod u64_cqf;
pub use u64_cqf::*;
mod u32_cqf;
pub use u32_cqf::*;


