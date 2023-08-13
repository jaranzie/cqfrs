
use libc::{
    madvise, mmap, msync, munmap, MAP_ANONYMOUS, MAP_FAILED, MAP_PRIVATE, MAP_SHARED, PROT_EXEC,
    PROT_READ, PROT_WRITE,
};
use std::fs::OpenOptions;
use std::ops::DerefMut;
use std::os::fd::AsRawFd;
use std::ptr::Unique;
use std::sync::OnceLock;
use std::{ops::Deref, path::PathBuf};

static PAGE_SIZE: OnceLock<usize> = OnceLock::new();

/// T should by repr C
pub struct MmapSlice<T> {
    ptr: Unique<T>,
    len: usize,
}

impl<T> MmapSlice<T>
where
    T: Copy,
{
    pub fn new(len: usize) -> Result<Self, std::io::Error> {
        let _ = PAGE_SIZE.set(unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize });
        let ptr = unsafe {
            mmap(
                std::ptr::null_mut(),
                len * std::mem::size_of::<T>(),
                PROT_READ | PROT_WRITE | PROT_EXEC,
                MAP_PRIVATE | MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == MAP_FAILED {
            return Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory));
        }
        let ptr = Unique::new(ptr as *mut T).unwrap();
        Ok(Self { ptr, len })
    }

    pub fn new_file(len: usize, file_name: PathBuf) -> Result<Self, std::io::Error> {
        let _ = PAGE_SIZE.set(unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize });
        if file_name.exists() {
            return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)?;
        file.set_len((len * std::mem::size_of::<T>()) as u64)?;
        let ptr = unsafe {
            mmap(
                std::ptr::null_mut(),
                len * std::mem::size_of::<T>(),
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        drop(file);
        if ptr == MAP_FAILED {
            return Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory));
        }
        let ptr = Unique::new(ptr as *mut T).unwrap();
        Ok(Self { ptr, len })
    }

    pub fn open_file(len: usize, file_name: PathBuf) -> Result<Self, std::io::Error> {
        let _ = PAGE_SIZE.set(unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize });
        if !file_name.exists() {
            return Err(std::io::Error::from(std::io::ErrorKind::NotFound));
        }
        let file = OpenOptions::new().read(true).write(true).open(file_name)?;
        let ptr = unsafe {
            mmap(
                std::ptr::null_mut(),
                len * std::mem::size_of::<T>(),
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        drop(file);
        if ptr == MAP_FAILED {
            return Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory));
        }
        let ptr = Unique::new(ptr as *mut T).unwrap();
        Ok(Self { ptr, len })
    }

    pub fn open_file_read_only(len: usize, file_name: PathBuf) -> Result<Self, std::io::Error> {
        let _ = PAGE_SIZE.set(unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize });
        if !file_name.exists() {
            return Err(std::io::Error::from(std::io::ErrorKind::NotFound));
        }
        let file = OpenOptions::new().read(true).write(true).open(file_name)?;
        let ptr = unsafe {
            mmap(
                std::ptr::null_mut(),
                len * std::mem::size_of::<T>(),
                PROT_READ,
                MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        drop(file);
        if ptr == MAP_FAILED {
            return Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory));
        }
        let ptr = Unique::new(ptr as *mut T).unwrap();
        Ok(Self { ptr, len })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr()
    }

    pub fn sync(&self) {
        unsafe {
            msync(
                self.ptr.as_ptr() as *mut libc::c_void,
                self.len * std::mem::size_of::<T>(),
                libc::MS_SYNC,
            )
        };
    }

    pub fn advise_seq(&self) {
        unsafe {
            madvise(
                self.ptr.as_ptr() as *mut libc::c_void,
                self.len * std::mem::size_of::<T>(),
                libc::MADV_SEQUENTIAL,
            )
        };
    }
    pub fn advise_random(&self) {
        unsafe {
            madvise(
                self.ptr.as_ptr() as *mut libc::c_void,
                self.len * std::mem::size_of::<T>(),
                libc::MADV_RANDOM,
            )
        };
    }

    pub fn advise_need(&self, index: usize, len: usize) {
        unsafe {
            madvise(
                self.ptr.as_ptr().offset(index as isize) as *mut libc::c_void,
                len * std::mem::size_of::<T>(),
                libc::MADV_WILLNEED,
            )
        };
    }

    pub fn advise_dont_need(&self, index: usize, len: usize) {
        unsafe {
            madvise(
                self.ptr.as_ptr().offset(index as isize) as *mut libc::c_void,
                len * std::mem::size_of::<T>(),
                libc::MADV_DONTNEED,
            )
        };
    }
}

impl<T> Deref for MmapSlice<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl<T> DerefMut for MmapSlice<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl<T> Drop for MmapSlice<T> {
    fn drop(&mut self) {
        unsafe {
            munmap(
                self.ptr.as_ptr() as *mut libc::c_void,
                self.len * std::mem::size_of::<T>(),
            )
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_mmap_slice() {
        let mut mmap_slice = MmapSlice::<u8>::new(10).unwrap();
        for i in 0..10 {
            mmap_slice[i] = i as u8;
        }
        for i in 0..10 {
            assert_eq!(mmap_slice[i], i as u8);
        }
    }

    #[test]
    fn test_mmap_struct_slice() {
        #[repr(C)]
        #[derive(Copy, Clone)]
        struct Test {
            a: u8,
            b: u8,
            c: u8,
        }
        let mut mmap_slice = MmapSlice::<Test>::new(10).unwrap();
        for i in 0..10 {
            mmap_slice[i].a = i as u8;
            mmap_slice[i].b = i as u8;
            mmap_slice[i].c = i as u8;
        }
    }

    #[test]
    #[should_panic]
    fn test_bounds_check() {
        let mmap_slice = MmapSlice::<u8>::new(10).unwrap();
        mmap_slice[10];
    }
}
