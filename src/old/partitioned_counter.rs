// use crossbeam::utils::CachePadded;
// use libc::{sched_getcpu, sysconf, _SC_NPROCESSORS_ONLN};
// use std::sync::atomic::AtomicI64;
// use std::sync::atomic::Ordering;
// use std::sync::Arc;

// #[derive(Default)]
// #[repr(align(128))]
// pub struct LocalCounter {
//     counter: AtomicI64,
// }

// pub struct PartitionedCounter {
//     pub local_counters: Box<[LocalCounter]>,
//     pub global_counter: *const AtomicI64,
//     pub num_counters: u32,
//     pub threshold: i32,
// }

// impl PartitionedCounter {
//     pub fn new(global_counter: *const AtomicI64, num_counters: u32, threshold: i32) -> Self {
//         let num_cpus = unsafe { sysconf(_SC_NPROCESSORS_ONLN) };
//         let num_counters = num_counters.min(num_cpus as u32);
//         // println!("{num_cpus} num_counters: {}", num_counters);
//         let mut local_counters: Vec<LocalCounter> = Vec::with_capacity(num_counters as usize);
//         for _ in 0..num_counters {
//             local_counters.push(LocalCounter {
//                 counter: AtomicI64::new(0),
//                 // padding: [0; 7],
//             })
//         }

//         PartitionedCounter {
//             local_counters: local_counters.into_boxed_slice(),
//             global_counter,
//             num_counters,
//             threshold,
//         }
//     }

//     pub fn sync(&self) {
//         for index in 0..self.num_counters as usize {
//             // may need to make localcounter counter atomic
//             let c = self.local_counters[index].counter.swap(0, Ordering::SeqCst);
//             (unsafe { &*self.global_counter }).fetch_add(c, Ordering::SeqCst);
//         }
//     }

//     pub fn add(&self, count: i64) {
//         let cpu_id = unsafe { sched_getcpu() };
//         // println!("cpu_id: {}", cpu_id);
//         let counter_index = cpu_id as usize % self.num_counters as usize;
//         let current_count = self.local_counters[counter_index]
//             .counter
//             .fetch_add(count, Ordering::SeqCst);
//         if current_count > self.threshold as i64 || current_count < -self.threshold as i64 {
//             let new_count = self.local_counters[counter_index]
//                 .counter
//                 .swap(0, Ordering::SeqCst);
//             (unsafe { &*self.global_counter }).fetch_add(new_count, Ordering::SeqCst);
//         }
//     }
// }

// impl Drop for PartitionedCounter {
//     fn drop(&mut self) {
//         self.sync();
//         // drop(self.local_counters);
//     }
// }
