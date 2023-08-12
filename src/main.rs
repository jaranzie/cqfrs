// use cqfrs::{CountingQuotientFilter, HashMode};
// use rand::Rng;
// use rayon::prelude::*;
// use std::sync::{atomic::AtomicI32, Arc};
// use std::time::Instant;

// const LOGN_SLOTS: u64 = 26;

// fn main() {
//     // let qf = CountingQuotientFilter::new(LOGN_SLOTS, LOGN_SLOTS, HashMode::Invertible).unwrap();

//     // let n_strings: usize = ((1 << LOGN_SLOTS) as f32 * 0.9) as usize;
//     // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

//     // let mut rng = rand::thread_rng();
//     // for _ in 0..n_strings {
//     //     numbers.push(rng.gen())
//     // }

//     // let now = Instant::now();
//     // for i in 0..n_strings {
//     //     //qf.insert(strings[i].as_bytes(), 3)?;
//     //     qf.insert(numbers[i] as u64, 1).expect("insert failed!");
//     // }

//     ///////////////////////////////////////
//     // let num_threads = 6;
//     // let numbers = Arc::new(numbers);
//     // let qf = Arc::new(qf);

//     // let mut handles = Vec::with_capacity(num_threads);

//     // let now = Instant::now();
//     // for i in 0..num_threads {
//     //     let qf = qf.clone();
//     //     let numbers = numbers.clone();
//     //     handles.push(std::thread::spawn(move || {
//     //         for j in (i*n_strings/num_threads)..((i+1)*n_strings/num_threads) {
//     //             // println!("inserting {}", numbers[j]);
//     //             qf.insert(numbers[j] as u64, 1).expect("insert failed!");
//     //         }
//     //     }));
//     // }

//     // for handle in handles {
//     //     handle.join().unwrap();
//     // }
//     ////////////////////////////////////////////////
//     // let inserts = Arc::new(AtomicI32::new(0));
//     // numbers.par_iter().for_each(|&i| {
//     //     qf.insert(i as u64, 1).expect("insert failed!");
//     //     // inserts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
//     // });

//     // println!("inserted {} elements", inserts.load(std::sync::atomic::Ordering::SeqCst));
//     // println!("{n_strings}");
//     // for i in 0..n_strings {
//     //     //qf.insert(strings[i].as_bytes(), 3)?;
//     //     qf.insert(numbers[i] as u64, 1).expect("insert failed!");
//     // }

//     // let elapsed = now.elapsed();
//     // println!("insert took {} seconds!", elapsed.as_millis());

//     // let now = Instant::now();

//     // let qf = Arc::new(qf);

//     // let elapsed = now.elapsed();
//     // println!("insert took {} seconds!", elapsed.as_secs());

//     ///////////////////////////////////////////////////////////////////////

//     let mut qf =
//         CountingQuotientFilter::new(25, 25, HashMode::Invertible).expect("failed to make cqf");

//     let qf = CountingQuotientFilter::new_file(25, 25, HashMode::Invertible, "test.qf".into())
//         .expect("failed to make cqf");
//     // let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
//     let n_strings: usize = 10_000_000;

//     let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);
//     let mut rng = rand::thread_rng();

//     for _ in 0..n_strings {
//         numbers.push(rng.gen())
//     }

//     for i in 0..n_strings / 2 {
//         //qf.insert(strings[i].as_bytes(), 3)?;
//         match qf.insert(numbers[i], 3) {
//             Err(_) => eprintln!("Error inserting"),
//             _ => continue,
//         };
//     }
//     let mut total_in = 0;
//     for i in 0..n_strings / 2 {
//         //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
//         let p = qf.query(numbers[i]);
//         total_in += p;
//         assert!(qf.query(numbers[i]) > 0, "false negative!");
//     }

//     let mut present: u32 = 0;
//     for i in n_strings / 2..n_strings {
//         /*
//         if qf.query(strings[i].as_bytes()) > 0 {
//             present += 1;
//         }
//         */
//         if qf.query(numbers[i]) > 0 {
//             present += 1;
//         }
//     }
//     assert_eq!(present, 0);
//     println!("{} elements present", total_in);

//     drop(qf);

//     // MMAP LOAD TEST

//     let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
//     let n_strings: usize = 10_000_000;

//     // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);
//     // let mut rng = rand::thread_rng();

//     // for _ in 0..n_strings {
//     //     numbers.push(rng.gen())
//     // }

//     let mut total_in = 0;
//     for i in 0..n_strings / 2 {
//         //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
//         let p = qf.query(numbers[i]);
//         total_in += p;
//         assert!(qf.query(numbers[i]) > 0, "false negative!");
//     }

//     let mut present: u32 = 0;
//     for i in n_strings / 2..n_strings {
//         /*
//         if qf.query(strings[i].as_bytes()) > 0 {
//             present += 1;
//         }
//         */
//         if qf.query(numbers[i]) > 0 {
//             present += 1;
//         }
//     }
//     assert_eq!(present, 0);
//     println!("{} elements present", total_in);

//     // let qf = CountingQuotientFilter::new_file(25,25,HashMode::Invertible, "test.qf".into()).expect("failed to make cqf");
// }

use cqfrs::{CountingQuotientFilter, HashMode};
use rand::Rng;
use rayon::prelude::*;
use std::fs;
use std::sync::{atomic::AtomicI32, Arc};
use std::time::Instant;

const LOGN_SLOTS: u64 = 23;
use xxhash_rust::xxh3::xxh3_64;
fn main() {
    // {
        if fs::metadata("test.qf").is_ok() {
            fs::remove_file("test.qf").expect("failed to remove file");
        }
        {
            let qf = CountingQuotientFilter::new_file(LOGN_SLOTS, LOGN_SLOTS, HashMode::Invertible, "test.qf".into()).unwrap();
        }
        let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
    // }
    // {
    //     let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
    // } 
        // qf.clear();
    let n_strings: usize = ((1 << (LOGN_SLOTS)) as f32 * 0.95) as usize;
    let mut numbers: Vec<u64> = (0..n_strings as u64).map(|n| xxh3_64(&n.to_le_bytes())).collect();

    /* let mut rng = rand::thread_rng();
    for _ in 0..n_strings {
        numbers.push(rng.gen())
    } */

    // numbers = (0..n_strings as u64).map(|n| xxh3_64(&n.to_le_bytes())).collect();
    numbers.sort();

    println!("inserting now...");
    // let now = Instant::now();
    for i in 0..n_strings {
        //qf.insert(strings[i].as_bytes(), 3)?;
        // println!("inserting {}", numbers[i]);
        // println!("inserting {}", numbers[i]);
        qf.insert_by_hash(numbers[i], 1).expect("insert failed!");
    }

    println!("{}", qf.query_by_hash(numbers[0]));
    
    // let qf = CountingQuotientFilter::new(LOGN_SLOTS, LOGN_SLOTS, HashMode::Invertible).unwrap();
    // qf.clear();
    // println!("cleared");
    // let n_strings: usize = ((1 << (LOGN_SLOTS)) as f32 * 0.9) as usize;
    // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

    // let mut rng = rand::thread_rng();
    // for _ in 0..n_strings {
    //     numbers.push(rng.gen())
    // }

    // numbers.clone().into_par_iter().for_each(|n| {
    //     qf.insert(n, 1).expect("insert failed!");
    // });

    // let now = Instant::now();
    // for i in 0..n_strings {
    //     //qf.insert(strings[i].as_bytes(), 3)?;
    //     // println!("inserting {}", numbers[i]);
    //     qf.insert((numbers[i]) as u64, 1).expect("insert failed!");
    // }
    // let mut item_count = AtomicI32::new(0);

    // qf.set_count(numbers[0], 100).expect("Failed to set count");
    // println!("count is {}", qf.query(numbers[0]));
    // qf.set_count(numbers[0], 300).expect("Failed to set count");
    // println!("count is {}", qf.query(numbers[0]));
    // qf.set_count(numbers[0], 2).expect("Failed to set count");
    // println!("count is {}", qf.query(numbers[0]));

    // Iterator::for_each(qf.into_iter(), |item| {
    //     item_count.fetch_add(item.count as i32, std::sync::atomic::Ordering::SeqCst);
    // });
    
    // let now = Instant::now();
    // println!("num threads {}", rayon::current_num_threads());
    // rayon::iter::ParallelIterator::for_each(qf.into_par_iter(), |item| {
    //     item_count.fetch_add(item.count as i32, std::sync::atomic::Ordering::SeqCst);
    // });
    // qf.into_par_iter().for_each(|item| {
    //     item_count.fetch_add(item.count as i32, std::sync::atomic::Ordering::SeqCst);
    // });
    // println!("num threads {}", rayon::current_num_threads());
    // println!("Time to iterate: {:?}", now.elapsed());

    // let mut unique_strings = std::collections::HashSet::new();
    // for i in 0..n_strings {
    //     unique_strings.insert(numbers[i]);
    // }   

    // println!("Item Count {}, should be {}",item_count.load(std::sync::atomic::Ordering::SeqCst), unique_strings.len());

    // for i in 0..n_strings {
    //     //qf.insert(strings[i].as_bytes(), 3)?;
    //     // println!("inserting {}", numbers[i]);
    //     assert!(qf.query((numbers[i]) as u64) > 0);
    // }

    // qf.print();
    // NOTE offsets may be off by one ??
    ///////////////////////////////////////
    // let num_threads = 6;
    // let numbers = Arc::new(numbers);
    // let qf = Arc::new(qf);

    // let mut handles = Vec::with_capacity(num_threads);

    // let now = Instant::now();
    // for i in 0..num_threads {
    //     let qf = qf.clone();
    //     let numbers = numbers.clone();
    //     handles.push(std::thread::spawn(move || {
    //         for j in (i*n_strings/num_threads)..((i+1)*n_strings/num_threads) {
    //             // println!("inserting {}", numbers[j]);
    //             qf.insert(numbers[j] as u64, 1).expect("insert failed!");
    //         }
    //     }));
    // }

    // for handle in handles {
    //     handle.join().unwrap();
    // }
    ////////////////////////////////////////////////
    // let inserts = Arc::new(AtomicI32::new(0));
    // numbers.par_iter().for_each(|&i| {
    //     qf.insert(i as u64, 1).expect("insert failed!");
    //     // inserts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    // });

    // println!("inserted {} elements", inserts.load(std::sync::atomic::Ordering::SeqCst));
    // println!("{n_strings}");
    // for i in 0..n_strings {
    //     //qf.insert(strings[i].as_bytes(), 3)?;
    //     qf.insert(numbers[i] as u64, 1).expect("insert failed!");
    // }

    // let elapsed = now.elapsed();
    // println!("insert took {} seconds!", elapsed.as_millis());

    // let now = Instant::now();

    // let qf = Arc::new(qf);

    // let elapsed = now.elapsed();
    // println!("insert took {} seconds!", elapsed.as_secs());

    ///////////////////////////////////////////////////////////////////////

    // let mut qf =
    //     CountingQuotientFilter::new(25, 25, HashMode::Invertible).expect("failed to make cqf");

    // let qf = CountingQuotientFilter::new_file(25, 25, HashMode::Invertible, "test.qf".into())
    //     .expect("failed to make cqf");
    // // let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
    // let n_strings: usize = 10_000_000;

    // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);
    // let mut rng = rand::thread_rng();

    // for _ in 0..n_strings {
    //     numbers.push(rng.gen())
    // }

    // for i in 0..n_strings / 2 {
    //     //qf.insert(strings[i].as_bytes(), 3)?;
    //     match qf.insert(numbers[i], 3) {
    //         Err(_) => eprintln!("Error inserting"),
    //         _ => continue,
    //     };
    // }
    // let mut total_in = 0;
    // for i in 0..n_strings {
    //     //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
    //     let p = qf.query(numbers[i]);
    //     total_in += p;
    //     assert!(qf.query(numbers[i]) > 0, "false negative!");
    // }
    // println!("{} elements present", total_in);

    // let mut present: u32 = 0;
    // for i in n_strings / 2..n_strings {
    //     /*
    //     if qf.query(strings[i].as_bytes()) > 0 {
    //         present += 1;
    //     }
    //     */
    //     if qf.query(numbers[i]) > 0 {
    //         present += 1;
    //     }
    // }
    // assert_eq!(present, 0);
    // println!("{} elements present", total_in);

    // drop(qf);

    // // MMAP LOAD TEST

    // let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
    // let n_strings: usize = 10_000_000;

    // // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);
    // // let mut rng = rand::thread_rng();

    // // for _ in 0..n_strings {
    // //     numbers.push(rng.gen())
    // // }

    // let mut total_in = 0;
    // for i in 0..n_strings / 2 {
    //     //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
    //     let p = qf.query(numbers[i]);
    //     total_in += p;
    //     assert!(qf.query(numbers[i]) > 0, "false negative!");
    // }

    // let mut present: u32 = 0;
    // for i in n_strings / 2..n_strings {
    //     /*
    //     if qf.query(strings[i].as_bytes()) > 0 {
    //         present += 1;
    //     }
    //     */
    //     if qf.query(numbers[i]) > 0 {
    //         present += 1;
    //     }
    // }
    // assert_eq!(present, 0);
    // println!("{} elements present", total_in);

    // let qf = CountingQuotientFilter::new_file(25,25,HashMode::Invertible, "test.qf".into()).expect("failed to make cqf");
}
