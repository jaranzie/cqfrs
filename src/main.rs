use cqfrs::{CountingQuotientFilter, HashMode};
use rand::Rng;
use rayon::prelude::*;
use std::sync::{atomic::AtomicI32, Arc};
use std::time::Instant;

const LOGN_SLOTS: u64 = 26;

fn main() {
    let qf = CountingQuotientFilter::new(LOGN_SLOTS, LOGN_SLOTS, HashMode::Invertible).unwrap();

    let n_strings: usize = ((1 << LOGN_SLOTS) as f32 * 0.9) as usize;
    let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

    let mut rng = rand::thread_rng();
    for _ in 0..n_strings {
        numbers.push(rng.gen())
    }

    let now = Instant::now();
    // for i in 0..n_strings {
    //     //qf.insert(strings[i].as_bytes(), 3)?;
    //     qf.insert(numbers[i] as u64, 1).expect("insert failed!");
    // }

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
    numbers.par_iter().for_each(|&i| {
        qf.insert(i as u64, 1).expect("insert failed!");
        // inserts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    });

    // println!("inserted {} elements", inserts.load(std::sync::atomic::Ordering::SeqCst));
    // println!("{n_strings}");
    // for i in 0..n_strings {
    //     //qf.insert(strings[i].as_bytes(), 3)?;
    //     qf.insert(numbers[i] as u64, 1).expect("insert failed!");
    // }

    let elapsed = now.elapsed();
    println!("insert took {} seconds!", elapsed.as_millis());

    // let now = Instant::now();

    // let qf = Arc::new(qf);

    // let elapsed = now.elapsed();
    // println!("insert took {} seconds!", elapsed.as_secs());

    ///////////////////////////////////////////////////////////////////////

    // let mut qf = CountingQuotientFilter::new(25,25,HashMode::Invertible).expect("failed to make cqf");

    // let thread_count = 6;
    // let n_strings: usize = 10_000_000;

    // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);
    // let mut rng = rand::thread_rng();

    // for _ in 0..n_strings {
    //     numbers.push(rng.gen())
    // }

    // for i in 0..n_strings/2 {
    //     //qf.insert(strings[i].as_bytes(), 3)?;
    //     match qf.insert(numbers[i], 3) {
    //         Err(_) => eprintln!("Error inserting"),
    //         _ => continue
    //     };
    // }

    // for i in 0..n_strings/2 {
    //     //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
    //     assert!(qf.query(numbers[i]) > 0, "false negative!");
    // }

    // let mut present: u32 = 0;
    // for i in n_strings/2..n_strings {
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
}
