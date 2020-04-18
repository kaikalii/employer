### Description

This crate provides an interface for "outsourcing" work to other threads and checking
if the work has finished and produced a result.

### Example
```rust
use std::{thread, time::Duration};
use outsource::*;

fn main () {
    // Create a new `Outsourcer`
    // We give it a work function that will be run on each input
    let outsourcer = Outsourcer::new(|i: i32| {
        // Sleep to simulate a more complex computation
        thread::sleep(Duration::from_millis(100));
        2 * i + 1
    });

    // Start some jobs
    outsourcer.start(1);
    outsourcer.start(2);
    outsourcer.start(3);

    // Each job should take about 100 ms, so if we check them
    // immediately, they should still all be in progress
    assert!(outsourcer.get(&1).is_in_progress());
    assert!(outsourcer.get(&2).is_in_progress());
    assert!(outsourcer.get(&3).is_in_progress());

    // Sleep the main thread to let the jobs finish
    thread::sleep(Duration::from_millis(200));

    // Check the results
    assert_eq!(outsourcer.get(&1).finished().unwrap(), 3);
    assert_eq!(outsourcer.get(&2).finished().unwrap(), 5);
    assert_eq!(outsourcer.get(&3).finished().unwrap(), 7);
}
```
