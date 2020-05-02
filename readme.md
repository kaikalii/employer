### Description

This crate provides an interface for spawning worker threads and checking
if their work has finished and produced a result.

### When should you use this?

This crate is designed for applications where you need to do some expensive
task, but the main thread can carry on just fine without that task's result.
The most common domain for this type of problem is in asset loading. This crate
allows you to start loading assets and then render/play/use them when they are
finished loading.

### Example
```rust
use std::{thread, time::Duration};
use employe::*;

fn main () {
    // Create a new `Employer`
    // We give it a work function that will be run on each input
    let employer = Employer::new(|i: i32| {
        // Sleep to simulate a more complex computation
        thread::sleep(Duration::from_millis(100));
        2 * i + 1
    });

    // Start some jobs
    employer.start(1);
    employer.start(2);
    employer.start(3);

    // Each job should take about 100 ms, so if we check them
    // immediately, they should still all be in progress
    assert!(employer.get(&1).is_in_progress());
    assert!(employer.get(&2).is_in_progress());
    assert!(employer.get(&3).is_in_progress());

    // Sleep the main thread to let the jobs finish
    thread::sleep(Duration::from_millis(200));

    // Check the results
    assert_eq!(employer.get(&1).finished().unwrap(), 3);
    assert_eq!(employer.get(&2).finished().unwrap(), 5);
    assert_eq!(employer.get(&3).finished().unwrap(), 7);
}
```
