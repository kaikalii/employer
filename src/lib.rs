#![deny(missing_docs)]

/*!
### Description

This crate provides an interface for "outsourcing" work to other threads and checking
if the work has finished and produced a result.

The primary struct is the [`Outsourcer`](struct.Outsourcer.html), which can be used to
start new jobs and also holds the results of finished jobs.

### Example
```
use std::{thread, time::Duration};
use outsource::*;

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
```
*/

use std::{
    borrow::Borrow,
    cmp::Ordering as CmpOrdering,
    fmt,
    hash::Hash,
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    },
    thread,
};

use lockfree::{
    map::{Map, ReadGuard},
    set::Set,
};

/// The status of a job
pub enum Job<'a, K, V> {
    /// No job exists with the given input
    None,
    /// The job is in progress
    InProgress,
    /// The job has finished
    Finished(Guard<'a, K, V>),
}

impl<'a, K, V> Job<'a, K, V> {
    /// Check if the `Job` is finished
    pub fn is_finished(&self) -> bool {
        matches!(self, Job::Finished(_))
    }
    /// Check if the `Job` is in progress
    pub fn is_in_progress(&self) -> bool {
        matches!(self, Job::InProgress)
    }
    /// Check if the `Job` exists
    pub fn exists(&self) -> bool {
        !matches!(self, Job::None)
    }
    /// Get a reference to the result of the `Job` if is is finished
    pub fn as_finished(&self) -> Option<&Guard<'a, K, V>> {
        if let Job::Finished(job) = self {
            Some(job)
        } else {
            None
        }
    }
    /// Convert the `Job` to an `Option<Guard>`
    pub fn finished(self) -> Option<Guard<'a, K, V>> {
        if let Job::Finished(job) = self {
            Some(job)
        } else {
            None
        }
    }
}

impl<'a, K, V> From<Job<'a, K, V>> for Option<Guard<'a, K, V>> {
    fn from(job: Job<'a, K, V>) -> Self {
        job.finished()
    }
}

/// An interface for outsourcing work to other threads
pub struct Outsourcer<K, V, F> {
    set: Arc<Set<K>>,
    map: Arc<Map<K, V>>,
    f: Arc<F>,
    pending: Arc<AtomicUsize>,
}

impl<K, V, F> Outsourcer<K, V, F> {
    /// Create a new `Outsourcer` with the given function
    pub fn new(f: F) -> Self {
        Outsourcer {
            set: Arc::new(Set::new()),
            map: Arc::new(Map::new()),
            f: Arc::new(f),
            pending: Arc::new(AtomicUsize::new(0)),
        }
    }
    /// Get the job with the given input
    pub fn get<'a, Q>(&'a self, input: &Q) -> Job<'a, K, V>
    where
        Q: Hash + Ord,
        K: Borrow<Q>,
    {
        if self.set.contains(input) {
            Job::InProgress
        } else if let Some(rg) = self.map.get(input) {
            Job::Finished(Guard(rg))
        } else {
            Job::None
        }
    }
    /// Check if the job with the given input has finished
    pub fn finished<Q>(&self, input: &Q) -> bool
    where
        Q: Hash + Ord,
        K: Borrow<Q>,
    {
        self.get(input).is_finished()
    }
    /// Check the number of jobs that are in progress
    pub fn in_progress_len(&self) -> usize {
        self.pending.load(AtomicOrdering::Relaxed)
    }
}

impl<K, V, F> Outsourcer<K, V, F>
where
    K: Ord + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    F: Fn(K) -> V + Send + Sync + 'static,
{
    fn _start(&self, input: K) {
        let _ = self.set.insert(input.clone());
        self.pending.fetch_add(1, AtomicOrdering::Relaxed);
        let map = Arc::clone(&self.map);
        let set = Arc::clone(&self.set);
        let f = Arc::clone(&self.f);
        let pending = Arc::clone(&self.pending);
        thread::spawn(move || {
            let res = f(input.clone());
            set.remove(&input);
            pending.fetch_sub(1, AtomicOrdering::Relaxed);
            map.insert(input, res);
        });
    }
    /**
    Start a new job with the the given input if one with the same
    input is not in progress or finished

    If you want to start the job even if one with the same input
    is already finished, use
    [`Outsourcer::restart`](struct.Outsourcer.html#method.restart)
    or
    [`Outsourcer::restart_if`](struct.Outsourcer.html#method.restart_if).
    */
    pub fn start(&self, input: K) {
        if !self.get(&input).exists() {
            self._start(input);
        }
    }
    /**
    Start a new job with the given input if one with the same input
    is not in progress

    If you want to avoid starting a new job if one with the same
    input has already finished, use
    [`Outsourcer::start`](struct.Outsourcer.html#method.start).
    */
    pub fn restart(&self, input: K) {
        if !self.get(&input).is_in_progress() {
            self._start(input);
        }
    }
    /**
    Start a new job with the given input if one with the same input
    is not in progress, and if it exists and is finished, the result
    satisfies the supplied condition

    An example use case is if your job involves IO. In this case,
    your work function could return a `Result`, and the condition
    supplied to this function would be `Result::is_err`.

    If you want to avoid starting a new job if one with the same
    input has already finished, use
    [`Outsourcer::start`](struct.Outsourcer.html#method.start).
    */
    pub fn restart_if<G>(&self, input: K, condition: G)
    where
        G: FnOnce(&V) -> bool,
    {
        let restart = match self.get(&input) {
            Job::None => true,
            Job::InProgress => false,
            Job::Finished(guard) => condition(&*guard),
        };
        if restart {
            self._start(input)
        }
    }
}

impl<K, V, F> fmt::Debug for Outsourcer<K, V, F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Outsourcer")
            .field("in progress", &self.in_progress_len())
            .field("finished", &self.map)
            .finish()
    }
}

/// A guard to the result of a finished job
pub struct Guard<'a, K, V>(ReadGuard<'a, K, V>);

impl<'a, K, V> Deref for Guard<'a, K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.0.val()
    }
}

impl<'a, K, V, T> PartialEq<T> for Guard<'a, K, V>
where
    V: PartialEq<T>,
{
    fn eq(&self, other: &T) -> bool {
        (**self).eq(other)
    }
}

impl<'a, K, V, T> PartialOrd<T> for Guard<'a, K, V>
where
    V: PartialOrd<T>,
{
    fn partial_cmp(&self, other: &T) -> Option<CmpOrdering> {
        (**self).partial_cmp(other)
    }
}

impl<'a, K, V> fmt::Debug for Guard<'a, K, V>
where
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <V as fmt::Debug>::fmt(self, f)
    }
}

impl<'a, K, V> fmt::Display for Guard<'a, K, V>
where
    V: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <V as fmt::Display>::fmt(self, f)
    }
}
