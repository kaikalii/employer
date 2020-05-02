#![warn(missing_docs)]

/*!
### Description

This crate provides an interface for spawning worker threads and checking
if their work has finished and produced a result.

The primary struct is the [`Employer`](struct.Employer.html), which can be used to
start new jobs and also holds the results of finished jobs.

### When should you use this?

This crate is designed for applications where you need to do some expensive
task, but the main thread can carry on just fine without that task's result.
The most common domain for this type of problem is in asset loading. This crate
allows you to start loading assets and then render/play/use them when they are
finished loading.

### Example
```
use std::{thread, time::Duration};
use employer::*;

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
        Arc, Mutex,
    },
    thread,
};

use lockfree::{
    map::{self, Map},
    set::{self, Set},
};

/**
A description of a job

This trait defines work to be done through an
[`Employer`](struct.Employer.html).
This crate provides two implementations. A stateless one,
and a stateful one.

### Stateless Example

`JobDescription<I, Output = O>` is emplemented for all
`F` where `F: Fn(I) -> O + Send + Sync`. This is the basic
stateless implementation.
```
use employer::*;

let employer = Employer::new(|i| i + 1); // No state, just a function

employer.start(1);

assert_eq!(employer.wait_for(&1).unwrap(), 2);
```

### Stateful Example

`JobDescription<I, Output = O>` is emplemented for all
`(S, F)` where `F: Fn(&S, I) -> O + Send + Sync, S: Send + Sync`.
This is the basic stateful implementation. **State is shared
accross threads**. The function [`Employer::with_state`](struct.Employer.html#method.with_state)
makes this easier to construct.

```
use employer::*;

let employer = Employer::with_state(3, |state: &i32, i| i + *state); // A state and a function

employer.start(1);

assert_eq!(employer.wait_for(&1).unwrap(), 4);
```
*/
pub trait JobDescription<I>: Send + Sync {
    /// The job output type
    type Output;
    /// Do the job
    fn work(&self, input: I) -> Self::Output;
}

impl<F, I, O> JobDescription<I> for F
where
    F: Fn(I) -> O + Send + Sync,
{
    type Output = O;
    fn work(&self, input: I) -> Self::Output {
        self(input)
    }
}

impl<'a, F, I, O, S> JobDescription<I> for (S, F)
where
    F: Fn(&S, I) -> O + Send + Sync,
    S: Send + Sync,
{
    type Output = O;
    fn work(&self, input: I) -> Self::Output {
        (self.1)(&self.0, input)
    }
}

/// The status of a job
pub enum Job<'a, K, V> {
    /// No job exists with the given input
    None,
    /// The job is in progress
    InProgress,
    /// The job has finished
    Finished(OutputGuard<'a, K, V>),
}

impl<'a, K, V> Default for Job<'a, K, V> {
    fn default() -> Self {
        Job::None
    }
}

impl<'a, K, V> fmt::Debug for Job<'a, K, V>
where
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Job::None => write!(f, "None"),
            Job::InProgress => write!(f, "InProgress"),
            Job::Finished(g) => g.fmt(f),
        }
    }
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
    pub fn as_finished(&self) -> Option<&OutputGuard<'a, K, V>> {
        if let Job::Finished(job) = self {
            Some(job)
        } else {
            None
        }
    }
    /// Convert the `Job` to an `Option<OutputGuard>`
    pub fn finished(self) -> Option<OutputGuard<'a, K, V>> {
        if let Job::Finished(job) = self {
            Some(job)
        } else {
            None
        }
    }
}

impl<'a, K, V> From<Job<'a, K, V>> for Option<OutputGuard<'a, K, V>> {
    fn from(job: Job<'a, K, V>) -> Self {
        job.finished()
    }
}

/**
An interface for spawning worker threads and storing their results

The `description` passed to `Employer::new` must implement the
[`JobDescription`](trait.JobDescription.html) trait.
*/
pub struct Employer<K, V, D> {
    locks: Arc<Map<K, Arc<Mutex<()>>>>,
    in_progress: Arc<Set<K>>,
    finished: Arc<Map<K, V>>,
    desc: Arc<D>,
    in_progress_len: Arc<AtomicUsize>,
    finished_len: Arc<AtomicUsize>,
}

impl<K, V, D> Employer<K, V, D> {
    /// Create a new `Employer` with the given job description
    pub fn new(description: D) -> Self {
        Employer {
            locks: Arc::new(Map::new()),
            in_progress: Arc::new(Set::new()),
            finished: Arc::new(Map::new()),
            desc: Arc::new(description),
            in_progress_len: Arc::new(AtomicUsize::new(0)),
            finished_len: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<K, V, S, F> Employer<K, V, (S, F)>
where
    (S, F): JobDescription<K, Output = V>,
{
    /// Create a new `Employer` wither the given shared state and job function
    pub fn with_state(state: S, f: F) -> Self {
        Employer::new((state, f))
    }
}

impl<K, V, D> Employer<K, V, D> {
    /// Get the job with the given input
    pub fn get<'a, Q>(&'a self, input: &Q) -> Job<'a, K, V>
    where
        Q: Hash + Ord,
        K: Borrow<Q>,
    {
        if self.in_progress.contains(input) {
            Job::InProgress
        } else if let Some(rg) = self.finished.get(input) {
            Job::Finished(OutputGuard(rg))
        } else {
            Job::None
        }
    }
    /// Block the thread, wait for a job to finish, and get its result
    ///
    /// Returns `None` if a job with the given input does not exist
    pub fn wait_for<'a, Q>(&'a self, input: &Q) -> Option<OutputGuard<'a, K, V>>
    where
        Q: Hash + Ord,
        K: Borrow<Q>,
    {
        if let Some(rg) = self.locks.get(input) {
            loop {
                let done_guard = rg.val().lock().expect("Progress lock poisoned");
                if let Some(res) = self.get(input).finished() {
                    drop(done_guard);
                    break Some(res);
                } else {
                    drop(done_guard);
                }
            }
        } else {
            None
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
        self.in_progress_len.load(AtomicOrdering::Relaxed)
    }
    /// Check the number of jobs that are in progress
    pub fn finished_len(&self) -> usize {
        self.finished_len.load(AtomicOrdering::Relaxed)
    }
    /// Iterate over finished job input/output pairs
    pub fn finished_iter(&self) -> JobIter<K, V> {
        self.finished.iter()
    }
    /// Iterate over in_progress job inputs
    pub fn in_progress_inputs(&self) -> impl Iterator<Item = InputGuard<K, V>> {
        self.in_progress
            .iter()
            .map(|g| InputGuard(InputGuardInner::Set(g)))
    }
    /// Iterate over finished job inputs
    pub fn finished_inputs(&self) -> impl Iterator<Item = InputGuard<K, V>> {
        self.finished
            .iter()
            .map(|g| InputGuard(InputGuardInner::Map(g)))
    }
    /// Iterate over all job inputs
    pub fn inputs(&self) -> impl Iterator<Item = InputGuard<K, V>> {
        self.finished_inputs().chain(self.in_progress_inputs())
    }
    /// Iterate over finished job outputs
    pub fn outputs(&self) -> impl Iterator<Item = OutputGuard<K, V>> {
        self.finished.iter().map(OutputGuard)
    }
}

impl<K, V, D> Employer<K, V, D>
where
    K: Ord + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    D: JobDescription<K, Output = V> + 'static,
{
    fn _start(&self, input: K) {
        // Create lock
        let done_lock = Arc::new(Mutex::new(()));
        self.locks.insert(input.clone(), Arc::clone(&done_lock));
        // Add job to in_progress
        let _ = self.in_progress.insert(input.clone());
        // Increment in_progress_len
        self.in_progress_len.fetch_add(1, AtomicOrdering::Relaxed);
        // Clone Arcs for job thread
        let finished = Arc::clone(&self.finished);
        let in_progress = Arc::clone(&self.in_progress);
        let desc = Arc::clone(&self.desc);
        let in_progress_len = Arc::clone(&self.in_progress_len);
        let finished_len = Arc::clone(&self.finished_len);
        // Spawn job thread
        thread::spawn(move || {
            // Lock until done
            let done_guard = done_lock.lock().expect("Progress lock poisoned");
            // Do work
            let res = desc.work(input.clone());
            // Remove job from in_progress
            in_progress.remove(&input);
            // Decrement in_progress_len
            in_progress_len.fetch_sub(1, AtomicOrdering::Relaxed);
            // Increment finished_len
            finished_len.fetch_add(1, AtomicOrdering::Relaxed);
            // Add result to finished
            finished.insert(input, res);
            // Unlock
            drop(done_guard);
        });
    }
    /**
    Start a new job with the the given input if one with the same
    input is not in progress or finished

    If you want to start the job even if one with the same input
    is already finished, use
    [`Employer::restart`](struct.Employer.html#method.restart)
    or
    [`Employer::restart_if`](struct.Employer.html#method.restart_if).
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
    [`Employer::start`](struct.Employer.html#method.start).
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
    [`Employer::start`](struct.Employer.html#method.start).
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
    /// Remove a finished job with the given input
    pub fn remove<Q>(&self, input: &Q)
    where
        Q: Hash + Ord,
        K: Borrow<Q>,
    {
        if self.finished.remove(input).is_some() {
            self.locks.remove(input);
            self.finished_len.fetch_sub(1, AtomicOrdering::Relaxed);
        }
    }
}

impl<K, V, D> fmt::Debug for Employer<K, V, D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Employer")
            .field("in progress", &self.in_progress_len())
            .field("finished", &self.finished)
            .finish()
    }
}

impl<K, V, D> Default for Employer<K, V, D>
where
    D: Default,
{
    fn default() -> Self {
        Employer::new(D::default())
    }
}

impl<K, V, D> From<Employer<K, V, D>> for Arc<D> {
    fn from(employer: Employer<K, V, D>) -> Self {
        employer.desc
    }
}

impl<K, V, D> From<D> for Employer<K, V, D> {
    fn from(desc: D) -> Self {
        Employer::new(desc)
    }
}

/// A guard to finished job input/output pairs
pub type JobGuard<'a, K, V> = lockfree::map::ReadGuard<'a, K, V>;
/// An iterator over guards to finished job input/output pairs
pub type JobIter<'a, K, V> = lockfree::map::Iter<'a, K, V>;

impl<'a, K, V, H> IntoIterator for &'a Employer<K, V, H> {
    type Item = JobGuard<'a, K, V>;
    type IntoIter = JobIter<'a, K, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.finished.iter()
    }
}

/// A guard to the result of a finished job
pub struct OutputGuard<'a, K, V>(map::ReadGuard<'a, K, V>);

impl<'a, K, V> Deref for OutputGuard<'a, K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.0.val()
    }
}

impl<'a, K, V, T> PartialEq<T> for OutputGuard<'a, K, V>
where
    V: PartialEq<T>,
{
    fn eq(&self, other: &T) -> bool {
        (**self).eq(other)
    }
}

impl<'a, K, V, T> PartialOrd<T> for OutputGuard<'a, K, V>
where
    V: PartialOrd<T>,
{
    fn partial_cmp(&self, other: &T) -> Option<CmpOrdering> {
        (**self).partial_cmp(other)
    }
}

impl<'a, K, V> fmt::Debug for OutputGuard<'a, K, V>
where
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <V as fmt::Debug>::fmt(self, f)
    }
}

impl<'a, K, V> fmt::Display for OutputGuard<'a, K, V>
where
    V: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <V as fmt::Display>::fmt(self, f)
    }
}

enum InputGuardInner<'a, K, V> {
    Map(map::ReadGuard<'a, K, V>),
    Set(set::ReadGuard<'a, K>),
}

/// A guard to the input of a job
pub struct InputGuard<'a, K, V>(InputGuardInner<'a, K, V>);

impl<'a, K, V> Deref for InputGuard<'a, K, V> {
    type Target = K;
    fn deref(&self) -> &Self::Target {
        match &self.0 {
            InputGuardInner::Map(g) => g.key(),
            InputGuardInner::Set(g) => &*g,
        }
    }
}

impl<'a, K, V, T> PartialEq<T> for InputGuard<'a, K, V>
where
    K: PartialEq<T>,
{
    fn eq(&self, other: &T) -> bool {
        (**self).eq(other)
    }
}

impl<'a, K, V, T> PartialOrd<T> for InputGuard<'a, K, V>
where
    K: PartialOrd<T>,
{
    fn partial_cmp(&self, other: &T) -> Option<CmpOrdering> {
        (**self).partial_cmp(other)
    }
}

impl<'a, K, V> fmt::Debug for InputGuard<'a, K, V>
where
    K: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <K as fmt::Debug>::fmt(self, f)
    }
}

impl<'a, K, V> fmt::Display for InputGuard<'a, K, V>
where
    K: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <K as fmt::Display>::fmt(self, f)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    #[test]
    fn wait_for() {
        let employer = Employer::new(|i| {
            thread::sleep(Duration::from_millis(200));
            2 * i + 1
        });
        employer.start(1);
        employer.start(2);
        employer.start(3);
        assert_eq!(employer.wait_for(&1).unwrap(), 3);
        assert_eq!(employer.wait_for(&2).unwrap(), 5);
        assert_eq!(employer.wait_for(&3).unwrap(), 7);
    }
    #[test]
    fn state() {
        let employer = Employer::new((3, |state: &i32, i| i + *state));
        employer.start(1);
        assert_eq!(employer.wait_for(&1).unwrap(), 4);
    }
}
