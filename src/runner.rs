//! Worker threads that run jobs, one at a time for any one target.

use std::collections::HashSet;
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use crossbeam_channel::Sender;
use futures_channel::oneshot;

/// A job was asked for a target that already has one running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Busy;

impl std::fmt::Display for Busy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("another job for this target is running")
    }
}

impl std::error::Error for Busy {}

type Job = Box<dyn FnOnce() + Send>;

/// Runs jobs on threads of its own, one at a time per target, so a slow job
/// holds up only its own target. Cheap to clone; the threads end with the last clone.
pub struct Runner<K> {
    shared: Arc<Shared<K>>,
}

struct Shared<K> {
    jobs: Sender<Job>,
    busy: Mutex<HashSet<K>>,
}

impl<K> Clone for Runner<K> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<K: Clone + Eq + Hash + Send + 'static> Runner<K> {
    /// Starts `workers` threads named `{name}-{n}`.
    pub fn start(name: &str, workers: usize) -> std::io::Result<Self> {
        let (jobs, queue) = crossbeam_channel::unbounded::<Job>();
        for worker in 0..workers.max(1) {
            let queue = queue.clone();
            std::thread::Builder::new()
                .name(format!("{name}-{worker}"))
                .spawn(move || {
                    for job in queue {
                        let _ = std::panic::catch_unwind(AssertUnwindSafe(job));
                    }
                })?;
        }
        Ok(Self {
            shared: Arc::new(Shared {
                jobs,
                busy: Mutex::default(),
            }),
        })
    }

    /// [`Busy`] at once while another job for `target` runs. The target is
    /// free again before the answer arrives; a job that panicked cancels its answer.
    pub fn run<R: Send + 'static>(
        &self,
        target: K,
        job: impl FnOnce() -> R + Send + 'static,
    ) -> Result<oneshot::Receiver<R>, Busy> {
        if !self.shared.busy().insert(target.clone()) {
            return Err(Busy);
        }
        let (tx, rx) = oneshot::channel();
        let free = Free {
            shared: self.shared.clone(),
            target,
        };
        let job: Job = Box::new(move || {
            let answer = job();
            drop(free);
            let _ = tx.send(answer);
        });
        let _ = self.shared.jobs.send(job);
        Ok(rx)
    }
}

impl<K> Shared<K> {
    fn busy(&self) -> std::sync::MutexGuard<'_, HashSet<K>> {
        self.busy.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct Free<K: Eq + Hash> {
    shared: Arc<Shared<K>>,
    target: K,
}

impl<K: Eq + Hash> Drop for Free<K> {
    fn drop(&mut self) {
        self.shared.busy().remove(&self.target);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORKERS: usize = 4;

    fn runner() -> Runner<u32> {
        Runner::start("test-runner", WORKERS).unwrap()
    }

    fn answer<R>(rx: Result<oneshot::Receiver<R>, Busy>) -> R {
        futures::executor::block_on(rx.expect("not busy")).expect("answered")
    }

    #[test]
    fn a_second_job_for_the_same_target_is_busy_until_the_first_is_done() {
        let runner = runner();
        let (release, released) = crossbeam_channel::bounded::<()>(0);
        let first = runner.run(1, move || {
            let _ = released.recv();
            "first"
        });

        assert_eq!(runner.run(1, || "again").err(), Some(Busy));
        assert_eq!(answer(runner.run(2, || "other")), "other", "another target is not held up");

        release.send(()).unwrap();
        assert_eq!(answer(first), "first");
        assert_eq!(answer(runner.run(1, || "after")), "after");
    }

    #[test]
    fn a_long_job_does_not_hold_up_the_rest() {
        let runner = runner();
        let (release, released) = crossbeam_channel::bounded::<()>(0);
        let long = runner.run(u32::MAX, move || {
            let _ = released.recv();
        });
        for target in 0..(WORKERS as u32 * 2) {
            answer(runner.run(target, || ()));
        }
        release.send(()).unwrap();
        answer(long);
    }

    #[test]
    fn a_job_that_panics_frees_its_target_and_its_worker() {
        let runner = runner();
        for _ in 0..(WORKERS * 2) {
            let panicked = runner.run(7, || -> () { panic!("boom") }).unwrap();
            assert!(futures::executor::block_on(panicked).is_err());
        }
        assert_eq!(answer(runner.run(7, || 1)), 1);
    }

    #[test]
    fn the_target_is_free_by_the_time_the_answer_arrives() {
        let runner = runner();
        for _ in 0..100 {
            answer(runner.run(3, || ()));
        }
    }
}
