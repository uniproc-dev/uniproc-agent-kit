//! A generation counter that futures can wait on, whatever executor polls them.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

/// Counts notifications and wakes whoever waits for the next one.
#[derive(Default)]
pub struct Notify {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    generation: u64,
    waiters: Vec<Waker>,
}

impl Notify {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many notifications there have been.
    pub fn generation(&self) -> u64 {
        self.lock().generation
    }

    /// Moves the generation and wakes every waiter.
    pub fn notify(&self) {
        let waiters = {
            let mut state = self.lock();
            state.generation += 1;
            std::mem::take(&mut state.waiters)
        };
        for waker in waiters {
            waker.wake();
        }
    }

    /// Resolves to the generation once it is no longer `since`.
    pub fn changed(&self, since: u64) -> Changed<'_> {
        Changed { notify: self, since }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// See [`Notify::changed`].
pub struct Changed<'a> {
    notify: &'a Notify,
    since: u64,
}

impl Future for Changed<'_> {
    type Output = u64;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u64> {
        let mut state = self.notify.lock();
        if state.generation != self.since {
            return Poll::Ready(state.generation);
        }
        if !state.waiters.iter().any(|w| w.will_wake(cx.waker())) {
            state.waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn a_generation_already_moved_resolves_at_once() {
        let notify = Notify::new();
        let seen = notify.generation();
        notify.notify();
        assert_eq!(futures::executor::block_on(notify.changed(seen)), seen + 1);
    }

    #[test]
    fn a_waiter_is_woken_by_the_next_notify() {
        let notify = Arc::new(Notify::new());
        let seen = notify.generation();
        let waiter = {
            let notify = notify.clone();
            std::thread::spawn(move || futures::executor::block_on(notify.changed(seen)))
        };
        std::thread::sleep(Duration::from_millis(50));
        notify.notify();
        assert_eq!(waiter.join().unwrap(), seen + 1);
    }

    #[test]
    fn a_dropped_waiter_leaves_nothing_behind_after_a_notify() {
        let notify = Notify::new();
        let seen = notify.generation();
        let mut pending = Box::pin(notify.changed(seen));
        let waker = futures::task::noop_waker();
        assert!(pending.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        drop(pending);
        notify.notify();
        assert!(notify.lock().waiters.is_empty());
    }
}
