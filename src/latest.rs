//! The value a collector published last, shared with whoever reads it.

use std::fmt::Display;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::tag::{Epoch, Tagged, Versioned};

/// The value published last, or why the last attempt to produce one failed.
/// Cheap to clone; every clone reads and writes the same value.
pub struct Latest<T> {
    state: Arc<Mutex<State<T>>>,
}

struct State<T> {
    value: Versioned<Arc<T>>,
    failure: Option<String>,
}

impl<T> Clone for Latest<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Latest<T> {
    pub fn new(initial: T) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                value: Versioned::new(Epoch::new(), Arc::new(initial)),
                failure: None,
            })),
        }
    }

    /// The value and its tag, or why the last attempt failed.
    pub fn get(&self) -> Result<Tagged<Arc<T>>, String> {
        let state = self.lock();
        match &state.failure {
            Some(failure) => Err(failure.clone()),
            None => Ok(state.value.get().clone()),
        }
    }

    /// Takes `value` under a new tag, whether or not it differs; clears a failure.
    pub fn replace(&self, value: T) {
        let mut state = self.lock();
        state.value.replace(Arc::new(value));
        state.failure = None;
    }

    /// Readers get `reason` until the next value.
    pub fn fail(&self, reason: impl Display) {
        self.lock().failure = Some(format!("{reason:#}"));
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T: PartialEq> Latest<T> {
    /// Takes `value`, under a new tag only if it differs; clears a failure.
    /// True when the tag moved.
    pub fn set(&self, value: T) -> bool {
        let mut state = self.lock();
        state.failure = None;
        state.value.set(Arc::new(value))
    }

    /// [`set`](Self::set) for a value, [`fail`](Self::fail) for an error.
    pub fn publish<E: Display>(&self, produced: Result<T, E>) {
        match produced {
            Ok(value) => {
                self.set(value);
            }
            Err(error) => self.fail(error),
        }
    }
}

impl<T: Default> Default for Latest<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_value_again_keeps_its_tag() {
        let latest = Latest::new(0);
        latest.publish(Ok::<_, String>(1));
        let first = latest.get().unwrap().etag;
        latest.publish(Ok::<_, String>(1));
        assert_eq!(latest.get().unwrap().etag, first);
    }

    #[test]
    fn a_different_value_moves_the_tag() {
        let latest = Latest::new(0);
        let first = latest.get().unwrap().etag;
        assert!(latest.set(2));
        let second = latest.get().unwrap();
        assert_ne!(second.etag, first);
        assert_eq!(*second.value, 2);
    }

    #[test]
    fn a_failure_is_read_until_the_next_value() {
        let latest = Latest::new(0);
        latest.set(1);
        latest.publish(Err::<i32, _>(anyhow::anyhow!("map gone")));
        assert_eq!(latest.get().unwrap_err(), "map gone");
        latest.set(1);
        assert_eq!(*latest.get().unwrap().value, 1, "the same value clears the failure too");
    }

    #[test]
    fn an_error_is_kept_with_its_causes() {
        let latest = Latest::new(0);
        let error = anyhow::anyhow!("no such map").context("collect failed");
        latest.fail(error);
        assert_eq!(latest.get().unwrap_err(), "collect failed: no such map");
    }

    #[test]
    fn every_clone_reads_the_same_value() {
        let latest = Latest::new(0);
        let reader = latest.clone();
        latest.replace(0);
        assert_eq!(reader.get().unwrap().etag, latest.get().unwrap().etag);
    }
}
