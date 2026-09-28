//! A collector on a thread of its own, ticked when the deadline it named
//! passes and whenever someone wakes it.
//!
//! The collector owns its schedule: every tick returns when the next one is
//! due. A wake ticks it sooner, but never sooner than `spacing` after the
//! last tick ended, so a burst of wakes costs one tick.
//!
//! The thread is a state machine. Wakes and the stop travel one channel,
//! and every wait reads it, so a stop is seen wherever the thread waits;
//! a tick in progress runs to its end first.
//!
//! | from | wake | stop | deadline | tick returns `next` | tick panics |
//! |---|---|---|---|---|---|
//! | `Waiting { due, rested }` | `Ticking(Woken)` once rested, else `Cooling { rested }` | `Stopped` | `Ticking(Due)` at `due` | | |
//! | `Cooling { until }` | `Cooling { until }` | `Stopped` | `Ticking(Woken)` at `until` | | |
//! | `Ticking(why)` | read in `Waiting` | read in `Waiting` | | `Waiting { max(next, rested), rested }`, `rested` = now + spacing | `Failed` |
//! | `Stopped`, `Failed` | final | final | | | |
//!
//! A hung-up channel counts as a stop. The first tick runs as `Due` before
//! [`Monitor::start`] returns; a panic there is the error of `start`, a
//! panic later is [`Monitor::failure`].

mod machine;

use std::sync::mpsc;
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};

use machine::State;
pub use machine::{Waker, Why};

/// What a [`Monitor`] runs on its thread.
pub trait Collector {
    /// Runs on the monitor's thread before the first tick; the error is the
    /// error of [`Monitor::start`]. `waker` wakes this monitor from anywhere.
    fn start(&mut self, waker: Waker) -> Result<()> {
        let _ = waker;
        Ok(())
    }

    /// Collects and hands the result over; returns when the next tick is
    /// due unless a wake comes first.
    fn tick(&mut self, why: Why) -> Instant;
}

/// Ticks a [`Collector`] on a thread of its own. Stops it when dropped.
pub struct Monitor {
    waker: Waker,
    failure: Arc<OnceLock<String>>,
    thread: Option<JoinHandle<()>>,
}

impl Monitor {
    /// Starts `collector` on a new thread named `name` and returns once its
    /// first tick is over, or with the error its start gave.
    pub fn start(name: &str, spacing: Duration, mut collector: impl Collector + Send + 'static) -> Result<Self> {
        let (waker, inbox) = machine::channel();
        let failure = Arc::new(OnceLock::new());
        let (started, outcome) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new().name(name.to_string()).spawn({
            let waker = waker.clone();
            let failure = failure.clone();
            move || {
                if let Err(error) = collector.start(waker) {
                    let _ = started.send(Err(error));
                    return;
                }
                let mut tick = |why| collector.tick(why);
                let mut state = machine::next_state(State::Ticking { why: Why::Due }, &inbox, spacing, &mut tick);
                if let State::Failed(reason) = &state {
                    let _ = started.send(Err(anyhow!("the first tick panicked: {reason}")));
                    return;
                }
                let _ = started.send(Ok(()));
                while !state.is_over() {
                    state = machine::next_state(state, &inbox, spacing, &mut tick);
                }
                if let State::Failed(reason) = state {
                    let _ = failure.set(reason);
                }
            }
        })?;

        let outcome = outcome
            .recv()
            .unwrap_or_else(|_| Err(anyhow!("the monitor's thread ended before it started")));
        match outcome {
            Ok(()) => Ok(Self {
                waker,
                failure,
                thread: Some(thread),
            }),
            Err(error) => {
                let _ = thread.join();
                Err(error)
            }
        }
    }

    /// Asks for a tick now, within the spacing.
    pub fn wake(&self) {
        self.waker.wake();
    }

    /// Wakes this monitor without holding it; outlives it harmlessly.
    pub fn waker(&self) -> Waker {
        self.waker.clone()
    }

    /// What the tick panicked with; the monitor ticks no more after that.
    pub fn failure(&self) -> Option<&str> {
        self.failure.get().map(String::as_str)
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        self.waker.stop();
        if let Some(thread) = self.thread.take()
            && thread.thread().id() != std::thread::current().id()
        {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{Receiver, Sender};
    use std::sync::Mutex;

    struct Slot<T>(Mutex<Option<T>>);

    impl<T> Slot<T> {
        fn new() -> Arc<Self> {
            Arc::new(Self(Mutex::new(None)))
        }

        fn put(&self, value: T) {
            *self.0.lock().unwrap() = Some(value);
        }

        fn take(&self) -> Option<T> {
            self.0.lock().unwrap().take()
        }
    }

    struct Counting {
        ticks: Sender<(u32, Why)>,
        count: u32,
        every: Duration,
    }

    impl Collector for Counting {
        fn tick(&mut self, why: Why) -> Instant {
            self.count += 1;
            let _ = self.ticks.send((self.count, why));
            Instant::now() + self.every
        }
    }

    fn counting(spacing: Duration, every: Duration) -> (Monitor, Receiver<(u32, Why)>) {
        let (ticks, rx) = crossbeam_channel::unbounded();
        let monitor = Monitor::start(
            "test-monitor",
            spacing,
            Counting {
                ticks,
                count: 0,
                every,
            },
        )
        .unwrap();
        (monitor, rx)
    }

    const LONG: Duration = Duration::from_secs(60);
    const WAIT: Duration = Duration::from_secs(5);

    #[test]
    fn the_first_tick_is_over_before_start_returns() {
        let (_monitor, ticks) = counting(Duration::from_millis(10), LONG);
        assert_eq!(ticks.try_recv(), Ok((1, Why::Due)));
    }

    #[test]
    fn a_wake_ticks_without_waiting_for_the_deadline() {
        let (monitor, ticks) = counting(Duration::from_millis(10), LONG);
        let _ = ticks.recv();
        monitor.wake();
        assert_eq!(ticks.recv_timeout(WAIT), Ok((2, Why::Woken)));
    }

    #[test]
    fn nothing_waking_it_ticks_at_the_deadlines_it_names() {
        let (_monitor, ticks) = counting(Duration::from_millis(1), Duration::from_millis(20));
        for expected in 1..=3 {
            assert_eq!(ticks.recv_timeout(WAIT), Ok((expected, Why::Due)));
        }
    }

    #[test]
    fn a_burst_of_wakes_is_spaced_out() {
        let spacing = Duration::from_millis(100);
        let (monitor, ticks) = counting(spacing, LONG);
        let _ = ticks.recv();
        let began = Instant::now();
        for _ in 0..3 {
            monitor.wake();
            let _ = ticks.recv_timeout(WAIT).unwrap();
            monitor.wake();
        }
        assert!(began.elapsed() >= spacing * 2, "{:?}", began.elapsed());
    }

    #[test]
    fn a_waker_handed_to_the_collector_wakes_it_from_another_thread() {
        struct Handing(Arc<Slot<Waker>>, Sender<Why>);
        impl Collector for Handing {
            fn start(&mut self, waker: Waker) -> Result<()> {
                self.0.put(waker);
                Ok(())
            }
            fn tick(&mut self, why: Why) -> Instant {
                let _ = self.1.send(why);
                Instant::now() + LONG
            }
        }
        let slot = Slot::new();
        let (tx, whys) = crossbeam_channel::unbounded();
        let _monitor = Monitor::start("test-monitor", Duration::from_millis(1), Handing(slot.clone(), tx)).unwrap();
        let waker = slot.take().unwrap();
        let _ = whys.recv();
        std::thread::spawn(move || waker.wake()).join().unwrap();
        assert_eq!(whys.recv_timeout(WAIT), Ok(Why::Woken));
    }

    #[test]
    fn a_start_that_fails_is_the_error_of_start() {
        struct Failing;
        impl Collector for Failing {
            fn start(&mut self, _: Waker) -> Result<()> {
                Err(anyhow!("no collector"))
            }
            fn tick(&mut self, _: Why) -> Instant {
                unreachable!()
            }
        }
        let started = Monitor::start("test-monitor", LONG, Failing);
        assert_eq!(started.err().unwrap().to_string(), "no collector");
    }

    struct Panicking {
        after: u32,
    }

    impl Collector for Panicking {
        fn tick(&mut self, _: Why) -> Instant {
            if self.after == 0 {
                panic!("collector broke");
            }
            self.after -= 1;
            Instant::now()
        }
    }

    #[test]
    fn a_first_tick_that_panics_is_the_error_of_start() {
        let started = Monitor::start("test-monitor", LONG, Panicking { after: 0 });
        assert_eq!(started.err().unwrap().to_string(), "the first tick panicked: collector broke");
    }

    #[test]
    fn a_tick_that_panics_later_is_the_failure() {
        let monitor = Monitor::start("test-monitor", Duration::from_millis(1), Panicking { after: 1 }).unwrap();
        let deadline = Instant::now() + WAIT;
        while monitor.failure().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(monitor.failure(), Some("collector broke"));
    }

    #[test]
    fn dropping_it_stops_the_ticks() {
        let (monitor, ticks) = counting(Duration::from_millis(1), Duration::from_millis(5));
        drop(monitor);
        let _ = ticks.try_iter().count();
        assert!(ticks.recv_timeout(Duration::from_millis(100)).is_err());
    }

    #[test]
    fn dropping_it_from_its_own_tick_does_not_wait_for_itself() {
        struct Owning(Arc<Slot<Monitor>>, Sender<()>);
        impl Collector for Owning {
            fn tick(&mut self, _: Why) -> Instant {
                drop(self.0.take());
                let _ = self.1.send(());
                Instant::now() + LONG
            }
        }
        let slot = Slot::new();
        let (tx, done) = crossbeam_channel::unbounded();
        let monitor = Monitor::start("test-monitor", Duration::from_millis(1), Owning(slot.clone(), tx)).unwrap();
        let _ = done.recv();
        let waker = monitor.waker();
        slot.put(monitor);
        waker.wake();
        assert_eq!(done.recv_timeout(WAIT), Ok(()));
        assert!(slot.take().is_none(), "the tick dropped it");
    }
}
