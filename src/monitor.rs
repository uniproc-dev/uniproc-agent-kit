//! A collector on a thread of its own that reports as soon as it has
//! something new, at least once a period, and hands every report over.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};

/// How often a [`Monitor`] reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cadence {
    /// The longest a report waits when nothing woke the monitor.
    pub period: Duration,
    /// The shortest gap between two reports, so a burst of wake-ups costs a handful.
    pub spacing: Duration,
}

impl Default for Cadence {
    fn default() -> Self {
        Self {
            period: Duration::from_secs(1),
            spacing: Duration::from_millis(50),
        }
    }
}

/// Reports from a thread of its own. Stops the collector when dropped.
pub struct Monitor {
    running: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Monitor {
    /// Runs `start` on a new thread named `name`; the tick it returns is
    /// called for every report, and each report goes to `publish`.
    ///
    /// Returns once the first report has been handed over, or with the
    /// error `start` gave. The collector wakes the thread for an early report
    /// by unparking it: `std::thread::current()` inside `start` is that thread.
    pub fn start<T, R>(
        name: &str,
        cadence: Cadence,
        start: impl FnOnce() -> Result<T> + Send + 'static,
        mut publish: impl FnMut(R) + Send + 'static,
    ) -> Result<Self>
    where
        T: FnMut() -> R,
    {
        let running = Arc::new(AtomicBool::new(true));
        let (started, outcome) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new().name(name.to_string()).spawn({
            let running = running.clone();
            move || {
                let mut tick = match start() {
                    Ok(tick) => tick,
                    Err(error) => {
                        let _ = started.send(Err(error));
                        return;
                    }
                };
                publish(tick());
                let _ = started.send(Ok(()));

                let mut last = Instant::now();
                loop {
                    std::thread::park_timeout(cadence.period);
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }
                    let since = last.elapsed();
                    if since < cadence.spacing {
                        std::thread::sleep(cadence.spacing - since);
                    }
                    publish(tick());
                    last = Instant::now();
                }
            }
        })?;

        let outcome = outcome
            .recv()
            .unwrap_or_else(|_| Err(anyhow!("the monitor's thread ended before it started")));
        match outcome {
            Ok(()) => Ok(Self {
                running,
                thread: Some(thread),
            }),
            Err(error) => {
                let _ = thread.join();
                Err(error)
            }
        }
    }

    /// Asks for a report now, within the cadence's spacing.
    pub fn wake(&self) {
        if let Some(thread) = &self.thread {
            thread.thread().unpark();
        }
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counting(cadence: Cadence) -> (Monitor, crossbeam_channel::Receiver<u32>) {
        let (tx, rx) = crossbeam_channel::unbounded();
        let monitor = Monitor::start(
            "test-monitor",
            cadence,
            || {
                let mut n = 0;
                Ok(move || {
                    n += 1;
                    n
                })
            },
            move |n| {
                let _ = tx.send(n);
            },
        )
        .unwrap();
        (monitor, rx)
    }

    fn slow() -> Cadence {
        Cadence {
            period: Duration::from_secs(60),
            spacing: Duration::from_millis(10),
        }
    }

    #[test]
    fn the_first_report_is_in_before_start_returns() {
        let (_monitor, reports) = counting(slow());
        assert_eq!(reports.try_recv(), Ok(1));
    }

    #[test]
    fn a_wake_reports_without_waiting_for_the_period() {
        let (monitor, reports) = counting(slow());
        assert_eq!(reports.recv().unwrap(), 1);
        monitor.wake();
        assert_eq!(reports.recv_timeout(Duration::from_secs(5)), Ok(2));
    }

    #[test]
    fn nothing_waking_it_still_reports_every_period() {
        let (_monitor, reports) = counting(Cadence {
            period: Duration::from_millis(20),
            spacing: Duration::from_millis(1),
        });
        for expected in 1..=3 {
            assert_eq!(reports.recv_timeout(Duration::from_secs(5)), Ok(expected));
        }
    }

    #[test]
    fn a_burst_of_wakes_is_spaced_out() {
        let spacing = Duration::from_millis(100);
        let (monitor, reports) = counting(Cadence {
            period: Duration::from_secs(60),
            spacing,
        });
        let _ = reports.recv();
        let began = Instant::now();
        for _ in 0..3 {
            monitor.wake();
            let _ = reports.recv_timeout(Duration::from_secs(5)).unwrap();
            monitor.wake();
        }
        assert!(began.elapsed() >= spacing * 2, "{:?}", began.elapsed());
    }

    #[test]
    fn a_start_that_fails_is_the_error_of_start() {
        let started = Monitor::start(
            "test-monitor",
            Cadence::default(),
            || -> Result<fn() -> ()> { Err(anyhow!("no collector")) },
            |_| {},
        );
        assert_eq!(started.err().unwrap().to_string(), "no collector");
    }

    #[test]
    fn dropping_it_stops_the_reports() {
        let (monitor, reports) = counting(slow());
        drop(monitor);
        let _ = reports.try_iter().count();
        assert!(reports.recv_timeout(Duration::from_millis(100)).is_err());
    }
}
