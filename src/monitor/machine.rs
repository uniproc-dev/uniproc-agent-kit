use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

/// Why a [`Collector`](super::Collector) is ticked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// The deadline the last tick named has passed; the first tick is due too.
    Due,
    /// Someone woke the monitor before that.
    Woken,
}

pub(super) enum Trigger {
    Wake,
    Stop,
}

/// Wakes a [`Monitor`](super::Monitor) from any thread. Wakes that come
/// before the monitor reads the first one make a single tick.
#[derive(Clone)]
pub struct Waker {
    tx: Sender<Trigger>,
    pending: Arc<AtomicBool>,
}

impl Waker {
    /// Asks for a tick now, within the monitor's spacing. Does nothing once it stopped.
    pub fn wake(&self) {
        if !self.pending.swap(true, Ordering::AcqRel) {
            let _ = self.tx.send(Trigger::Wake);
        }
    }

    pub(super) fn stop(&self) {
        let _ = self.tx.send(Trigger::Stop);
    }
}

pub(super) struct Inbox {
    rx: Receiver<Trigger>,
    pending: Arc<AtomicBool>,
}

enum Event {
    Wake,
    Stop,
    Deadline,
}

impl Inbox {
    fn wait(&self, until: Instant) -> Event {
        match self.rx.recv_timeout(until.saturating_duration_since(Instant::now())) {
            Ok(Trigger::Wake) => {
                self.pending.store(false, Ordering::Release);
                Event::Wake
            }
            Ok(Trigger::Stop) | Err(RecvTimeoutError::Disconnected) => Event::Stop,
            Err(RecvTimeoutError::Timeout) => Event::Deadline,
        }
    }
}

pub(super) fn channel() -> (Waker, Inbox) {
    let (tx, rx) = mpsc::channel();
    let pending = Arc::new(AtomicBool::new(false));
    (
        Waker {
            tx,
            pending: pending.clone(),
        },
        Inbox { rx, pending },
    )
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum State {
    Waiting { due: Instant, rested: Instant },
    Cooling { until: Instant },
    Ticking { why: Why },
    Stopped,
    Failed(String),
}

impl State {
    pub(super) fn is_over(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed(_))
    }
}

pub(super) fn next_state(
    state: State,
    inbox: &Inbox,
    spacing: Duration,
    tick: &mut dyn FnMut(Why) -> Instant,
) -> State {
    match state {
        State::Waiting { due, rested } => match inbox.wait(due) {
            Event::Wake if Instant::now() >= rested => State::Ticking { why: Why::Woken },
            Event::Wake => State::Cooling { until: rested },
            Event::Deadline => State::Ticking { why: Why::Due },
            Event::Stop => State::Stopped,
        },
        State::Cooling { until } => match inbox.wait(until) {
            Event::Wake => State::Cooling { until },
            Event::Deadline => State::Ticking { why: Why::Woken },
            Event::Stop => State::Stopped,
        },
        State::Ticking { why } => match catch_unwind(AssertUnwindSafe(|| tick(why))) {
            Ok(next) => {
                let rested = Instant::now() + spacing;
                State::Waiting {
                    due: next.max(rested),
                    rested,
                }
            }
            Err(panic) => State::Failed(message(panic)),
        },
        over @ (State::Stopped | State::Failed(_)) => over,
    }
}

fn message(panic: Box<dyn Any + Send>) -> String {
    match panic.downcast::<String>() {
        Ok(message) => *message,
        Err(panic) => panic
            .downcast_ref::<&str>()
            .map_or_else(|| "a panic without a message".to_string(), |message| message.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LONG: Duration = Duration::from_secs(60);

    fn queued(triggers: &[Trigger]) -> (Waker, Inbox) {
        let (waker, inbox) = channel();
        for trigger in triggers {
            match trigger {
                Trigger::Wake => waker.wake(),
                Trigger::Stop => waker.stop(),
            }
        }
        (waker, inbox)
    }

    fn untouched(_: Why) -> Instant {
        panic!("no tick expected")
    }

    fn waiting(due: Instant, rested: Instant) -> State {
        State::Waiting { due, rested }
    }

    #[test]
    fn waiting_past_the_deadline_ticks_as_due() {
        let (_waker, inbox) = queued(&[]);
        let now = Instant::now();
        let next = next_state(waiting(now, now), &inbox, LONG, &mut untouched);
        assert_eq!(next, State::Ticking { why: Why::Due });
    }

    #[test]
    fn a_wake_after_the_rest_ticks_at_once() {
        let (_waker, inbox) = queued(&[Trigger::Wake]);
        let now = Instant::now();
        let next = next_state(waiting(now + LONG, now), &inbox, LONG, &mut untouched);
        assert_eq!(next, State::Ticking { why: Why::Woken });
    }

    #[test]
    fn a_wake_within_the_rest_cools_down_until_it_ends() {
        let (_waker, inbox) = queued(&[Trigger::Wake]);
        let rested = Instant::now() + LONG;
        let next = next_state(waiting(rested + LONG, rested), &inbox, LONG, &mut untouched);
        assert_eq!(next, State::Cooling { until: rested });
    }

    #[test]
    fn a_stop_while_waiting_stops() {
        let (_waker, inbox) = queued(&[Trigger::Stop]);
        let now = Instant::now();
        let next = next_state(waiting(now + LONG, now), &inbox, LONG, &mut untouched);
        assert_eq!(next, State::Stopped);
    }

    #[test]
    fn a_stop_is_read_even_past_the_deadline() {
        let (_waker, inbox) = queued(&[Trigger::Stop]);
        let now = Instant::now();
        let next = next_state(waiting(now, now), &inbox, LONG, &mut untouched);
        assert_eq!(next, State::Stopped);
    }

    #[test]
    fn a_hung_up_channel_stops() {
        let (waker, inbox) = queued(&[]);
        drop(waker);
        let now = Instant::now();
        let next = next_state(waiting(now + LONG, now), &inbox, LONG, &mut untouched);
        assert_eq!(next, State::Stopped);
    }

    #[test]
    fn a_wake_while_cooling_keeps_cooling() {
        let (_waker, inbox) = queued(&[Trigger::Wake]);
        let until = Instant::now() + LONG;
        let next = next_state(State::Cooling { until }, &inbox, LONG, &mut untouched);
        assert_eq!(next, State::Cooling { until });
    }

    #[test]
    fn a_cooldown_that_ends_ticks_as_woken() {
        let (_waker, inbox) = queued(&[]);
        let next = next_state(State::Cooling { until: Instant::now() }, &inbox, LONG, &mut untouched);
        assert_eq!(next, State::Ticking { why: Why::Woken });
    }

    #[test]
    fn a_stop_while_cooling_stops() {
        let (_waker, inbox) = queued(&[Trigger::Stop]);
        let until = Instant::now() + LONG;
        let next = next_state(State::Cooling { until }, &inbox, LONG, &mut untouched);
        assert_eq!(next, State::Stopped);
    }

    #[test]
    fn a_tick_is_told_why_and_names_the_next_deadline() {
        let (_waker, inbox) = queued(&[]);
        let spacing = Duration::from_millis(10);
        let wanted = Instant::now() + LONG;
        let mut told = None;
        let before = Instant::now();
        let next = next_state(State::Ticking { why: Why::Woken }, &inbox, spacing, &mut |why| {
            told = Some(why);
            wanted
        });
        assert_eq!(told, Some(Why::Woken));
        let State::Waiting { due, rested } = next else {
            panic!("{next:?}")
        };
        assert_eq!(due, wanted);
        assert!(rested >= before + spacing && rested <= Instant::now() + spacing);
    }

    #[test]
    fn a_deadline_sooner_than_the_spacing_waits_the_spacing() {
        let (_waker, inbox) = queued(&[]);
        let next = next_state(
            State::Ticking { why: Why::Due },
            &inbox,
            Duration::from_millis(10),
            &mut |_| Instant::now(),
        );
        let State::Waiting { due, rested } = next else {
            panic!("{next:?}")
        };
        assert_eq!(due, rested);
    }

    #[test]
    fn a_tick_that_panics_fails_with_its_message() {
        let (_waker, inbox) = queued(&[]);
        let next = next_state(State::Ticking { why: Why::Due }, &inbox, LONG, &mut |_| panic!("boom"));
        assert_eq!(next, State::Failed("boom".into()));
        let formatted = next_state(State::Ticking { why: Why::Due }, &inbox, LONG, &mut |why| {
            panic!("{why:?}")
        });
        assert_eq!(formatted, State::Failed("Due".into()));
    }

    #[test]
    fn the_final_states_stay_without_reading_the_channel() {
        let (_waker, inbox) = queued(&[Trigger::Wake]);
        assert_eq!(next_state(State::Stopped, &inbox, LONG, &mut untouched), State::Stopped);
        let failed = State::Failed("gone".into());
        assert_eq!(next_state(failed, &inbox, LONG, &mut untouched), State::Failed("gone".into()));
        assert!(inbox.rx.try_recv().is_ok(), "the wake is still queued");
    }

    #[test]
    fn wakes_before_the_first_is_read_are_one_and_the_next_is_sent_again() {
        let (waker, inbox) = queued(&[Trigger::Wake, Trigger::Wake, Trigger::Wake]);
        let now = Instant::now();
        assert!(matches!(inbox.wait(now), Event::Wake));
        assert!(matches!(inbox.wait(now), Event::Deadline));
        waker.wake();
        assert!(matches!(inbox.wait(now), Event::Wake));
    }
}
