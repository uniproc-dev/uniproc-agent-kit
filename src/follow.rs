//! Who follows what: watches that stream a key's status and holds that keep
//! a key followed for a while, on one side; on the other, the board the
//! thread that learns the statuses keeps, whatever it learns them from.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use futures_channel::mpsc;
use futures_core::Stream;

enum Request<K, S> {
    Watch {
        key: K,
        id: u64,
        tx: mpsc::UnboundedSender<S>,
    },
    Hold {
        key: K,
        id: u64,
        until: Instant,
    },
    Release {
        key: K,
        id: u64,
    },
}

struct Shared<K, S> {
    requests: Sender<Request<K, S>>,
    next_id: AtomicU64,
    wake: Box<dyn Fn() + Send + Sync>,
}

/// A board and the handle that asks it to follow keys. `wake` is called
/// after every request, to rouse the thread that keeps the board.
pub fn board<K, S>(wake: impl Fn() + Send + Sync + 'static) -> (Following<K, S>, Board<K, S>) {
    let (requests, queue) = crossbeam_channel::unbounded();
    let following = Following {
        shared: Arc::new(Shared {
            requests,
            next_id: AtomicU64::new(0),
            wake: Box::new(wake),
        }),
    };
    let board = Board {
        queue,
        entries: HashMap::new(),
    };
    (following, board)
}

/// Asks the board to follow keys; cheap to clone.
pub struct Following<K, S> {
    shared: Arc<Shared<K, S>>,
}

impl<K, S> Clone for Following<K, S> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<K, S> Following<K, S>
where
    K: Clone + Send + Sync + 'static,
    S: Send + 'static,
{
    /// The key's status now, then every change, until the stream is dropped.
    /// Ends when the key cannot be followed or is gone.
    pub fn watch(&self, key: K) -> Watch<S> {
        let (tx, rx) = mpsc::unbounded();
        let id = self.send(|id| Request::Watch {
            key: key.clone(),
            id,
            tx,
        });
        Watch {
            rx,
            _release: self.release(key, id),
        }
    }

    /// Follows the key for at most `span`, or until the hold is dropped without [`Hold::keep`].
    pub fn hold(&self, key: K, span: Duration) -> Hold {
        let until = Instant::now() + span;
        let id = self.send(|id| Request::Hold {
            key: key.clone(),
            id,
            until,
        });
        Hold(self.release(key, id))
    }

    fn send(&self, request: impl FnOnce(u64) -> Request<K, S>) -> u64 {
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        if self.shared.requests.send(request(id)).is_ok() {
            (self.shared.wake)();
        }
        id
    }

    fn release(&self, key: K, id: u64) -> Release {
        let shared = self.shared.clone();
        Release(Some(Box::new(move || {
            if shared.requests.send(Request::Release { key, id }).is_ok() {
                (shared.wake)();
            }
        })))
    }
}

struct Release(Option<Box<dyn FnOnce() + Send + Sync>>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            release();
        }
    }
}

/// A key's status: the current one first, then every change.
pub struct Watch<S> {
    rx: mpsc::UnboundedReceiver<S>,
    _release: Release,
}

impl<S> Stream for Watch<S> {
    type Item = S;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

/// Keeps a key followed while it is held, and after it with [`keep`](Self::keep).
pub struct Hold(Release);

impl Hold {
    /// Leaves the key followed until the hold's span ends.
    pub fn keep(mut self) {
        self.0.0 = None;
    }
}

struct Followed<S> {
    last: Option<S>,
    watchers: HashMap<u64, mpsc::UnboundedSender<S>>,
    holds: HashMap<u64, Instant>,
}

impl<S> Followed<S> {
    fn new() -> Self {
        Self {
            last: None,
            watchers: HashMap::new(),
            holds: HashMap::new(),
        }
    }

    fn is_followed(&self) -> bool {
        !self.watchers.is_empty() || !self.holds.is_empty()
    }
}

/// What the thread that learns the statuses keeps: who follows which key,
/// and the status each was last told.
pub struct Board<K, S> {
    queue: Receiver<Request<K, S>>,
    entries: HashMap<K, Followed<S>>,
}

impl<K, S> Board<K, S>
where
    K: Clone + Eq + Hash,
    S: Clone + PartialEq,
{
    /// Takes the requests made since the last call. `open` is asked about
    /// every key the board does not follow yet; false means it cannot be
    /// followed, and a watch on it ends at once.
    pub fn take_requests(&mut self, mut open: impl FnMut(&K) -> bool) {
        for request in self.queue.try_iter() {
            match request {
                Request::Watch { key, id, tx } => {
                    if let Some(followed) = follow(&mut self.entries, key, &mut open) {
                        if let Some(status) = &followed.last {
                            let _ = tx.unbounded_send(status.clone());
                        }
                        followed.watchers.insert(id, tx);
                    }
                }
                Request::Hold { key, id, until } => {
                    if let Some(followed) = follow(&mut self.entries, key, &mut open) {
                        followed.holds.insert(id, until);
                    }
                }
                Request::Release { key, id } => {
                    if let Some(followed) = self.entries.get_mut(&key) {
                        followed.watchers.remove(&id);
                        followed.holds.remove(&id);
                    }
                }
            }
        }
    }

    /// Hands `status` to the key's watchers unless it is the one they were
    /// last told; true when it was new.
    pub fn publish(&mut self, key: &K, status: S) -> bool {
        let Some(followed) = self.entries.get_mut(key) else {
            return false;
        };
        if followed.last.as_ref() == Some(&status) {
            return false;
        }
        followed
            .watchers
            .retain(|_, tx| tx.unbounded_send(status.clone()).is_ok());
        followed.last = Some(status);
        true
    }

    /// The status the key's watchers were last told.
    pub fn last(&self, key: &K) -> Option<&S> {
        self.entries.get(key).and_then(|f| f.last.as_ref())
    }

    /// Forgets the last status, so the next one goes out even if it is the same.
    pub fn forget(&mut self, key: &K) {
        if let Some(followed) = self.entries.get_mut(key) {
            followed.last = None;
        }
    }

    /// Ends every watch on the key and stops following it: it is gone.
    pub fn end(&mut self, key: &K) {
        self.entries.remove(key);
    }

    /// Drops the holds that ran out by `now` and stops following the keys
    /// nobody follows any more; returns those keys.
    pub fn sweep(&mut self, now: Instant) -> Vec<K> {
        for followed in self.entries.values_mut() {
            followed.holds.retain(|_, until| *until > now);
            followed.watchers.retain(|_, tx| !tx.is_closed());
        }
        let done: Vec<K> = self
            .entries
            .iter()
            .filter(|(_, f)| !f.is_followed())
            .map(|(key, _)| key.clone())
            .collect();
        for key in &done {
            self.entries.remove(key);
        }
        done
    }

    /// When the earliest hold runs out.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.entries
            .values()
            .flat_map(|f| f.holds.values().copied())
            .min()
    }

    /// The keys the board follows.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.keys()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn follow<'a, K: Eq + Hash + Clone, S>(
    entries: &'a mut HashMap<K, Followed<S>>,
    key: K,
    open: &mut impl FnMut(&K) -> bool,
) -> Option<&'a mut Followed<S>> {
    match entries.entry(key) {
        Entry::Occupied(entry) => Some(entry.into_mut()),
        Entry::Vacant(entry) => {
            if !open(entry.key()) {
                return None;
            }
            Some(entry.insert(Followed::new()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::atomic::AtomicUsize;

    fn new() -> (Following<&'static str, u32>, Board<&'static str, u32>, Arc<AtomicUsize>) {
        let woken = Arc::new(AtomicUsize::new(0));
        let (following, board) = board({
            let woken = woken.clone();
            move || {
                woken.fetch_add(1, Ordering::Relaxed);
            }
        });
        (following, board, woken)
    }

    fn open_all(_: &&'static str) -> bool {
        true
    }

    fn ready<S>(watch: &mut Watch<S>) -> Option<Option<S>> {
        futures::executor::block_on(async {
            futures::future::poll_fn(|cx| match watch.poll_next_unpin(cx) {
                Poll::Ready(item) => Poll::Ready(Some(item)),
                Poll::Pending => Poll::Ready(None),
            })
            .await
        })
    }

    #[test]
    fn every_request_wakes_the_board() {
        let (following, _board, woken) = new();
        let watch = following.watch("a");
        let hold = following.hold("a", Duration::from_secs(1));
        drop(watch);
        drop(hold);
        assert_eq!(woken.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn a_watch_gets_every_new_status_once() {
        let (following, mut board, _) = new();
        let mut watch = following.watch("a");
        board.take_requests(open_all);
        assert!(board.publish(&"a", 1));
        assert!(!board.publish(&"a", 1));
        assert!(board.publish(&"a", 2));
        assert_eq!(ready(&mut watch), Some(Some(1)));
        assert_eq!(ready(&mut watch), Some(Some(2)));
        assert_eq!(ready(&mut watch), None);
    }

    #[test]
    fn a_late_watch_starts_with_the_last_status() {
        let (following, mut board, _) = new();
        let _first = following.watch("a");
        board.take_requests(open_all);
        board.publish(&"a", 5);
        let mut late = following.watch("a");
        board.take_requests(|_| panic!("already followed"));
        assert_eq!(ready(&mut late), Some(Some(5)));
    }

    #[test]
    fn a_key_that_cannot_be_opened_ends_its_watch() {
        let (following, mut board, _) = new();
        let mut watch = following.watch("gone");
        board.take_requests(|_| false);
        assert_eq!(ready(&mut watch), Some(None));
        assert!(board.is_empty());
    }

    #[test]
    fn ending_a_key_ends_its_watches() {
        let (following, mut board, _) = new();
        let mut watch = following.watch("a");
        board.take_requests(open_all);
        board.end(&"a");
        assert_eq!(ready(&mut watch), Some(None));
    }

    #[test]
    fn a_dropped_watch_leaves_the_key_to_the_sweep() {
        let (following, mut board, _) = new();
        let watch = following.watch("a");
        board.take_requests(open_all);
        drop(watch);
        board.take_requests(open_all);
        assert_eq!(board.sweep(Instant::now()), vec!["a"]);
        assert!(board.is_empty());
    }

    #[test]
    fn a_hold_follows_until_it_runs_out() {
        let (following, mut board, _) = new();
        following.hold("a", Duration::from_secs(60)).keep();
        board.take_requests(open_all);
        let now = Instant::now();
        assert!(board.sweep(now).is_empty());
        let deadline = board.next_deadline().expect("a hold is pending");
        assert_eq!(board.sweep(deadline), vec!["a"]);
    }

    #[test]
    fn a_hold_dropped_without_keep_lets_go_at_once() {
        let (following, mut board, _) = new();
        let hold = following.hold("a", Duration::from_secs(60));
        board.take_requests(open_all);
        drop(hold);
        board.take_requests(open_all);
        assert_eq!(board.sweep(Instant::now()), vec!["a"]);
    }

    #[test]
    fn a_forgotten_status_goes_out_again() {
        let (following, mut board, _) = new();
        let mut watch = following.watch("a");
        board.take_requests(open_all);
        board.publish(&"a", 1);
        board.forget(&"a");
        assert!(board.publish(&"a", 1));
        assert_eq!(ready(&mut watch), Some(Some(1)));
        assert_eq!(ready(&mut watch), Some(Some(1)));
    }
}
