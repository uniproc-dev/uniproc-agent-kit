//! Pieces the uniproc agents share, whatever the OS underneath:
//! tagged values for conditional reads, a thread that ticks a collector
//! on its own schedule and on wakes, the latest of its reports, a worker pool that runs one job per target, a board of who
//! follows what, and a counter futures can wait on.

pub mod follow;
pub mod latest;
pub mod monitor;
pub mod notify;
pub mod runner;
pub mod tag;

pub use follow::{Board, Following, Hold, Watch, board};
pub use latest::Latest;
pub use monitor::{Collector, Monitor, Waker, Why};
pub use notify::Notify;
pub use runner::{Busy, Runner};
pub use tag::{Epoch, Tagged, Versioned};
