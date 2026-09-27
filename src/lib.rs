//! Pieces the uniproc agents share, whatever the OS underneath:
//! tagged values for conditional reads, a thread that reports as things
//! change and the latest of its reports, a worker pool that runs one job per target, and a board of who
//! follows what.

pub mod follow;
pub mod latest;
pub mod monitor;
pub mod runner;
pub mod tag;

pub use follow::{Board, Following, Hold, Watch, board};
pub use latest::Latest;
pub use monitor::{Cadence, Monitor};
pub use runner::{Busy, Runner};
pub use tag::{Epoch, Tagged, Versioned};
