pub mod api;
pub mod contracts;
pub mod flusher;
pub mod metrics;
pub mod storage;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/zombi.rs"));
}
