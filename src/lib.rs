pub mod api;
pub mod contracts;
pub mod flusher;
pub mod storage;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/zombi.rs"));
}
