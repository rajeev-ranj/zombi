#![no_main]

use libfuzzer_sys::fuzz_target;
use tempfile::TempDir;
use zombi::contracts::HotStorage;
use zombi::storage::RocksDbStorage;

fuzz_target!(|data: &[u8]| {
    // Create a fresh storage for each fuzz iteration
    let dir = TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();

    // Fuzz the write path with arbitrary payloads
    // This should never panic, regardless of input
    let _ = storage.write("fuzz-topic", 0, data, 0, None);

    // Also try reading back - should handle any state gracefully
    let _ = storage.read("fuzz-topic", 0, 0, 100);
});
