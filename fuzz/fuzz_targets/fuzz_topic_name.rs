#![no_main]

use libfuzzer_sys::fuzz_target;
use tempfile::TempDir;
use zombi::contracts::HotStorage;
use zombi::storage::RocksDbStorage;

fuzz_target!(|data: &[u8]| {
    // Fuzz topic names with arbitrary strings
    // This tests handling of special characters, unicode, etc.
    let dir = TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();

    // Convert bytes to string (invalid UTF-8 becomes replacement chars)
    let topic = String::from_utf8_lossy(data);

    // Try write with fuzzed topic name - should not panic
    let _ = storage.write(&topic, 0, b"payload", 0, None);

    // Try read with fuzzed topic name - should not panic
    let _ = storage.read(&topic, 0, 0, 100);

    // Try high watermark with fuzzed topic name - should not panic
    let _ = storage.high_watermark(&topic, 0);
});
