#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    // Fuzz protobuf decoding - should handle malformed data gracefully
    // This should never panic, only return errors for invalid input
    let _ = zombi::proto::Event::decode(data);
});
