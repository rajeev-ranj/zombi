//! Benchmarks for Zombi storage system.
//!
//! Run with: cargo bench
//! View results in: target/criterion/report/index.html

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;
use zombi::contracts::HotStorage;
use zombi::storage::RocksDbStorage;

fn create_storage() -> (RocksDbStorage, TempDir) {
    let dir = TempDir::new().unwrap();
    let storage = RocksDbStorage::open(dir.path()).unwrap();
    (storage, dir)
}

// =============================================================================
// Write Benchmarks
// =============================================================================

fn bench_write_single_event(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_single");

    // Benchmark different payload sizes
    for size in [64, 256, 1024, 4096].iter() {
        let (storage, _dir) = create_storage();
        let payload = vec![0u8; *size];

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                storage
                    .write("bench-topic", 0, black_box(&payload), 0, None)
                    .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_write_with_idempotency(c: &mut Criterion) {
    let (storage, _dir) = create_storage();
    let payload = vec![0u8; 1024];
    let mut counter = 0u64;

    c.bench_function("write_1kb_with_idempotency_key", |b| {
        b.iter(|| {
            counter += 1;
            let key = format!("key-{}", counter);
            storage
                .write("bench-topic", 0, black_box(&payload), 0, Some(&key))
                .unwrap()
        });
    });
}

fn bench_write_burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_burst");

    for batch_size in [10, 100, 1000].iter() {
        let (storage, _dir) = create_storage();
        let payload = vec![0u8; 256];

        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| {
                    for _ in 0..size {
                        storage
                            .write("bench-topic", 0, black_box(&payload), 0, None)
                            .unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_write_multiple_partitions(c: &mut Criterion) {
    let (storage, _dir) = create_storage();
    let payload = vec![0u8; 256];
    let mut partition = 0u32;

    c.bench_function("write_round_robin_4_partitions", |b| {
        b.iter(|| {
            partition = (partition + 1) % 4;
            storage
                .write("bench-topic", partition, black_box(&payload), 0, None)
                .unwrap()
        });
    });
}

// =============================================================================
// Read Benchmarks
// =============================================================================

fn bench_read_single(c: &mut Criterion) {
    let (storage, _dir) = create_storage();

    // Pre-populate with data
    for i in 0..10000 {
        storage
            .write("bench-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
            .unwrap();
    }

    c.bench_function("read_single_event", |b| {
        b.iter(|| storage.read("bench-topic", 0, black_box(5000), 1).unwrap());
    });
}

fn bench_read_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_batch");

    let (storage, _dir) = create_storage();

    // Pre-populate with data
    for i in 0..10000 {
        storage
            .write("bench-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
            .unwrap();
    }

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| storage.read("bench-topic", 0, black_box(0), size).unwrap());
            },
        );
    }

    group.finish();
}

fn bench_read_from_middle(c: &mut Criterion) {
    let (storage, _dir) = create_storage();

    // Pre-populate with data
    for i in 0..10000 {
        storage
            .write("bench-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
            .unwrap();
    }

    c.bench_function("read_100_from_offset_5000", |b| {
        b.iter(|| {
            storage
                .read("bench-topic", 0, black_box(5000), 100)
                .unwrap()
        });
    });
}

// =============================================================================
// Mixed Read/Write Benchmarks
// =============================================================================

fn bench_mixed_workload(c: &mut Criterion) {
    let (storage, _dir) = create_storage();
    let payload = vec![0u8; 256];

    // Pre-populate with some data
    for i in 0..1000 {
        storage
            .write("bench-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
            .unwrap();
    }

    let mut counter = 0u64;

    c.bench_function("mixed_80_write_20_read", |b| {
        b.iter(|| {
            counter += 1;
            if counter.is_multiple_of(5) {
                // 20% reads
                storage.read("bench-topic", 0, black_box(0), 10).unwrap();
            } else {
                // 80% writes
                storage
                    .write("bench-topic", 0, black_box(&payload), 0, None)
                    .unwrap();
            }
        });
    });
}

// =============================================================================
// Consumer Offset Benchmarks
// =============================================================================

fn bench_commit_offset(c: &mut Criterion) {
    let (storage, _dir) = create_storage();
    let mut offset = 0u64;

    c.bench_function("commit_offset", |b| {
        b.iter(|| {
            offset += 1;
            storage
                .commit_offset("bench-group", "bench-topic", 0, black_box(offset))
                .unwrap()
        });
    });
}

fn bench_get_offset(c: &mut Criterion) {
    let (storage, _dir) = create_storage();

    // Pre-commit an offset
    storage
        .commit_offset("bench-group", "bench-topic", 0, 1000)
        .unwrap();

    c.bench_function("get_offset", |b| {
        b.iter(|| {
            storage
                .get_offset("bench-group", "bench-topic", black_box(0))
                .unwrap()
        });
    });
}

// =============================================================================
// High Watermark Benchmarks
// =============================================================================

fn bench_high_watermark(c: &mut Criterion) {
    let (storage, _dir) = create_storage();

    // Pre-populate with data
    for i in 0..1000 {
        storage
            .write("bench-topic", 0, format!("event-{}", i).as_bytes(), 0, None)
            .unwrap();
    }

    c.bench_function("high_watermark", |b| {
        b.iter(|| storage.high_watermark("bench-topic", black_box(0)).unwrap());
    });
}

// =============================================================================
// Criterion Groups
// =============================================================================

criterion_group!(
    write_benches,
    bench_write_single_event,
    bench_write_with_idempotency,
    bench_write_burst,
    bench_write_multiple_partitions,
);

criterion_group!(
    read_benches,
    bench_read_single,
    bench_read_batch,
    bench_read_from_middle,
);

criterion_group!(mixed_benches, bench_mixed_workload,);

criterion_group!(
    consumer_benches,
    bench_commit_offset,
    bench_get_offset,
    bench_high_watermark,
);

criterion_main!(write_benches, read_benches, mixed_benches, consumer_benches);
