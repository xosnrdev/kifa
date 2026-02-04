use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lib_kifa::FlushMode;
use lib_kifa::engine::{Config, StorageEngine};
use tempfile::TempDir;

const BATCH_SIZE: u64 = 10_000;
const SCAN_ENTRIES: usize = 50_000;
const PAYLOAD_SIZES: &[usize] = &[64, 256, 1024, 4000];
const MAX_PAYLOAD: usize = 4000;
const PAYLOAD_BUF: [u8; MAX_PAYLOAD] = [0xAB; MAX_PAYLOAD];
const MEASUREMENT_TIME: Duration = Duration::from_secs(10);

fn mode_name(mode: FlushMode) -> &'static str {
    match mode {
        FlushMode::Normal => "normal",
        FlushMode::Cautious => "cautious",
        FlushMode::Emergency => "emergency",
    }
}

fn open_engine(dir: &TempDir, config: Config) -> StorageEngine {
    StorageEngine::open(dir.path(), config).unwrap().0
}

fn append_throughput(c: &mut Criterion) {
    for mode in [FlushMode::Normal, FlushMode::Cautious, FlushMode::Emergency] {
        let mut group = c.benchmark_group(format!("append/{}", mode_name(mode)));
        group.sample_size(50);
        group.measurement_time(MEASUREMENT_TIME);
        group.throughput(Throughput::Elements(BATCH_SIZE));

        for &size in PAYLOAD_SIZES {
            let payload = &PAYLOAD_BUF[..size];

            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
                b.iter_custom(|iters| {
                    let dir = TempDir::new().unwrap();
                    let engine = open_engine(&dir, Config::default());
                    engine.set_flush_mode(mode);

                    let start = Instant::now();
                    for _ in 0..iters {
                        for _ in 0..BATCH_SIZE {
                            engine.append(payload).unwrap();
                        }
                    }
                    start.elapsed()
                });
            });
        }

        group.finish();
    }
}

fn append_no_compaction(c: &mut Criterion) {
    let mode = FlushMode::Cautious;
    let mut group = c.benchmark_group("append/cautious_no_compaction");
    group.sample_size(50);
    group.measurement_time(MEASUREMENT_TIME);
    group.throughput(Throughput::Elements(BATCH_SIZE));

    for &size in PAYLOAD_SIZES {
        let payload = &PAYLOAD_BUF[..size];

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter_custom(|iters| {
                let dir = TempDir::new().unwrap();
                let config = Config { compaction_enabled: false, ..Config::default() };
                let engine = open_engine(&dir, config);
                engine.set_flush_mode(mode);

                let start = Instant::now();
                for _ in 0..iters {
                    for _ in 0..BATCH_SIZE {
                        engine.append(payload).unwrap();
                    }
                }
                start.elapsed()
            });
        });
    }

    group.finish();
}

fn scan_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    group.sample_size(30);
    group.measurement_time(MEASUREMENT_TIME);
    group.throughput(Throughput::Elements(SCAN_ENTRIES as u64));

    let payload = &PAYLOAD_BUF[..256];

    let dir = TempDir::new().unwrap();
    let engine = open_engine(&dir, Config::default());
    for _ in 0..SCAN_ENTRIES {
        engine.append(payload).unwrap();
    }
    engine.flush().unwrap();
    drop(engine);

    group.bench_function("full_scan", |b| {
        b.iter(|| {
            let engine = open_engine(&dir, Config::default());
            let count = engine.entries().unwrap().count();
            assert_eq!(count, SCAN_ENTRIES);
        });
    });

    group.finish();
}

criterion_group!(benches, append_throughput, append_no_compaction, scan_throughput);
criterion_main!(benches);
