use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Instant,
};

use atomic_float::AtomicF64;
use criterion::{
    criterion_group, criterion_main, BatchSize, Bencher, BenchmarkId, Criterion, Throughput,
};
use rand::Rng;

use crate::memtable::{generate_kvs, util::bench_context::BenchContext};

static READ_NUM: AtomicUsize = AtomicUsize::new(0);
static WRITE_NUM: AtomicUsize = AtomicUsize::new(0);
static READ_SECS: AtomicF64 = AtomicF64::new(0.0);
static WRITE_SECS: AtomicF64 = AtomicF64::new(0.0);

struct Input {
    ratio: bool,
    kv_size: usize,
    batch_size: usize,
}

fn memtable_round(ctx: &BenchContext, input: &Input) {
    if input.ratio {
        let now = Instant::now();
        let read_count = ctx.read(input.batch_size);
        let d = now.elapsed();
        READ_SECS.fetch_add(
            d.as_secs() as f64 + d.subsec_nanos() as f64 * 1e-9,
            Ordering::Relaxed,
        );
        READ_NUM.fetch_add(read_count, Ordering::Relaxed);
    } else {
        generate_kvs(input.kv_size, input.batch_size, 20)
            .iter()
            .for_each(|kv| {
                let now = Instant::now();
                ctx.write(kv);
                let d = now.elapsed();
                WRITE_SECS.fetch_add(
                    d.as_secs() as f64 + d.subsec_nanos() as f64 * 1e-9,
                    Ordering::Relaxed,
                );
                WRITE_NUM.fetch_add(kv.len(), Ordering::Relaxed);
            });
    }
}

fn bench_read_write_ctx_frac(b: &mut Bencher<'_>, frac: &usize) {
    let frac = *frac;
    let ctx = Arc::new(BenchContext::default());
    let thread_ctx = ctx.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();

    let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !thread_stop.load(Ordering::Relaxed) {
            let f = rng.gen_range(0..=10);
            let input = Input {
                ratio: f < frac,
                kv_size: 100,
                batch_size: 1000,
            };
            memtable_round(&thread_ctx, &input);
        }
    });

    let mut rng = rand::thread_rng();
    b.iter_batched_ref(
        || {
            let f = rng.gen_range(0..=10);
            Input {
                ratio: f < frac,
                kv_size: 100,
                batch_size: 1000,
            }
        },
        |input| {
            memtable_round(&ctx, input);
        },
        BatchSize::SmallInput,
    );
    stop.store(true, Ordering::Relaxed);
    handle.join().unwrap();
}

#[allow(clippy::print_stdout)]
fn bench_memtable_read_write_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_read_write_ratio");
    for i in 0..=10 {
        READ_NUM.store(0, Ordering::Relaxed);
        WRITE_NUM.store(0, Ordering::Relaxed);
        READ_SECS.store(0.0, Ordering::Relaxed);
        WRITE_SECS.store(0.0, Ordering::Relaxed);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "read ratio: {:.2}% , write ratio: {:.2}%",
                i as f64 / 10_f64 * 100.0,
                (10 - i) as f64 / 10_f64 * 100.0,
            )),
            &i,
            bench_read_write_ctx_frac,
        );
        group.throughput(Throughput::Elements(100 * 1000));

        // the time is a little different the real time
        let read_num = READ_NUM.load(Ordering::Relaxed);
        let read_time = READ_SECS.load(Ordering::Relaxed);
        let read_tps = read_num as f64 / read_time as f64;
        let write_num = WRITE_NUM.load(Ordering::Relaxed);
        let write_time = WRITE_SECS.load(Ordering::Relaxed);
        let write_tps = write_num as f64 / write_time as f64;
        println!(
            "\nread numbers: {}, read thrpt: {}\nwrite numbers: {}, write thrpt {}\n",
            read_num, read_tps, write_num, write_tps
        );
    }
    group.finish();
}

criterion_group!(benches, bench_memtable_read_write_ratio);
criterion_main!(benches);
