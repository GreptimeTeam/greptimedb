use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

use criterion::{criterion_group, criterion_main, BatchSize, Bencher, BenchmarkId, Criterion};
use rand::Rng;

use crate::memtable::{generate_kvs, util::bench_context::BenchContext};

fn memtable_round(ctx: &BenchContext, case: &(bool, usize, usize)) {
    if case.0 {
        ctx.read(case.2);
    } else {
        generate_kvs(case.1, case.2, 20)
            .iter()
            .for_each(|kv| ctx.write(kv));
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
        while !thread_stop.load(Ordering::SeqCst) {
            let f = rng.gen_range(0..10);
            let case = (f < frac, 100, 1000);
            memtable_round(&thread_ctx, &case);
        }
    });
    let mut rng = rand::thread_rng();
    b.iter_batched_ref(
        || {
            let f = rng.gen_range(0..10);
            (f < frac, 100, 1000)
        },
        |case| {
            memtable_round(&ctx, case);
        },
        BatchSize::SmallInput,
    );
    stop.store(true, Ordering::SeqCst);
    handle.join().unwrap();
}

fn bench_memtable_read_write_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_read_write_ratio");
    for i in 0..=10 {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "read ratio: {:.2}% , write ratio: {:.2}%",
                i as f64 / 10_f64 * 100.0,
                (10 - i) as f64 / 10_f64 * 100.0,
            )),
            &i,
            bench_read_write_ctx_frac,
        );
    }
    group.finish();
}

criterion_group!(benches, bench_memtable_read_write_ratio);
criterion_main!(benches);
