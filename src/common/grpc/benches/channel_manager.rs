use common_grpc::channel_manager::ChannelManager;
use criterion::{criterion_group, criterion_main, Criterion};

#[tokio::main]
async fn do_bench_channel_manager() {
    let m = ChannelManager::new();
    let task_count = 8;
    let mut joins = Vec::with_capacity(task_count);

    for _ in 0..task_count {
        let m_clone = m.clone();
        let join = tokio::spawn(async move {
            for _ in 0..10000 {
                let idx = rand::random::<usize>() % 100;
                let ret = m_clone.get(format!("{}", idx));
                assert!(ret.is_ok());
            }
        });
        joins.push(join);
    }

    for join in joins {
        let _ = join.await;
    }
}

fn bench_channel_manager(c: &mut Criterion) {
    c.bench_function("bench channel manager", |b| {
        b.iter(do_bench_channel_manager);
    });
}

criterion_group!(benches, bench_channel_manager);
criterion_main!(benches);
