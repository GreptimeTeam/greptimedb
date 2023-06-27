// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
                let ret = m_clone.get(format!("{idx}"));
                let _ = ret.unwrap();
            }
        });
        joins.push(join);
    }

    for join in joins {
        let _ = join.await;
    }
}

fn bench_channel_manager(c: &mut Criterion) {
    let _ = c.bench_function("bench channel manager", |b| {
        b.iter(do_bench_channel_manager);
    });
}

criterion_group!(benches, bench_channel_manager);
criterion_main!(benches);
