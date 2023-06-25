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

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use crate::memtable::generate_kvs;
use crate::memtable::util::bench_context::BenchContext;

fn bench_memtable_read(c: &mut Criterion) {
    // the length of string in value is 20
    let kvs = generate_kvs(10, 10000, 20);
    let ctx = BenchContext::new();
    kvs.iter().for_each(|kv| ctx.write(kv));
    let mut group = c.benchmark_group("memtable_read");
    let _ = group
        .throughput(Throughput::Elements(10 * 10000))
        .bench_function("read", |b| b.iter(|| ctx.read(100)));
    group.finish();
}

criterion_group!(benches, bench_memtable_read);
criterion_main!(benches);
