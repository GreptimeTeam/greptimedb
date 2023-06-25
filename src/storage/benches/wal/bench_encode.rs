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

use criterion::{criterion_group, criterion_main, Criterion};
use storage::codec::Encoder;
use storage::write_batch::{codec, WriteBatch};

use crate::wal::util::gen_new_batch_and_types;

/*
-------------------------------------
                encode               |
-------------------------------------
rows |  protobuf    |    arrow       |
------------------------------------
10   |  4.8732 us    |  5.7388 us    |
------------------------------------
100  |  40.928 us    |  24.988 us    |
------------------------------------
10000|  425.69 us    |  229.74 us    |
------------------------------------
*/

fn encode_arrow(batch: &WriteBatch) {
    let encoder = codec::PayloadEncoder::new();
    let mut dst = vec![];
    let result = encoder.encode(batch.payload(), &mut dst);
    assert!(result.is_ok());
}

fn bench_wal_encode(c: &mut Criterion) {
    let (batch_10, _) = gen_new_batch_and_types(1);
    let (batch_100, _) = gen_new_batch_and_types(10);
    let (batch_10000, _) = gen_new_batch_and_types(100);

    let mut group = c.benchmark_group("wal_encode");
    let _ = group
        .bench_function("arrow_encode_with_10_num_rows", |b| {
            b.iter(|| encode_arrow(&batch_10))
        })
        .bench_function("arrow_encode_with_100_num_rows", |b| {
            b.iter(|| encode_arrow(&batch_100))
        })
        .bench_function("arrow_encode_with_10000_num_rows", |b| {
            b.iter(|| encode_arrow(&batch_10000))
        });
    group.finish();
}

criterion_group!(benches, bench_wal_encode);
criterion_main!(benches);
