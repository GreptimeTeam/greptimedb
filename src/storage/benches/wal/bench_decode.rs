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
use storage::codec::{Decoder, Encoder};
use storage::write_batch::{codec, WriteBatch};

use crate::wal::util::gen_new_batch_and_types;

/*
-------------------------------------
                decode               |
-------------------------------------
rows |  protobuf    |    arrow       |
------------------------------------
10   |  8.6485 us    |  8.8028 us    |
------------------------------------
100  |  63.850 us    |  46.174 us   |
------------------------------------
10000|  654.46 us    |  433.58 us    |
------------------------------------
*/

fn encode_arrow(batch: &WriteBatch, dst: &mut Vec<u8>) {
    let encoder = codec::PayloadEncoder::new();
    let result = encoder.encode(batch.payload(), dst);
    assert!(result.is_ok());
}

fn decode_arrow(dst: &[u8], mutation_types: &[i32]) {
    let decoder = codec::PayloadDecoder::new(mutation_types);
    let result = decoder.decode(dst);
    assert!(result.is_ok());
}

fn bench_wal_decode(c: &mut Criterion) {
    let (batch_10, types_10) = gen_new_batch_and_types(1);
    let (batch_100, types_100) = gen_new_batch_and_types(10);
    let (batch_10000, types_10000) = gen_new_batch_and_types(100);

    let mut dst_arrow_10 = vec![];
    let mut dst_arrow_100 = vec![];
    let mut dst_arrow_10000 = vec![];

    encode_arrow(&batch_10, &mut dst_arrow_10);
    encode_arrow(&batch_100, &mut dst_arrow_100);
    encode_arrow(&batch_10000, &mut dst_arrow_10000);

    let mut group = c.benchmark_group("wal_decode");
    let _ = group
        .bench_function("arrow_decode_with_10_num_rows", |b| {
            b.iter(|| decode_arrow(&dst_arrow_10, &types_10))
        })
        .bench_function("arrow_decode_with_100_num_rows", |b| {
            b.iter(|| decode_arrow(&dst_arrow_100, &types_100))
        })
        .bench_function("arrow_decode_with_10000_num_rows", |b| {
            b.iter(|| decode_arrow(&dst_arrow_10000, &types_10000))
        });
    group.finish();
}

criterion_group!(benches, bench_wal_decode);
criterion_main!(benches);
