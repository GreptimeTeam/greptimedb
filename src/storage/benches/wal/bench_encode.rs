#![allow(clippy::derive_partial_eq_without_eq)]

use criterion::{criterion_group, criterion_main, Criterion};
use storage::codec::Encoder;
use storage::write_batch::{codec, WriteBatch};

use super::util::gen_new_batch_and_extras;

tonic::include_proto!("greptime.storage.wal.v1");

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

fn encode_arrow(batch: &WriteBatch, mutation_extras: &[storage::proto::wal::MutationExtra]) {
    let encoder = codec::WriteBatchArrowEncoder::new(mutation_extras.to_vec());
    let mut dst = vec![];
    let result = encoder.encode(batch, &mut dst);
    assert!(result.is_ok());
}

fn encode_protobuf(batch: &WriteBatch, _mutation_extras: &[storage::proto::wal::MutationExtra]) {
    let encoder = codec::WriteBatchProtobufEncoder {};
    let mut dst = vec![];
    let result = encoder.encode(batch, &mut dst);
    assert!(result.is_ok());
}

fn bench_wal_encode(c: &mut Criterion) {
    let (batch_10, extras_10) = gen_new_batch_and_extras(1);
    let (batch_100, extras_100) = gen_new_batch_and_extras(10);
    let (batch_10000, extras_10000) = gen_new_batch_and_extras(100);

    let mut group = c.benchmark_group("wal_encode");
    group.bench_function("protobuf_encode_with_10_num_rows", |b| {
        b.iter(|| encode_protobuf(&batch_10, &extras_10))
    });
    group.bench_function("protobuf_encode_with_100_num_rows", |b| {
        b.iter(|| encode_protobuf(&batch_100, &extras_100))
    });
    group.bench_function("protobuf_encode_with_10000_num_rows", |b| {
        b.iter(|| encode_protobuf(&batch_10000, &extras_10000))
    });
    group.bench_function("arrow_encode_with_10_num_rows", |b| {
        b.iter(|| encode_arrow(&batch_10, &extras_10))
    });
    group.bench_function("arrow_encode_with_100_num_rows", |b| {
        b.iter(|| encode_arrow(&batch_100, &extras_100))
    });
    group.bench_function("arrow_encode_with_10000_num_rows", |b| {
        b.iter(|| encode_arrow(&batch_10000, &extras_10000))
    });
    group.finish();
}

criterion_group!(benches, bench_wal_encode);
criterion_main!(benches);
