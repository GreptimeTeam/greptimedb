use criterion::{criterion_group, criterion_main, Criterion};
use storage::codec::{Decoder, Encoder};
use storage::write_batch::{codec, WriteBatch};

use super::util::gen_new_batch_and_extras;

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

fn encode_arrow(
    batch: &WriteBatch,
    mutation_extras: &[storage::proto::wal::MutationExtra],
    dst: &mut Vec<u8>,
) {
    let encoder = codec::WriteBatchArrowEncoder::new(mutation_extras.to_vec());
    let result = encoder.encode(batch, dst);
    assert!(result.is_ok());
}

fn encode_protobuf(batch: &WriteBatch, dst: &mut Vec<u8>) {
    let encoder = codec::WriteBatchProtobufEncoder {};
    let result = encoder.encode(batch, dst);
    assert!(result.is_ok());
}

fn decode_arrow(dst: &[u8], mutation_extras: &[storage::proto::wal::MutationExtra]) {
    let decoder = codec::WriteBatchArrowDecoder::new(mutation_extras.to_vec());
    let result = decoder.decode(dst);
    assert!(result.is_ok());
}

fn decode_protobuf(dst: &[u8], mutation_extras: &[storage::proto::wal::MutationExtra]) {
    let decoder = codec::WriteBatchProtobufDecoder::new(mutation_extras.to_vec());
    let result = decoder.decode(dst);
    assert!(result.is_ok());
}

fn bench_wal_decode(c: &mut Criterion) {
    let (batch_10, extras_10) = gen_new_batch_and_extras(1);
    let (batch_100, extras_100) = gen_new_batch_and_extras(10);
    let (batch_10000, extras_10000) = gen_new_batch_and_extras(100);
    let mut dst_protobuf_10 = vec![];
    let mut dst_protobuf_100 = vec![];
    let mut dst_protobuf_10000 = vec![];

    let mut dst_arrow_10 = vec![];
    let mut dst_arrow_100 = vec![];
    let mut dst_arrow_10000 = vec![];

    encode_protobuf(&batch_10, &mut dst_protobuf_10);
    encode_protobuf(&batch_100, &mut dst_protobuf_100);
    encode_protobuf(&batch_10000, &mut dst_protobuf_10000);

    encode_arrow(&batch_10, &extras_10, &mut dst_arrow_10);
    encode_arrow(&batch_100, &extras_100, &mut dst_arrow_100);
    encode_arrow(&batch_10000, &extras_10000, &mut dst_arrow_10000);

    let mut group = c.benchmark_group("wal_decode");
    group.bench_function("protobuf_decode_with_10_num_rows", |b| {
        b.iter(|| decode_protobuf(&dst_protobuf_10, &extras_10))
    });
    group.bench_function("protobuf_decode_with_100_num_rows", |b| {
        b.iter(|| decode_protobuf(&dst_protobuf_100, &extras_100))
    });
    group.bench_function("protobuf_decode_with_10000_num_rows", |b| {
        b.iter(|| decode_protobuf(&dst_protobuf_10000, &extras_10000))
    });
    group.bench_function("arrow_decode_with_10_num_rows", |b| {
        b.iter(|| decode_arrow(&dst_arrow_10, &extras_10))
    });
    group.bench_function("arrow_decode_with_100_num_rows", |b| {
        b.iter(|| decode_arrow(&dst_arrow_100, &extras_100))
    });
    group.bench_function("arrow_decode_with_10000_num_rows", |b| {
        b.iter(|| decode_arrow(&dst_arrow_10000, &extras_10000))
    });
    group.finish();
}

criterion_group!(benches, bench_wal_decode);
criterion_main!(benches);
