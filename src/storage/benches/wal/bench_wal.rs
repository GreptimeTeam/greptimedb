use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{BooleanVector, Int64Vector, UInt64Vector};
use storage::codec::{Decoder, Encoder};
use storage::proto;
use storage::write_batch::{codec, PutData, WriteBatch};
use store_api::storage::{consts, PutOperation, WriteRequest};

use super::util::write_batch_util;

tonic::include_proto!("greptime.storage.wal.v1");

fn new_test_batch() -> WriteBatch {
    write_batch_util::new_write_batch(
        &[
            ("k1", LogicalTypeId::UInt64, false),
            (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
            ("ts", LogicalTypeId::Int64, false),
            ("v1", LogicalTypeId::Boolean, true),
        ],
        Some(2),
    )
}

fn gen_new_batch_and_extras() -> (WriteBatch, Vec<storage::proto::wal::MutationExtra>) {
    let mut batch = new_test_batch();
    for i in 0..10 {
        let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3]));
        let boolv = Arc::new(BooleanVector::from(vec![Some(true), Some(false), None]));
        let tsv = Arc::new(Int64Vector::from_vec(vec![i, i, i]));

        let mut put_data = PutData::default();
        put_data.add_key_column("k1", intv.clone()).unwrap();
        put_data.add_version_column(intv).unwrap();
        put_data.add_value_column("v1", boolv).unwrap();
        put_data.add_key_column("ts", tsv).unwrap();

        batch.put(put_data).unwrap();
    }

    let extras = proto::wal::gen_mutation_extras(&batch);

    (batch, extras)
}

fn codec_arrow() {
    let (batch, mutation_extras) = gen_new_batch_and_extras();
    let encoder = codec::WriteBatchArrowEncoder::new(mutation_extras.clone());
    let mut dst = vec![];
    encoder.encode(&batch, &mut dst);

    let decoder = codec::WriteBatchArrowDecoder::new(mutation_extras);
    decoder.decode(&dst);
}

fn codec_protobuf() {
    let (batch, mutation_extras) = gen_new_batch_and_extras();
    let encoder = codec::WriteBatchProtobufEncoder {};
    let mut dst = vec![];
    encoder.encode(&batch, &mut dst);

    let decoder = codec::WriteBatchProtobufDecoder::new(mutation_extras);
    decoder.decode(&dst);
}

fn bench_wal_protobuf(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_encode_decode");
    group.bench_function("protobuf", |b| b.iter(|| codec_protobuf()));
    group.bench_function("arrow", |b| b.iter(|| codec_arrow()));
    group.finish();
}

criterion_group!(benches, bench_wal_protobuf);
criterion_main!(benches);
