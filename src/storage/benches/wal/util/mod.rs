use std::sync::Arc;

use datatypes::{
    type_id::LogicalTypeId,
    vectors::{BooleanVector, Int64Vector, UInt64Vector},
};
use rand::Rng;
use storage::{
    proto,
    write_batch::{PutData, WriteBatch},
};
use store_api::storage::{consts, PutOperation, WriteRequest};

pub mod write_batch_util;

pub fn new_test_batch() -> WriteBatch {
    write_batch_util::new_write_batch(
        &[
            ("k1", LogicalTypeId::UInt64, false),
            (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
            ("ts", LogicalTypeId::Int64, false),
            ("v1", LogicalTypeId::Boolean, true),
            ("4", LogicalTypeId::Int64, false),
            ("5", LogicalTypeId::Int64, false),
            ("6", LogicalTypeId::Int64, false),
            ("7", LogicalTypeId::Int64, false),
            ("8", LogicalTypeId::Int64, false),
            ("9", LogicalTypeId::Int64, false),
            ("10", LogicalTypeId::Int64, false),
        ],
        Some(2),
    )
}

pub fn gen_new_batch_and_extras(
    putdate_nums: usize,
) -> (WriteBatch, Vec<storage::proto::wal::MutationExtra>) {
    let mut batch = new_test_batch();
    let mut rng = rand::thread_rng();

    for _ in 0..putdate_nums {
        let mut intvs = [0u64; 10];
        let mut boolvs = [true; 10];
        let mut tsvs = [0i64; 10];

        rng.fill(&mut intvs[..]);
        rng.fill(&mut boolvs[..]);
        rng.fill(&mut tsvs[..]);

        let intv = Arc::new(UInt64Vector::from_slice(&intvs));
        let boolv = Arc::new(BooleanVector::from(boolvs.to_vec()));
        let tsv = Arc::new(Int64Vector::from_slice(&tsvs));

        let mut put_data = PutData::default();
        put_data.add_key_column("k1", intv.clone()).unwrap();
        put_data.add_version_column(intv).unwrap();
        put_data.add_value_column("v1", boolv).unwrap();
        put_data.add_key_column("ts", tsv.clone()).unwrap();
        put_data.add_key_column("4", tsv.clone()).unwrap();
        put_data.add_key_column("5", tsv.clone()).unwrap();
        put_data.add_key_column("6", tsv.clone()).unwrap();
        put_data.add_key_column("7", tsv.clone()).unwrap();
        put_data.add_key_column("8", tsv.clone()).unwrap();
        put_data.add_key_column("9", tsv.clone()).unwrap();
        put_data.add_key_column("10", tsv.clone()).unwrap();

        batch.put(put_data).unwrap();
    }

    let extras = proto::wal::gen_mutation_extras(&batch);

    (batch, extras)
}
