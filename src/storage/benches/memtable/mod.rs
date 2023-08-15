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

pub mod bench_memtable_read;
pub mod bench_memtable_read_write_ratio;
pub mod bench_memtable_write;
pub mod util;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use api::v1::OpType;
use datatypes::prelude::ScalarVectorBuilder;
use datatypes::timestamp::TimestampMillisecond;
use datatypes::vectors::{
    StringVectorBuilder, TimestampMillisecondVectorBuilder, UInt64VectorBuilder,
};
use rand::distributions::Alphanumeric;
use rand::prelude::ThreadRng;
use rand::Rng;
use storage::memtable::KeyValues;
use store_api::storage::SequenceNumber;

static NEXT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn get_sequence() -> SequenceNumber {
    NEXT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

fn random_kv(rng: &mut ThreadRng, value_size: usize) -> ((i64, u64), (Option<u64>, String)) {
    let key0 = rng.gen_range(0..10000);
    let key1 = rng.gen::<u64>();
    let value1 = Some(rng.gen::<u64>());
    let value2 = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(value_size)
        .map(char::from)
        .collect();
    ((key0, key1), (value1, value2))
}
type KeyTuple = (i64, u64);
type ValueTuple = (Option<u64>, String);

fn random_kvs(len: usize, value_size: usize) -> (Vec<KeyTuple>, Vec<ValueTuple>) {
    let mut keys = Vec::with_capacity(len);
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        let mut rng = rand::thread_rng();
        let (key, value) = random_kv(&mut rng, value_size);
        keys.push(key);
        values.push(value);
    }
    (keys, values)
}

fn kvs_with_index(
    sequence: SequenceNumber,
    op_type: OpType,
    start_index_in_batch: usize,
    keys: &[(i64, u64)],
    values: &[(Option<u64>, String)],
) -> KeyValues {
    let mut key_builders = (
        TimestampMillisecondVectorBuilder::with_capacity(keys.len()),
        UInt64VectorBuilder::with_capacity(keys.len()),
    );
    for key in keys {
        key_builders.0.push(Some(TimestampMillisecond::from(key.0)));
        key_builders.1.push(Some(key.1));
    }
    let row_keys = vec![Arc::new(key_builders.1.finish()) as _];

    let mut value_builders = (
        UInt64VectorBuilder::with_capacity(values.len()),
        StringVectorBuilder::with_capacity(values.len()),
    );
    for value in values {
        value_builders.0.push(value.0);
        value_builders.1.push(Some(&value.1));
    }
    let row_values = vec![
        Arc::new(value_builders.0.finish()) as _,
        Arc::new(value_builders.1.finish()) as _,
    ];
    KeyValues {
        sequence,
        op_type,
        start_index_in_batch,
        keys: row_keys,
        values: row_values,
        timestamp: Some(Arc::new(key_builders.0.finish()) as _),
    }
}

fn generate_kv(kv_size: usize, start_index_in_batch: usize, value_size: usize) -> KeyValues {
    let (keys, values) = random_kvs(kv_size, value_size);
    kvs_with_index(
        get_sequence(),
        OpType::Put,
        start_index_in_batch,
        &keys,
        &values,
    )
}

fn generate_kvs(kv_size: usize, size: usize, value_size: usize) -> Vec<KeyValues> {
    (0..size)
        .map(|i| generate_kv(kv_size, i, value_size))
        .collect()
}
