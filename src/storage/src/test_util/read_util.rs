use std::sync::Arc;

use async_trait::async_trait;
use datatypes::prelude::ScalarVector;
use datatypes::vectors::{Int64Vector, UInt64Vector, UInt8Vector};

use crate::error::Result;
use crate::read::{Batch, BatchReader, BoxedBatchReader};

/// Build a new batch, with 0 sequence and value type.
fn new_kv_batch(key_values: &[(i64, Option<i64>)]) -> Batch {
    let key = Arc::new(Int64Vector::from_values(key_values.iter().map(|v| v.0)));
    let value = Arc::new(Int64Vector::from_iter(key_values.iter().map(|v| v.1)));
    let sequences = UInt64Vector::from_vec(vec![0; key_values.len()]);
    let value_types = UInt8Vector::from_vec(vec![0; key_values.len()]);

    Batch {
        keys: vec![key],
        sequences,
        value_types,
        values: vec![value],
    }
}

fn check_kv_batch(batches: &[Batch], expect: &[&[(i64, Option<i64>)]]) {
    for (batch, key_values) in batches.iter().zip(expect.iter()) {
        let key = batch.keys[0]
            .as_any()
            .downcast_ref::<Int64Vector>()
            .unwrap();
        let value = batch.values[0]
            .as_any()
            .downcast_ref::<Int64Vector>()
            .unwrap();

        for (i, (k, v)) in key_values.iter().enumerate() {
            assert_eq!(key.get_data(i).unwrap(), *k,);
            assert_eq!(value.get_data(i), *v,);
        }
    }
    assert_eq!(batches.len(), expect.len());
}

pub async fn check_reader_with_kv_batch(
    reader: &mut dyn BatchReader,
    expect: &[&[(i64, Option<i64>)]],
) {
    let mut result = Vec::new();
    while let Some(batch) = reader.next_batch().await.unwrap() {
        result.push(batch);
    }

    check_kv_batch(&result, expect);
}

/// A reader for test that takes batch from Vec.
pub struct VecBatchReader {
    batches: Vec<Batch>,
}

impl VecBatchReader {
    fn new(mut batches: Vec<Batch>) -> VecBatchReader {
        batches.reverse();

        VecBatchReader { batches }
    }
}

#[async_trait]
impl BatchReader for VecBatchReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        Ok(self.batches.pop())
    }
}

pub fn build_vec_reader(batches: &[&[(i64, Option<i64>)]]) -> VecBatchReader {
    let batches: Vec<_> = batches
        .iter()
        .filter(|key_values| !key_values.is_empty())
        .map(|key_values| new_kv_batch(key_values))
        .collect();

    VecBatchReader::new(batches)
}

pub fn build_boxed_vec_reader(batches: &[&[(i64, Option<i64>)]]) -> BoxedBatchReader {
    Box::new(build_vec_reader(batches))
}
