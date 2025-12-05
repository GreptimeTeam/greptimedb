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

use std::sync::Arc;

use common_function::scalars::vector::impl_conv::veclit_to_binlit;
use common_recordbatch::RecordBatch;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{BinaryVector, Helper};
use rand::Rng;
use table::test_util::MemTable;

use crate::QueryEngineRef;
use crate::tests::new_query_engine_with_table;

pub fn create_query_engine_for_vector10x3() -> QueryEngineRef {
    let mut column_schemas = vec![];
    let mut columns = vec![];
    let mut rng = rand::rng();

    let column_name = "vector";
    let column_schema = ColumnSchema::new(column_name, ConcreteDataType::binary_datatype(), true);
    column_schemas.push(column_schema);

    let vectors = (0..10)
        .map(|_| {
            let veclit = [
                rng.random_range(-100f32..100.0),
                rng.random_range(-100f32..100.0),
                rng.random_range(-100f32..100.0),
            ];
            veclit_to_binlit(&veclit)
        })
        .collect::<Vec<_>>();
    let column: VectorRef = Arc::new(BinaryVector::from(vectors));
    columns.push(column);

    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let vector_table = MemTable::table("vectors", recordbatch);
    new_query_engine_with_table(vector_table)
}

pub fn get_value_from_batches(column_name: &str, batches: Vec<RecordBatch>) -> Value {
    assert_eq!(1, batches.len());
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(1, batches[0].schema.num_columns());
    assert_eq!(column_name, batches[0].schema.column_schemas()[0].name);

    let batch = &batches[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(batch.column(0).len(), 1);
    let v = batch.column(0);
    assert_eq!(1, v.len());
    let v = Helper::try_into_vector(v).unwrap();
    v.get(0)
}
