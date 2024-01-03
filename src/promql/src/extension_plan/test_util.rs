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

//! Utils for testing extension plan

use std::sync::Arc;

use common_recordbatch::DfRecordBatch as RecordBatch;
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, TimestampMillisecondType,
};
use datafusion::physical_plan::memory::MemoryExec;
use datatypes::arrow::array::TimestampMillisecondArray;
use datatypes::arrow_array::StringArray;

pub(crate) const TIME_INDEX_COLUMN: &str = "timestamp";

pub(crate) fn prepare_test_data() -> MemoryExec {
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
        Field::new("value", DataType::Float64, true),
        Field::new("path", DataType::Utf8, true),
    ]));
    let timestamp_column = Arc::new(TimestampMillisecondArray::from(vec![
        0, 30_000, 60_000, 90_000, 120_000, // every 30s
        180_000, 240_000, // every 60s
        241_000, 271_000, 291_000, // others
    ])) as _;
    let field_column = Arc::new(Float64Array::from(vec![1.0; 10])) as _;
    let path_column = Arc::new(StringArray::from(vec!["foo"; 10])) as _;
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![timestamp_column, field_column, path_column],
    )
    .unwrap();

    MemoryExec::try_new(&[vec![data]], schema, None).unwrap()
}

pub(crate) fn prepare_test_data_with_nan() -> MemoryExec {
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let timestamp_column = Arc::new(TimestampMillisecondArray::from(vec![
        0, 30_000, 60_000, 90_000, 120_000, // every 30s
    ])) as _;
    let field_column = Arc::new(Float64Array::from(vec![0.0, f64::NAN, 6.0, f64::NAN, 12.0])) as _;
    let data = RecordBatch::try_new(schema.clone(), vec![timestamp_column, field_column]).unwrap();

    MemoryExec::try_new(&[vec![data]], schema, None).unwrap()
}
