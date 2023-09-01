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

use common_recordbatch::RecordBatch;
use datatypes::for_all_primitive_types;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::Int32Vector;
use function::{create_query_engine, get_numbers_from_table};
use num_traits::AsPrimitive;
use table::test_util::MemTable;

use super::new_query_engine_with_table;
use crate::error::Result;
use crate::tests::{exec_selection, function};
use crate::QueryEngine;

#[tokio::test]
async fn test_percentile_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = create_query_engine();

    macro_rules! test_percentile {
        ([], $( { $T:ty } ),*) => {
            $(
                let column_name = format!("{}_number", std::any::type_name::<$T>());
                test_percentile_success::<$T>(&column_name, "numbers", engine.clone()).await?;
            )*
        }
    }
    for_all_primitive_types! { test_percentile }
    Ok(())
}

#[tokio::test]
async fn test_percentile_correctness() -> Result<()> {
    let engine = create_correctness_engine();
    let sql = String::from("select PERCENTILE(corr_number,88.0) as percentile from corr_numbers");
    let record_batch = exec_selection(engine, &sql).await;
    let column = record_batch[0].column(0);
    let value = column.get(0);
    assert_eq!(value, Value::from(9.280_000_000_000_001_f64));
    Ok(())
}

async fn test_percentile_success<T>(
    column_name: &str,
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Result<()>
where
    T: WrapperType + AsPrimitive<f64>,
{
    let sql = format!("select PERCENTILE({column_name},50.0) as percentile from {table_name}");
    let result = exec_selection(engine.clone(), &sql).await;
    let value = function::get_value_from_batches("percentile", result);

    let numbers = get_numbers_from_table::<T>(column_name, table_name, engine.clone()).await;
    let expected_value = numbers.iter().map(|&n| n.as_()).collect::<Vec<f64>>();

    let expected_value: inc_stats::Percentiles<f64> = expected_value.iter().cloned().collect();
    let expected_value = expected_value.percentile(0.5).unwrap();
    assert_eq!(value, expected_value.into());
    Ok(())
}

fn create_correctness_engine() -> Arc<dyn QueryEngine> {
    // create engine

    let mut column_schemas = vec![];
    let mut columns = vec![];

    let column_schema = ColumnSchema::new("corr_number", ConcreteDataType::int32_datatype(), true);
    column_schemas.push(column_schema);

    let numbers = [3_i32, 6_i32, 8_i32, 10_i32];

    let column: VectorRef = Arc::new(Int32Vector::from_slice(numbers));
    columns.push(column);

    let schema = Arc::new(Schema::new(column_schemas));
    let number_table = MemTable::table("corr_numbers", RecordBatch::new(schema, columns).unwrap());
    new_query_engine_with_table(number_table)
}
