// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
mod function;

use common_query::Output;
use common_recordbatch::error::Result as RecordResult;
use common_recordbatch::{util, RecordBatch};
use datafusion::field_util::{FieldExt, SchemaExt};
use datatypes::for_all_primitive_types;
use datatypes::prelude::*;
use datatypes::types::PrimitiveElement;
use function::{create_query_engine, get_numbers_from_table};
use query::error::Result;
use query::QueryEngine;
use session::context::QueryContext;

#[tokio::test]
async fn test_argmin_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = create_query_engine();

    macro_rules! test_argmin {
        ([], $( { $T:ty } ),*) => {
            $(
                let column_name = format!("{}_number", std::any::type_name::<$T>());
                test_argmin_success::<$T>(&column_name, "numbers", engine.clone()).await?;
            )*
        }
    }
    for_all_primitive_types! { test_argmin }
    Ok(())
}

async fn test_argmin_success<T>(
    column_name: &str,
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Result<()>
where
    T: PrimitiveElement + PartialOrd,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    let result = execute_argmin(column_name, table_name, engine.clone())
        .await
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(result[0].df_recordbatch.num_columns(), 1);
    assert_eq!(1, result[0].schema.arrow_schema().fields().len());
    assert_eq!("argmin", result[0].schema.arrow_schema().field(0).name());

    let columns = result[0].df_recordbatch.columns();
    assert_eq!(1, columns.len());
    assert_eq!(columns[0].len(), 1);
    let v = VectorHelper::try_into_vector(&columns[0]).unwrap();
    assert_eq!(1, v.len());
    let value = v.get(0);

    let numbers = get_numbers_from_table::<T>(column_name, table_name, engine.clone()).await;
    let expected_value = match numbers.len() {
        0 => 0_u32,
        _ => {
            let mut index = 0;
            let mut min = numbers[0].into();
            for (i, &number) in numbers.iter().enumerate() {
                if min > number.into() {
                    min = number.into();
                    index = i;
                }
            }
            index as u32
        }
    };
    let expected_value = Value::from(expected_value);
    assert_eq!(value, expected_value);
    Ok(())
}

async fn execute_argmin<'a>(
    column_name: &'a str,
    table_name: &'a str,
    engine: Arc<dyn QueryEngine>,
) -> RecordResult<Vec<RecordBatch>> {
    let sql = format!(
        "select argmin({}) as argmin from {}",
        column_name, table_name
    );
    let plan = &engine
        .sql_to_plan(&sql, Arc::new(QueryContext::new()))
        .unwrap()[0];

    let output = engine.execute(plan).await.unwrap();
    let recordbatch_stream = match output {
        Output::Stream(batch) => batch,
        _ => unreachable!(),
    };
    util::collect(recordbatch_stream).await
}
