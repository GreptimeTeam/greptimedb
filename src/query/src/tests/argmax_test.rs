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

use datatypes::for_all_primitive_types;
use datatypes::prelude::*;
use datatypes::types::WrapperType;

use crate::error::Result;
use crate::tests::{exec_selection, function};
use crate::QueryEngine;

#[tokio::test]
async fn test_argmax_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = function::create_query_engine();

    macro_rules! test_argmax {
        ([], $( { $T:ty } ),*) => {
            $(
                let column_name = format!("{}_number", std::any::type_name::<$T>());
                test_argmax_success::<$T>(&column_name, "numbers", engine.clone()).await?;
            )*
        }
    }
    for_all_primitive_types! { test_argmax }
    Ok(())
}

async fn test_argmax_success<T>(
    column_name: &str,
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Result<()>
where
    T: WrapperType + PartialOrd,
{
    let sql = format!("select ARGMAX({column_name}) as argmax from {table_name}");
    let result = exec_selection(engine.clone(), &sql).await;
    let value = function::get_value_from_batches("argmax", result);

    let numbers =
        function::get_numbers_from_table::<T>(column_name, table_name, engine.clone()).await;
    let expected_value = match numbers.len() {
        0 => 0_u64,
        _ => {
            let mut index = 0;
            let mut max = numbers[0];
            for (i, &number) in numbers.iter().enumerate() {
                if max < number {
                    max = number;
                    index = i;
                }
            }
            index as u64
        }
    };
    let expected_value = Value::from(expected_value);
    assert_eq!(value, expected_value);
    Ok(())
}
