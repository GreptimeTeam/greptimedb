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
use num_traits::AsPrimitive;

use crate::error::Result;
use crate::tests::{exec_selection, function};
use crate::QueryEngine;

#[tokio::test]
async fn test_polyval_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = function::create_query_engine();

    macro_rules! test_polyval {
        ([], $( { $T:ty } ),*) => {
            $(
                let column_name = format!("{}_number", std::any::type_name::<$T>());
                test_polyval_success::<$T, <<<$T as WrapperType>::LogicalType as LogicalPrimitiveType>::LargestType as LogicalPrimitiveType>::Wrapper>(&column_name, "numbers", engine.clone()).await?;
            )*
        }
    }
    for_all_primitive_types! { test_polyval }
    Ok(())
}

async fn test_polyval_success<T, PolyT>(
    column_name: &str,
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Result<()>
where
    T: WrapperType,
    PolyT: WrapperType,
    T::Native: AsPrimitive<PolyT::Native>,
    PolyT::Native: std::ops::Mul<Output = PolyT::Native> + std::iter::Sum,
    i64: AsPrimitive<PolyT::Native>,
{
    let sql = format!("select POLYVAL({column_name}, 0) as polyval from {table_name}");
    let result = exec_selection(engine.clone(), &sql).await;
    let value = function::get_value_from_batches("polyval", result);

    let numbers =
        function::get_numbers_from_table::<T>(column_name, table_name, engine.clone()).await;
    let expected_value = numbers.iter().copied();
    let x = 0i64;
    let len = expected_value.len();
    let expected_native: PolyT::Native = expected_value
        .enumerate()
        .map(|(i, v)| v.into_native().as_() * (x.pow((len - 1 - i) as u32)).as_())
        .sum();
    assert_eq!(value, PolyT::from_native(expected_native).into());
    Ok(())
}
