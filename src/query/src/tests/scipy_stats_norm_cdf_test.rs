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

use common_query::Output;
use common_recordbatch::error::Result as RecordResult;
use common_recordbatch::{util, RecordBatch};
use datatypes::for_all_primitive_types;
use datatypes::types::WrapperType;
use num_traits::AsPrimitive;
use session::context::QueryContext;
use statrs::distribution::{ContinuousCDF, Normal};
use statrs::statistics::Statistics;

use crate::error::Result;
use crate::parser::QueryLanguageParser;
use crate::tests::function;
use crate::QueryEngine;

#[tokio::test]
async fn test_scipy_stats_norm_cdf_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = function::create_query_engine();

    macro_rules! test_scipy_stats_norm_cdf {
        ([], $( { $T:ty } ),*) => {
            $(
                let column_name = format!("{}_number", std::any::type_name::<$T>());
                test_scipy_stats_norm_cdf_success::<$T>(&column_name, "numbers", engine.clone()).await?;
            )*
        }
    }
    for_all_primitive_types! { test_scipy_stats_norm_cdf }
    Ok(())
}

async fn test_scipy_stats_norm_cdf_success<T>(
    column_name: &str,
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Result<()>
where
    T: WrapperType + AsPrimitive<f64>,
{
    let result = execute_scipy_stats_norm_cdf(column_name, table_name, engine.clone())
        .await
        .unwrap();
    let value = function::get_value_from_batches("scipy_stats_norm_cdf", result);

    let numbers =
        function::get_numbers_from_table::<T>(column_name, table_name, engine.clone()).await;
    let expected_value = numbers.iter().map(|&n| n.as_()).collect::<Vec<f64>>();
    let mean = expected_value.clone().mean();
    let stddev = expected_value.std_dev();

    let n = Normal::new(mean, stddev).unwrap();
    let expected_value = n.cdf(2.0);

    assert_eq!(value, expected_value.into());
    Ok(())
}

async fn execute_scipy_stats_norm_cdf<'a>(
    column_name: &'a str,
    table_name: &'a str,
    engine: Arc<dyn QueryEngine>,
) -> RecordResult<Vec<RecordBatch>> {
    let sql = format!(
        "select SCIPYSTATSNORMCDF({column_name},2.0) as scipy_stats_norm_cdf from {table_name}",
    );
    let stmt = QueryLanguageParser::parse_sql(&sql).unwrap();
    let plan = engine
        .statement_to_plan(stmt, Arc::new(QueryContext::new()))
        .unwrap();

    let output = engine.execute(&plan).await.unwrap();
    let recordbatch_stream = match output {
        Output::Stream(batch) => batch,
        _ => unreachable!(),
    };
    util::collect(recordbatch_stream).await
}
