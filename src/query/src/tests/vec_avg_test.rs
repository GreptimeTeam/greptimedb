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

use std::ops::AddAssign;

use common_function::scalars::vector::impl_conv::{as_veclit, veclit_to_binlit};
use datafusion_common::ScalarValue;
use datatypes::prelude::Value;
use nalgebra::{Const, DVectorView, Dyn, OVector};

use crate::tests::{exec_selection, function};

#[tokio::test]
async fn test_vec_avg_aggregator() -> Result<(), common_query::error::Error> {
    common_telemetry::init_default_ut_logging();
    let engine = function::create_query_engine_for_vector10x3();
    let sql = "select VEC_AVG(vector) as vec_avg from vectors";
    let result = exec_selection(engine.clone(), sql).await;
    let value = function::get_value_from_batches("vec_avg", result);

    let mut expected_value = None;

    let sql = "SELECT vector FROM vectors";
    let vectors = exec_selection(engine, sql).await;

    let column = vectors[0].column(0);
    let len = column.len();
    for i in 0..column.len() {
        let v = ScalarValue::try_from_array(&column, i)?;
        let vector = as_veclit(&v)?;
        let Some(vector) = vector else {
            expected_value = None;
            break;
        };
        expected_value
            .get_or_insert_with(|| OVector::zeros_generic(Dyn(3), Const::<1>))
            .add_assign(&DVectorView::from_slice(&vector, vector.len()));
    }
    let expected_value = match expected_value.map(|mut v| {
        v /= len as f32;
        veclit_to_binlit(v.as_slice())
    }) {
        None => Value::Null,
        Some(bytes) => Value::from(bytes),
    };
    assert_eq!(value, expected_value);

    Ok(())
}
