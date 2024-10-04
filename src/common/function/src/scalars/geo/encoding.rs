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

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error::{self, InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, TypeSignature};
use common_time::Timestamp;
use datafusion::logical_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{MutableVector, StringVectorBuilder, VectorRef};
use derive_more::Display;
use once_cell::sync::Lazy;
use snafu::{ensure, ResultExt};

use super::helpers::{ensure_columns_len, ensure_columns_n};
use crate::function::{Function, FunctionContext};

static COORDINATE_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
    vec![
        ConcreteDataType::float32_datatype(),
        ConcreteDataType::float64_datatype(),
    ]
});

fn build_sorted_path(
    columns: &[VectorRef],
) -> Result<Vec<(Option<f64>, Option<f64>, Option<Timestamp>)>> {
    // this macro ensures column vectos has same size as well
    ensure_columns_n!(columns, 3);

    let lat = &columns[0];
    let lng = &columns[1];
    let ts = &columns[2];

    let size = lat.len();

    let mut work_vec = Vec::with_capacity(size);
    for idx in 0..size {
        work_vec.push((
            lat.get(idx).as_f64_lossy(),
            lng.get(idx).as_f64_lossy(),
            ts.get(idx).as_timestamp(),
        ));
    }

    // sort by timestamp, we treat null timestamp as 0
    work_vec.sort_unstable_by_key(|tuple| tuple.2.unwrap_or(Timestamp::new_second(0)));
    Ok(work_vec)
}

/// This function accept rows of lat, lng and timestamp, sort with timestamp and
/// encoding them into a geojson-like path.
///
/// Example:
///
/// ```sql
/// SELECT geojson_encode(lat, lon, timestamp) FROM table;
/// ```
///
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct GeojsonPathEncode;

impl Function for GeojsonPathEncode {
    fn name(&self) -> &str {
        "geojson_encode"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        let mut signatures = Vec::new();
        let coord_types = COORDINATE_TYPES.as_slice();

        let ts_types = ConcreteDataType::timestamps();
        for lat_type in coord_types {
            for lng_type in coord_types {
                for ts_type in &ts_types {
                    signatures.push(TypeSignature::Exact(vec![
                        lat_type.clone(),
                        lng_type.clone(),
                        ts_type.clone(),
                    ]));
                }
            }
        }

        Signature::one_of(signatures, Volatility::Stable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        let work_vec = build_sorted_path(columns)?;

        let mut results = StringVectorBuilder::with_capacity(1);

        let result = serde_json::to_string(
            &work_vec
                .into_iter()
                // note that we transform to lng,lat for geojson compatibility
                .map(|(lat, lng, _)| vec![lng, lat])
                .collect::<Vec<Vec<Option<f64>>>>(),
        )
        .map_err(|e| {
            BoxedError::new(PlainError::new(
                format!("Serialization failure: {}", e),
                StatusCode::EngineExecuteQuery,
            ))
        })
        .context(error::ExecuteSnafu)?;

        results.push(Some(&result));

        Ok(results.to_vector())
    }
}
