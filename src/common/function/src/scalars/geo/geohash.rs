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

use std::fmt;

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error::{self, InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, TypeSignature};
use datafusion::logical_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::value::Value;
use datatypes::vectors::{MutableVector, StringVectorBuilder, VectorRef};
use geohash::Coord;
use snafu::{ensure, ResultExt};

use crate::function::{Function, FunctionContext};

/// Function that return geohash string for a given geospatial coordinate.
#[derive(Clone, Debug, Default)]
pub struct GeohashFunction;

const NAME: &str = "geohash";

impl Function for GeohashFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        let mut signatures = Vec::new();
        for coord_type in &[
            ConcreteDataType::float32_datatype(),
            ConcreteDataType::float64_datatype(),
        ] {
            for resolution_type in &[
                ConcreteDataType::int8_datatype(),
                ConcreteDataType::int16_datatype(),
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::uint8_datatype(),
                ConcreteDataType::uint16_datatype(),
                ConcreteDataType::uint32_datatype(),
                ConcreteDataType::uint64_datatype(),
            ] {
                signatures.push(TypeSignature::Exact(vec![
                    // latitude
                    coord_type.clone(),
                    // longitude
                    coord_type.clone(),
                    // resolution
                    resolution_type.clone(),
                ]));
            }
        }
        Signature::one_of(signatures, Volatility::Stable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 3,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect 3, provided : {}",
                    columns.len()
                ),
            }
        );

        let lat_vec = &columns[0];
        let lon_vec = &columns[1];
        let resolution_vec = &columns[2];

        let size = lat_vec.len();
        let mut results = StringVectorBuilder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.get(i).as_f64_lossy();
            let lon = lon_vec.get(i).as_f64_lossy();
            let r = match resolution_vec.get(i) {
                Value::Int8(v) => v as usize,
                Value::Int16(v) => v as usize,
                Value::Int32(v) => v as usize,
                Value::Int64(v) => v as usize,
                Value::UInt8(v) => v as usize,
                Value::UInt16(v) => v as usize,
                Value::UInt32(v) => v as usize,
                Value::UInt64(v) => v as usize,
                _ => unreachable!(),
            };

            let result = match (lat, lon) {
                (Some(lat), Some(lon)) => {
                    let coord = Coord { x: lon, y: lat };
                    let encoded = geohash::encode(coord, r)
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("Geohash error: {}", e),
                                StatusCode::EngineExecuteQuery,
                            ))
                        })
                        .context(error::ExecuteSnafu)?;
                    Some(encoded)
                }
                _ => None,
            };

            results.push(result.as_deref());
        }

        Ok(results.to_vector())
    }
}

impl fmt::Display for GeohashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME)
    }
}
