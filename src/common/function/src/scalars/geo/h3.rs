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
use h3o::{LatLng, Resolution};
use snafu::{ensure, ResultExt};

use crate::function::{Function, FunctionContext};

/// Function that returns [h3] encoding string for a given geospatial coordinate.
///
/// [h3]: https://h3geo.org/
#[derive(Clone, Debug, Default)]
pub struct H3Function;

const NAME: &str = "h3";

impl Function for H3Function {
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
                Value::Int8(v) => v as u8,
                Value::Int16(v) => v as u8,
                Value::Int32(v) => v as u8,
                Value::Int64(v) => v as u8,
                Value::UInt8(v) => v,
                Value::UInt16(v) => v as u8,
                Value::UInt32(v) => v as u8,
                Value::UInt64(v) => v as u8,
                _ => unreachable!(),
            };

            let result = match (lat, lon) {
                (Some(lat), Some(lon)) => {
                    let coord = LatLng::new(lat, lon)
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("H3 error: {}", e),
                                StatusCode::EngineExecuteQuery,
                            ))
                        })
                        .context(error::ExecuteSnafu)?;
                    let r = Resolution::try_from(r)
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("H3 error: {}", e),
                                StatusCode::EngineExecuteQuery,
                            ))
                        })
                        .context(error::ExecuteSnafu)?;
                    let encoded = coord.to_cell(r).to_string();
                    Some(encoded)
                }
                _ => None,
            };

            results.push(result.as_deref());
        }

        Ok(results.to_vector())
    }
}

impl fmt::Display for H3Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME)
    }
}
