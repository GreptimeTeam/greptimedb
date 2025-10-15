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
use std::sync::Arc;

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error;
use datafusion::arrow::array::{Array, AsArray, ListBuilder, StringViewBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, UInt8Type};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;
use datafusion_expr::type_coercion::aggregates::INTEGERS;
use datafusion_expr::{ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use geohash::Coord;
use snafu::ResultExt;

use crate::function::{Function, extract_args};
use crate::scalars::geo::helpers;

fn ensure_resolution_usize(v: u8) -> datafusion_common::Result<usize> {
    if v == 0 || v > 12 {
        return Err(DataFusionError::Execution(format!(
            "Invalid geohash resolution {v}, valid value range: [1, 12]"
        )));
    }
    Ok(v as usize)
}

/// Function that return geohash string for a given geospatial coordinate.
#[derive(Clone, Debug)]
pub(crate) struct GeohashFunction {
    signature: Signature,
}

impl Default for GeohashFunction {
    fn default() -> Self {
        let mut signatures = Vec::new();
        for coord_type in &[DataType::Float32, DataType::Float64] {
            for resolution_type in INTEGERS {
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
        Self {
            signature: Signature::one_of(signatures, Volatility::Stable),
        }
    }
}

impl GeohashFunction {
    const NAME: &'static str = "geohash";
}

impl Function for GeohashFunction {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [lat_vec, lon_vec, resolutions] = extract_args(self.name(), &args)?;

        let lat_vec = helpers::cast::<Float64Type>(&lat_vec)?;
        let lat_vec = lat_vec.as_primitive::<Float64Type>();
        let lon_vec = helpers::cast::<Float64Type>(&lon_vec)?;
        let lon_vec = lon_vec.as_primitive::<Float64Type>();
        let resolutions = helpers::cast::<UInt8Type>(&resolutions)?;
        let resolutions = resolutions.as_primitive::<UInt8Type>();

        let size = lat_vec.len();
        let mut builder = StringViewBuilder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.is_valid(i).then(|| lat_vec.value(i));
            let lon = lon_vec.is_valid(i).then(|| lon_vec.value(i));
            let r = resolutions
                .is_valid(i)
                .then(|| ensure_resolution_usize(resolutions.value(i)))
                .transpose()?;

            let result = match (lat, lon, r) {
                (Some(lat), Some(lon), Some(r)) => {
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

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl fmt::Display for GeohashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Self::NAME)
    }
}

/// Function that return geohash string for a given geospatial coordinate.
#[derive(Clone, Debug)]
pub(crate) struct GeohashNeighboursFunction {
    signature: Signature,
}

impl Default for GeohashNeighboursFunction {
    fn default() -> Self {
        let mut signatures = Vec::new();
        for coord_type in &[DataType::Float32, DataType::Float64] {
            for resolution_type in INTEGERS {
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
        Self {
            signature: Signature::one_of(signatures, Volatility::Stable),
        }
    }
}

impl GeohashNeighboursFunction {
    const NAME: &'static str = "geohash_neighbours";
}

impl Function for GeohashNeighboursFunction {
    fn name(&self) -> &str {
        GeohashNeighboursFunction::NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8View,
            true,
        ))))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [lat_vec, lon_vec, resolutions] = extract_args(self.name(), &args)?;

        let lat_vec = helpers::cast::<Float64Type>(&lat_vec)?;
        let lat_vec = lat_vec.as_primitive::<Float64Type>();
        let lon_vec = helpers::cast::<Float64Type>(&lon_vec)?;
        let lon_vec = lon_vec.as_primitive::<Float64Type>();
        let resolutions = helpers::cast::<UInt8Type>(&resolutions)?;
        let resolutions = resolutions.as_primitive::<UInt8Type>();

        let size = lat_vec.len();
        let mut builder = ListBuilder::new(StringViewBuilder::new());

        for i in 0..size {
            let lat = lat_vec.is_valid(i).then(|| lat_vec.value(i));
            let lon = lon_vec.is_valid(i).then(|| lon_vec.value(i));
            let r = resolutions
                .is_valid(i)
                .then(|| ensure_resolution_usize(resolutions.value(i)))
                .transpose()?;

            match (lat, lon, r) {
                (Some(lat), Some(lon), Some(r)) => {
                    let coord = Coord { x: lon, y: lat };
                    let encoded = geohash::encode(coord, r)
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("Geohash error: {}", e),
                                StatusCode::EngineExecuteQuery,
                            ))
                        })
                        .context(error::ExecuteSnafu)?;
                    let neighbours = geohash::neighbors(&encoded)
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("Geohash error: {}", e),
                                StatusCode::EngineExecuteQuery,
                            ))
                        })
                        .context(error::ExecuteSnafu)?;
                    builder.append_value(
                        [
                            neighbours.n,
                            neighbours.nw,
                            neighbours.w,
                            neighbours.sw,
                            neighbours.s,
                            neighbours.se,
                            neighbours.e,
                            neighbours.ne,
                        ]
                        .into_iter()
                        .map(Some),
                    );
                }
                _ => builder.append_null(),
            };
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl fmt::Display for GeohashNeighboursFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", GeohashNeighboursFunction::NAME)
    }
}
