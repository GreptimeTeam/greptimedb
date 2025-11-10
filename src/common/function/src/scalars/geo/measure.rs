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

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error;
use datafusion_common::arrow::array::{Array, AsArray, Float64Builder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use derive_more::Display;
use geo::algorithm::line_measures::metric_spaces::Euclidean;
use geo::{Area, Distance, Haversine};
use geo_types::Geometry;
use snafu::ResultExt;

use crate::function::{Function, extract_args};
use crate::scalars::geo::wkt::parse_wkt;

/// Return WGS84(SRID: 4326) euclidean distance between two geometry object, in degree
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct STDistance {
    signature: Signature,
}

impl Default for STDistance {
    fn default() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Stable),
        }
    }
}

impl Function for STDistance {
    fn name(&self) -> &str {
        "st_distance"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let wkt_this_vec = arg0.as_string_view();
        let arg1 = compute::cast(&arg1, &DataType::Utf8View)?;
        let wkt_that_vec = arg1.as_string_view();

        let size = wkt_this_vec.len();
        let mut builder = Float64Builder::with_capacity(size);

        for i in 0..size {
            let wkt_this = wkt_this_vec.is_valid(i).then(|| wkt_this_vec.value(i));
            let wkt_that = wkt_that_vec.is_valid(i).then(|| wkt_that_vec.value(i));

            let result = match (wkt_this, wkt_that) {
                (Some(wkt_this), Some(wkt_that)) => {
                    let geom_this = parse_wkt(wkt_this)?;
                    let geom_that = parse_wkt(wkt_that)?;

                    Some(Euclidean::distance(&geom_this, &geom_that))
                }
                _ => None,
            };

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Return great circle distance between two geometry object, in meters
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct STDistanceSphere {
    signature: Signature,
}

impl Default for STDistanceSphere {
    fn default() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Stable),
        }
    }
}

impl Function for STDistanceSphere {
    fn name(&self) -> &str {
        "st_distance_sphere_m"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let wkt_this_vec = arg0.as_string_view();
        let arg1 = compute::cast(&arg1, &DataType::Utf8View)?;
        let wkt_that_vec = arg1.as_string_view();

        let size = wkt_this_vec.len();
        let mut builder = Float64Builder::with_capacity(size);

        for i in 0..size {
            let wkt_this = wkt_this_vec.is_valid(i).then(|| wkt_this_vec.value(i));
            let wkt_that = wkt_that_vec.is_valid(i).then(|| wkt_that_vec.value(i));

            let result = match (wkt_this, wkt_that) {
                (Some(wkt_this), Some(wkt_that)) => {
                    let geom_this = parse_wkt(wkt_this)?;
                    let geom_that = parse_wkt(wkt_that)?;

                    match (geom_this, geom_that) {
                        (Geometry::Point(this), Geometry::Point(that)) => {
                            Some(Haversine::distance(this, that))
                        }
                        _ => {
                            Err(BoxedError::new(PlainError::new(
                                "Great circle distance between non-point objects are not supported for now.".to_string(),
                                StatusCode::Unsupported,
                            ))).context(error::ExecuteSnafu)?
                        }
                    }
                }
                _ => None,
            };

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Return area of given geometry object
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct STArea {
    signature: Signature,
}

impl Default for STArea {
    fn default() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Stable),
        }
    }
}

impl Function for STArea {
    fn name(&self) -> &str {
        "st_area"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;

        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let wkt_vec = arg0.as_string_view();

        let size = wkt_vec.len();
        let mut builder = Float64Builder::with_capacity(size);

        for i in 0..size {
            let wkt = wkt_vec.is_valid(i).then(|| wkt_vec.value(i));

            let result = if let Some(wkt) = wkt {
                let geom = parse_wkt(wkt)?;
                Some(geom.unsigned_area())
            } else {
                None
            };

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
