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
use common_query::error::{self, Result};
use common_query::prelude::{Signature, TypeSignature};
use datafusion::logical_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{Float64VectorBuilder, MutableVector, VectorRef};
use derive_more::Display;
use geo::algorithm::line_measures::metric_spaces::Euclidean;
use geo::{Area, Distance, Haversine};
use geo_types::Geometry;
use snafu::ResultExt;

use super::helpers::{ensure_columns_len, ensure_columns_n};
use super::wkt::parse_wkt;
use crate::function::{Function, FunctionContext};

/// Return WGS84(SRID: 4326) euclidean distance between two geometry object, in degree
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct STDistance;

impl Function for STDistance {
    fn name(&self) -> &str {
        "st_distance"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::string_datatype(),
            ]),
            Volatility::Stable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let wkt_this_vec = &columns[0];
        let wkt_that_vec = &columns[1];

        let size = wkt_this_vec.len();
        let mut results = Float64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let wkt_this = wkt_this_vec.get(i).as_string();
            let wkt_that = wkt_that_vec.get(i).as_string();

            let result = match (wkt_this, wkt_that) {
                (Some(wkt_this), Some(wkt_that)) => {
                    let geom_this = parse_wkt(&wkt_this)?;
                    let geom_that = parse_wkt(&wkt_that)?;

                    Some(Euclidean::distance(&geom_this, &geom_that))
                }
                _ => None,
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Return great circle distance between two geometry object, in meters
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct STDistanceSphere;

impl Function for STDistanceSphere {
    fn name(&self) -> &str {
        "st_distance_sphere_m"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::string_datatype(),
            ]),
            Volatility::Stable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let wkt_this_vec = &columns[0];
        let wkt_that_vec = &columns[1];

        let size = wkt_this_vec.len();
        let mut results = Float64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let wkt_this = wkt_this_vec.get(i).as_string();
            let wkt_that = wkt_that_vec.get(i).as_string();

            let result = match (wkt_this, wkt_that) {
                (Some(wkt_this), Some(wkt_that)) => {
                    let geom_this = parse_wkt(&wkt_this)?;
                    let geom_that = parse_wkt(&wkt_that)?;

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

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Return area of given geometry object
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct STArea;

impl Function for STArea {
    fn name(&self) -> &str {
        "st_area"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![ConcreteDataType::string_datatype()]),
            Volatility::Stable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 1);

        let wkt_vec = &columns[0];

        let size = wkt_vec.len();
        let mut results = Float64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let wkt = wkt_vec.get(i).as_string();

            let result = if let Some(wkt) = wkt {
                let geom = parse_wkt(&wkt)?;
                Some(geom.unsigned_area())
            } else {
                None
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}
