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

use common_query::error::Result;
use common_query::prelude::{Signature, TypeSignature};
use datafusion::logical_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{BooleanVectorBuilder, MutableVector, VectorRef};
use derive_more::Display;
use geo::algorithm::contains::Contains;
use geo::algorithm::intersects::Intersects;
use geo::algorithm::within::Within;

use super::helpers::{ensure_columns_len, ensure_columns_n};
use super::wkt::parse_wkt;
use crate::function::{Function, FunctionContext};

/// Test if spatial relationship: contains
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct STContains;

impl Function for STContains {
    fn name(&self) -> &str {
        "st_contains"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
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
        let mut results = BooleanVectorBuilder::with_capacity(size);

        for i in 0..size {
            let wkt_this = wkt_this_vec.get(i).as_string();
            let wkt_that = wkt_that_vec.get(i).as_string();

            let result = match (wkt_this, wkt_that) {
                (Some(wkt_this), Some(wkt_that)) => {
                    let geom_this = parse_wkt(&wkt_this)?;
                    let geom_that = parse_wkt(&wkt_that)?;

                    Some(geom_this.contains(&geom_that))
                }
                _ => None,
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Test if spatial relationship: within
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct STWithin;

impl Function for STWithin {
    fn name(&self) -> &str {
        "st_within"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
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
        let mut results = BooleanVectorBuilder::with_capacity(size);

        for i in 0..size {
            let wkt_this = wkt_this_vec.get(i).as_string();
            let wkt_that = wkt_that_vec.get(i).as_string();

            let result = match (wkt_this, wkt_that) {
                (Some(wkt_this), Some(wkt_that)) => {
                    let geom_this = parse_wkt(&wkt_this)?;
                    let geom_that = parse_wkt(&wkt_that)?;

                    Some(geom_this.is_within(&geom_that))
                }
                _ => None,
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Test if spatial relationship: within
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct STIntersects;

impl Function for STIntersects {
    fn name(&self) -> &str {
        "st_intersects"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
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
        let mut results = BooleanVectorBuilder::with_capacity(size);

        for i in 0..size {
            let wkt_this = wkt_this_vec.get(i).as_string();
            let wkt_that = wkt_that_vec.get(i).as_string();

            let result = match (wkt_this, wkt_that) {
                (Some(wkt_this), Some(wkt_that)) => {
                    let geom_this = parse_wkt(&wkt_this)?;
                    let geom_that = parse_wkt(&wkt_that)?;

                    Some(geom_this.intersects(&geom_that))
                }
                _ => None,
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}
