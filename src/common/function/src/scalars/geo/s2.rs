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

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, TypeSignature};
use datafusion::logical_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::value::Value;
use datatypes::vectors::{MutableVector, StringVectorBuilder, UInt64VectorBuilder, VectorRef};
use derive_more::Display;
use once_cell::sync::Lazy;
use s2::cellid::{CellID, MAX_LEVEL};
use s2::latlng::LatLng;
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::scalars::geo::helpers::{ensure_and_coerce, ensure_columns_len, ensure_columns_n};

static CELL_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
    vec![
        ConcreteDataType::int64_datatype(),
        ConcreteDataType::uint64_datatype(),
    ]
});

static COORDINATE_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
    vec![
        ConcreteDataType::float32_datatype(),
        ConcreteDataType::float64_datatype(),
    ]
});

static LEVEL_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
    vec![
        ConcreteDataType::int8_datatype(),
        ConcreteDataType::int16_datatype(),
        ConcreteDataType::int32_datatype(),
        ConcreteDataType::int64_datatype(),
        ConcreteDataType::uint8_datatype(),
        ConcreteDataType::uint16_datatype(),
        ConcreteDataType::uint32_datatype(),
        ConcreteDataType::uint64_datatype(),
    ]
});

/// Function that returns [s2] encoding cellid for a given geospatial coordinate.
///
/// [s2]: http://s2geometry.io
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct S2LatLngToCell;

impl Function for S2LatLngToCell {
    fn name(&self) -> &str {
        "s2_latlng_to_cell"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        let mut signatures = Vec::with_capacity(COORDINATE_TYPES.len());
        for coord_type in COORDINATE_TYPES.as_slice() {
            signatures.push(TypeSignature::Exact(vec![
                // latitude
                coord_type.clone(),
                // longitude
                coord_type.clone(),
            ]));
        }
        Signature::one_of(signatures, Volatility::Stable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let lat_vec = &columns[0];
        let lon_vec = &columns[1];

        let size = lat_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.get(i).as_f64_lossy();
            let lon = lon_vec.get(i).as_f64_lossy();

            let result = match (lat, lon) {
                (Some(lat), Some(lon)) => {
                    let coord = LatLng::from_degrees(lat, lon);
                    ensure!(
                        coord.is_valid(),
                        InvalidFuncArgsSnafu {
                            err_msg: "The input coordinates are invalid",
                        }
                    );
                    let cellid = CellID::from(coord);
                    let encoded: u64 = cellid.0;
                    Some(encoded)
                }
                _ => None,
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Return the level of current s2 cell
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct S2CellLevel;

impl Function for S2CellLevel {
    fn name(&self) -> &str {
        "s2_cell_level"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 1);

        let cell_vec = &columns[0];
        let size = cell_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i));
            let res = cell.map(|cell| cell.level());

            results.push(res);
        }

        Ok(results.to_vector())
    }
}

/// Return the string presentation of the cell
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct S2CellToToken;

impl Function for S2CellToToken {
    fn name(&self) -> &str {
        "s2_cell_to_token"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 1);

        let cell_vec = &columns[0];
        let size = cell_vec.len();
        let mut results = StringVectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i));
            let res = cell.map(|cell| cell.to_token());

            results.push(res.as_deref());
        }

        Ok(results.to_vector())
    }
}

/// Return parent at given level of current s2 cell
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct S2CellParent;

impl Function for S2CellParent {
    fn name(&self) -> &str {
        "s2_cell_parent"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell_and_level()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_vec = &columns[0];
        let level_vec = &columns[1];
        let size = cell_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i));
            let level = value_to_level(level_vec.get(i))?;
            let result = cell.map(|cell| cell.parent(level).0);

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

fn signature_of_cell() -> Signature {
    let mut signatures = Vec::with_capacity(CELL_TYPES.len());
    for cell_type in CELL_TYPES.as_slice() {
        signatures.push(TypeSignature::Exact(vec![cell_type.clone()]));
    }

    Signature::one_of(signatures, Volatility::Stable)
}

fn signature_of_cell_and_level() -> Signature {
    let mut signatures = Vec::with_capacity(CELL_TYPES.len() * LEVEL_TYPES.len());
    for cell_type in CELL_TYPES.as_slice() {
        for level_type in LEVEL_TYPES.as_slice() {
            signatures.push(TypeSignature::Exact(vec![
                cell_type.clone(),
                level_type.clone(),
            ]));
        }
    }
    Signature::one_of(signatures, Volatility::Stable)
}

fn cell_from_value(v: Value) -> Option<CellID> {
    match v {
        Value::Int64(v) => Some(CellID(v as u64)),
        Value::UInt64(v) => Some(CellID(v)),
        _ => None,
    }
}

fn value_to_level(v: Value) -> Result<u64> {
    match v {
        Value::Int8(v) => ensure_and_coerce!(v >= 0 && v <= MAX_LEVEL as i8, v as u64),
        Value::Int16(v) => ensure_and_coerce!(v >= 0 && v <= MAX_LEVEL as i16, v as u64),
        Value::Int32(v) => ensure_and_coerce!(v >= 0 && v <= MAX_LEVEL as i32, v as u64),
        Value::Int64(v) => ensure_and_coerce!(v >= 0 && v <= MAX_LEVEL as i64, v as u64),
        Value::UInt8(v) => ensure_and_coerce!(v <= MAX_LEVEL as u8, v as u64),
        Value::UInt16(v) => ensure_and_coerce!(v <= MAX_LEVEL as u16, v as u64),
        Value::UInt32(v) => ensure_and_coerce!(v <= MAX_LEVEL as u32, v as u64),
        Value::UInt64(v) => ensure_and_coerce!(v <= MAX_LEVEL, v),
        _ => unreachable!(),
    }
}
