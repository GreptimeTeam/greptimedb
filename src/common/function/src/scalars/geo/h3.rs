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

use std::str::FromStr;

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error::{self, Result};
use common_query::prelude::{Signature, TypeSignature};
use datafusion::logical_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::{Scalar, ScalarVectorBuilder};
use datatypes::value::{ListValue, Value};
use datatypes::vectors::{
    BooleanVectorBuilder, Float64VectorBuilder, Int32VectorBuilder, ListVectorBuilder,
    MutableVector, StringVectorBuilder, UInt64VectorBuilder, UInt8VectorBuilder, VectorRef,
};
use derive_more::Display;
use h3o::{CellIndex, LatLng, Resolution};
use once_cell::sync::Lazy;
use snafu::ResultExt;

use crate::function::{Function, FunctionContext};
use crate::scalars::geo::helpers::{ensure_and_coerce, ensure_columns_len, ensure_columns_n};

static CELL_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
    vec![
        ConcreteDataType::int64_datatype(),
        ConcreteDataType::uint64_datatype(),
        ConcreteDataType::string_datatype(),
    ]
});

static COORDINATE_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
    vec![
        ConcreteDataType::float32_datatype(),
        ConcreteDataType::float64_datatype(),
    ]
});
static RESOLUTION_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
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
static DISTANCE_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
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

static POSITION_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
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

/// Function that returns [h3] encoding cellid for a given geospatial coordinate.
///
/// [h3]: https://h3geo.org/
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3LatLngToCell;

impl Function for H3LatLngToCell {
    fn name(&self) -> &str {
        "h3_latlng_to_cell"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        let mut signatures = Vec::new();
        for coord_type in COORDINATE_TYPES.as_slice() {
            for resolution_type in RESOLUTION_TYPES.as_slice() {
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

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 3);

        let lat_vec = &columns[0];
        let lon_vec = &columns[1];
        let resolution_vec = &columns[2];

        let size = lat_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.get(i).as_f64_lossy();
            let lon = lon_vec.get(i).as_f64_lossy();
            let r = value_to_resolution(resolution_vec.get(i))?;

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
                    let encoded: u64 = coord.to_cell(r).into();
                    Some(encoded)
                }
                _ => None,
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns [h3] encoding cellid in string form for a given
/// geospatial coordinate.
///
/// [h3]: https://h3geo.org/
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3LatLngToCellString;

impl Function for H3LatLngToCellString {
    fn name(&self) -> &str {
        "h3_latlng_to_cell_string"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        let mut signatures = Vec::new();
        for coord_type in COORDINATE_TYPES.as_slice() {
            for resolution_type in RESOLUTION_TYPES.as_slice() {
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

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 3);

        let lat_vec = &columns[0];
        let lon_vec = &columns[1];
        let resolution_vec = &columns[2];

        let size = lat_vec.len();
        let mut results = StringVectorBuilder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.get(i).as_f64_lossy();
            let lon = lon_vec.get(i).as_f64_lossy();
            let r = value_to_resolution(resolution_vec.get(i))?;

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

/// Function that converts cell id to its string form
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellToString;

impl Function for H3CellToString {
    fn name(&self) -> &str {
        "h3_cell_to_string"
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
            let cell_id_string = cell_from_value(cell_vec.get(i))?.map(|c| c.to_string());

            results.push(cell_id_string.as_deref());
        }

        Ok(results.to_vector())
    }
}

/// Function that converts cell string id to uint64 number
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3StringToCell;

impl Function for H3StringToCell {
    fn name(&self) -> &str {
        "h3_string_to_cell"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![ConcreteDataType::string_datatype()]),
            Volatility::Stable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 1);

        let string_vec = &columns[0];
        let size = string_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = string_vec.get(i);

            let cell_id = match cell {
                Value::String(v) => Some(
                    CellIndex::from_str(v.as_utf8())
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("H3 error: {}", e),
                                StatusCode::EngineExecuteQuery,
                            ))
                        })
                        .context(error::ExecuteSnafu)?
                        .into(),
                ),
                _ => None,
            };

            results.push(cell_id);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns centroid latitude and longitude of given cell id
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellCenterLatLng;

impl Function for H3CellCenterLatLng {
    fn name(&self) -> &str {
        "h3_cell_center_latlng"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::float64_datatype(),
        ))
    }

    fn signature(&self) -> Signature {
        signature_of_cell()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 1);

        let cell_vec = &columns[0];
        let size = cell_vec.len();
        let mut results =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::float64_datatype(), size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let latlng = cell.map(LatLng::from);

            if let Some(latlng) = latlng {
                let result = ListValue::new(
                    vec![latlng.lat().into(), latlng.lng().into()],
                    ConcreteDataType::float64_datatype(),
                );
                results.push(Some(result.as_scalar_ref()));
            } else {
                results.push(None);
            }
        }

        Ok(results.to_vector())
    }
}

/// Function that returns resolution of given cell id
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellResolution;

impl Function for H3CellResolution {
    fn name(&self) -> &str {
        "h3_cell_resolution"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint8_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 1);

        let cell_vec = &columns[0];
        let size = cell_vec.len();
        let mut results = UInt8VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let res = cell.map(|cell| cell.resolution().into());

            results.push(res);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns base cell of given cell id
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellBase;

impl Function for H3CellBase {
    fn name(&self) -> &str {
        "h3_cell_base"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint8_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 1);

        let cell_vec = &columns[0];
        let size = cell_vec.len();
        let mut results = UInt8VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let res = cell.map(|cell| cell.base_cell().into());

            results.push(res);
        }

        Ok(results.to_vector())
    }
}

/// Function that check if given cell id is a pentagon
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellIsPentagon;

impl Function for H3CellIsPentagon {
    fn name(&self) -> &str {
        "h3_cell_is_pentagon"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 1);

        let cell_vec = &columns[0];
        let size = cell_vec.len();
        let mut results = BooleanVectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let res = cell.map(|cell| cell.is_pentagon());

            results.push(res);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns center child cell of given cell id
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellCenterChild;

impl Function for H3CellCenterChild {
    fn name(&self) -> &str {
        "h3_cell_center_child"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell_and_resolution()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_vec = &columns[0];
        let res_vec = &columns[1];
        let size = cell_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let res = value_to_resolution(res_vec.get(i))?;
            let result = cell
                .and_then(|cell| cell.center_child(res))
                .map(|c| c.into());

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns parent cell of given cell id and resolution
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellParent;

impl Function for H3CellParent {
    fn name(&self) -> &str {
        "h3_cell_parent"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell_and_resolution()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_vec = &columns[0];
        let res_vec = &columns[1];
        let size = cell_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let res = value_to_resolution(res_vec.get(i))?;
            let result = cell.and_then(|cell| cell.parent(res)).map(|c| c.into());

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns children cell list
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellToChildren;

impl Function for H3CellToChildren {
    fn name(&self) -> &str {
        "h3_cell_to_children"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::uint64_datatype(),
        ))
    }

    fn signature(&self) -> Signature {
        signature_of_cell_and_resolution()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_vec = &columns[0];
        let res_vec = &columns[1];
        let size = cell_vec.len();
        let mut results =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::uint64_datatype(), size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let res = value_to_resolution(res_vec.get(i))?;
            let result = cell.map(|cell| {
                let children: Vec<Value> = cell
                    .children(res)
                    .map(|child| Value::from(u64::from(child)))
                    .collect();
                ListValue::new(children, ConcreteDataType::uint64_datatype())
            });

            if let Some(list_value) = result {
                results.push(Some(list_value.as_scalar_ref()));
            } else {
                results.push(None);
            }
        }

        Ok(results.to_vector())
    }
}

/// Function that returns children cell count
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellToChildrenSize;

impl Function for H3CellToChildrenSize {
    fn name(&self) -> &str {
        "h3_cell_to_children_size"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell_and_resolution()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_vec = &columns[0];
        let res_vec = &columns[1];
        let size = cell_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let res = value_to_resolution(res_vec.get(i))?;
            let result = cell.map(|cell| cell.children_count(res));
            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns the cell position if its parent at given resolution
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellToChildPos;

impl Function for H3CellToChildPos {
    fn name(&self) -> &str {
        "h3_cell_to_child_pos"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_cell_and_resolution()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_vec = &columns[0];
        let res_vec = &columns[1];
        let size = cell_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let res = value_to_resolution(res_vec.get(i))?;
            let result = cell.and_then(|cell| cell.child_position(res));
            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns the cell at given position of the parent at given resolution
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3ChildPosToCell;

impl Function for H3ChildPosToCell {
    fn name(&self) -> &str {
        "h3_child_pos_to_cell"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        let mut signatures =
            Vec::with_capacity(POSITION_TYPES.len() * CELL_TYPES.len() * RESOLUTION_TYPES.len());
        for position_type in POSITION_TYPES.as_slice() {
            for cell_type in CELL_TYPES.as_slice() {
                for resolution_type in RESOLUTION_TYPES.as_slice() {
                    signatures.push(TypeSignature::Exact(vec![
                        position_type.clone(),
                        cell_type.clone(),
                        resolution_type.clone(),
                    ]));
                }
            }
        }
        Signature::one_of(signatures, Volatility::Stable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 3);

        let pos_vec = &columns[0];
        let cell_vec = &columns[1];
        let res_vec = &columns[2];
        let size = cell_vec.len();
        let mut results = UInt64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let pos = value_to_position(pos_vec.get(i))?;
            let res = value_to_resolution(res_vec.get(i))?;
            let result = cell.and_then(|cell| cell.child_at(pos, res).map(u64::from));
            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns cells with k distances of given cell
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3GridDisk;

impl Function for H3GridDisk {
    fn name(&self) -> &str {
        "h3_grid_disk"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::uint64_datatype(),
        ))
    }

    fn signature(&self) -> Signature {
        signature_of_cell_and_distance()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_vec = &columns[0];
        let k_vec = &columns[1];
        let size = cell_vec.len();
        let mut results =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::uint64_datatype(), size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let k = value_to_distance(k_vec.get(i))?;

            let result = cell.map(|cell| {
                let children: Vec<Value> = cell
                    .grid_disk::<Vec<_>>(k)
                    .into_iter()
                    .map(|child| Value::from(u64::from(child)))
                    .collect();
                ListValue::new(children, ConcreteDataType::uint64_datatype())
            });

            if let Some(list_value) = result {
                results.push(Some(list_value.as_scalar_ref()));
            } else {
                results.push(None);
            }
        }

        Ok(results.to_vector())
    }
}

/// Function that returns all cells within k distances of given cell
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3GridDiskDistances;

impl Function for H3GridDiskDistances {
    fn name(&self) -> &str {
        "h3_grid_disk_distances"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::uint64_datatype(),
        ))
    }

    fn signature(&self) -> Signature {
        signature_of_cell_and_distance()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_vec = &columns[0];
        let k_vec = &columns[1];
        let size = cell_vec.len();
        let mut results =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::uint64_datatype(), size);

        for i in 0..size {
            let cell = cell_from_value(cell_vec.get(i))?;
            let k = value_to_distance(k_vec.get(i))?;

            let result = cell.map(|cell| {
                let children: Vec<Value> = cell
                    .grid_disk_distances::<Vec<_>>(k)
                    .into_iter()
                    .map(|(child, _distance)| Value::from(u64::from(child)))
                    .collect();
                ListValue::new(children, ConcreteDataType::uint64_datatype())
            });

            if let Some(list_value) = result {
                results.push(Some(list_value.as_scalar_ref()));
            } else {
                results.push(None);
            }
        }

        Ok(results.to_vector())
    }
}

/// Function that returns distance between two cells
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3GridDistance;

impl Function for H3GridDistance {
    fn name(&self) -> &str {
        "h3_grid_distance"
    }
    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::int32_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_double_cells()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_this_vec = &columns[0];
        let cell_that_vec = &columns[1];
        let size = cell_this_vec.len();

        let mut results = Int32VectorBuilder::with_capacity(size);

        for i in 0..size {
            let result = match (
                cell_from_value(cell_this_vec.get(i))?,
                cell_from_value(cell_that_vec.get(i))?,
            ) {
                (Some(cell_this), Some(cell_that)) => {
                    let dist = cell_this
                        .grid_distance(cell_that)
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("H3 error: {}", e),
                                StatusCode::EngineExecuteQuery,
                            ))
                        })
                        .context(error::ExecuteSnafu)?;
                    Some(dist)
                }
                _ => None,
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Function that returns path cells between two cells
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3GridPathCells;

impl Function for H3GridPathCells {
    fn name(&self) -> &str {
        "h3_grid_path_cells"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::uint64_datatype(),
        ))
    }

    fn signature(&self) -> Signature {
        signature_of_double_cells()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_this_vec = &columns[0];
        let cell_that_vec = &columns[1];
        let size = cell_this_vec.len();
        let mut results =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::uint64_datatype(), size);

        for i in 0..size {
            let result = match (
                cell_from_value(cell_this_vec.get(i))?,
                cell_from_value(cell_that_vec.get(i))?,
            ) {
                (Some(cell_this), Some(cell_that)) => {
                    let cells = cell_this
                        .grid_path_cells(cell_that)
                        .and_then(|x| x.collect::<std::result::Result<Vec<CellIndex>, _>>())
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("H3 error: {}", e),
                                StatusCode::EngineExecuteQuery,
                            ))
                        })
                        .context(error::ExecuteSnafu)?;
                    Some(ListValue::new(
                        cells
                            .into_iter()
                            .map(|c| Value::from(u64::from(c)))
                            .collect(),
                        ConcreteDataType::uint64_datatype(),
                    ))
                }
                _ => None,
            };

            if let Some(list_value) = result {
                results.push(Some(list_value.as_scalar_ref()));
            } else {
                results.push(None);
            }
        }

        Ok(results.to_vector())
    }
}

/// Tests if cells contains given cells
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellContains;

impl Function for H3CellContains {
    fn name(&self) -> &str {
        "h3_cells_contains"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> Signature {
        let multi_cell_types = vec![
            ConcreteDataType::list_datatype(ConcreteDataType::int64_datatype()),
            ConcreteDataType::list_datatype(ConcreteDataType::uint64_datatype()),
            ConcreteDataType::list_datatype(ConcreteDataType::string_datatype()),
            ConcreteDataType::string_datatype(),
        ];

        let mut signatures = Vec::with_capacity(multi_cell_types.len() * CELL_TYPES.len());
        for multi_cell_type in &multi_cell_types {
            for cell_type in CELL_TYPES.as_slice() {
                signatures.push(TypeSignature::Exact(vec![
                    multi_cell_type.clone(),
                    cell_type.clone(),
                ]));
            }
        }

        Signature::one_of(signatures, Volatility::Stable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cells_vec = &columns[0];
        let cell_this_vec = &columns[1];

        let size = cell_this_vec.len();
        let mut results = BooleanVectorBuilder::with_capacity(size);

        for i in 0..size {
            let mut result = None;
            if let (cells, Some(cell_this)) = (
                cells_from_value(cells_vec.get(i))?,
                cell_from_value(cell_this_vec.get(i))?,
            ) {
                result = Some(false);

                for cell_that in cells.iter() {
                    // get cell resolution, and find cell_this's parent at
                    //  this solution, test if cell_that equals the parent
                    let resolution = cell_that.resolution();
                    if let Some(cell_this_parent) = cell_this.parent(resolution) {
                        if cell_this_parent == *cell_that {
                            result = Some(true);
                            break;
                        }
                    }
                }
            }

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Get WGS84 great circle distance of two cell centroid
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellDistanceSphereKm;

impl Function for H3CellDistanceSphereKm {
    fn name(&self) -> &str {
        "h3_distance_sphere_km"
    }
    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_double_cells()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_this_vec = &columns[0];
        let cell_that_vec = &columns[1];
        let size = cell_this_vec.len();

        let mut results = Float64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let result = match (
                cell_from_value(cell_this_vec.get(i))?,
                cell_from_value(cell_that_vec.get(i))?,
            ) {
                (Some(cell_this), Some(cell_that)) => {
                    let centroid_this = LatLng::from(cell_this);
                    let centroid_that = LatLng::from(cell_that);

                    Some(centroid_this.distance_km(centroid_that))
                }
                _ => None,
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

/// Get Euclidean distance of two cell centroid
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct H3CellDistanceEuclideanDegree;

impl H3CellDistanceEuclideanDegree {
    fn distance(centroid_this: LatLng, centroid_that: LatLng) -> f64 {
        ((centroid_this.lat() - centroid_that.lat()).powi(2)
            + (centroid_this.lng() - centroid_that.lng()).powi(2))
        .sqrt()
    }
}

impl Function for H3CellDistanceEuclideanDegree {
    fn name(&self) -> &str {
        "h3_distance_degree"
    }
    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        signature_of_double_cells()
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let cell_this_vec = &columns[0];
        let cell_that_vec = &columns[1];
        let size = cell_this_vec.len();

        let mut results = Float64VectorBuilder::with_capacity(size);

        for i in 0..size {
            let result = match (
                cell_from_value(cell_this_vec.get(i))?,
                cell_from_value(cell_that_vec.get(i))?,
            ) {
                (Some(cell_this), Some(cell_that)) => {
                    let centroid_this = LatLng::from(cell_this);
                    let centroid_that = LatLng::from(cell_that);

                    let dist = Self::distance(centroid_this, centroid_that);
                    Some(dist)
                }
                _ => None,
            };

            results.push(result);
        }

        Ok(results.to_vector())
    }
}

fn value_to_resolution(v: Value) -> Result<Resolution> {
    let r = match v {
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
    Resolution::try_from(r)
        .map_err(|e| {
            BoxedError::new(PlainError::new(
                format!("H3 error: {}", e),
                StatusCode::EngineExecuteQuery,
            ))
        })
        .context(error::ExecuteSnafu)
}

fn value_to_position(v: Value) -> Result<u64> {
    match v {
        Value::Int8(v) => ensure_and_coerce!(v >= 0, v as u64),
        Value::Int16(v) => ensure_and_coerce!(v >= 0, v as u64),
        Value::Int32(v) => ensure_and_coerce!(v >= 0, v as u64),
        Value::Int64(v) => ensure_and_coerce!(v >= 0, v as u64),
        Value::UInt8(v) => Ok(v as u64),
        Value::UInt16(v) => Ok(v as u64),
        Value::UInt32(v) => Ok(v as u64),
        Value::UInt64(v) => Ok(v),
        _ => unreachable!(),
    }
}

fn value_to_distance(v: Value) -> Result<u32> {
    match v {
        Value::Int8(v) => ensure_and_coerce!(v >= 0, v as u32),
        Value::Int16(v) => ensure_and_coerce!(v >= 0, v as u32),
        Value::Int32(v) => ensure_and_coerce!(v >= 0, v as u32),
        Value::Int64(v) => ensure_and_coerce!(v >= 0, v as u32),
        Value::UInt8(v) => Ok(v as u32),
        Value::UInt16(v) => Ok(v as u32),
        Value::UInt32(v) => Ok(v),
        Value::UInt64(v) => Ok(v as u32),
        _ => unreachable!(),
    }
}

fn signature_of_cell() -> Signature {
    let mut signatures = Vec::with_capacity(CELL_TYPES.len());
    for cell_type in CELL_TYPES.as_slice() {
        signatures.push(TypeSignature::Exact(vec![cell_type.clone()]));
    }

    Signature::one_of(signatures, Volatility::Stable)
}

fn signature_of_double_cells() -> Signature {
    let mut signatures = Vec::with_capacity(CELL_TYPES.len() * CELL_TYPES.len());
    for cell_type in CELL_TYPES.as_slice() {
        for cell_type2 in CELL_TYPES.as_slice() {
            signatures.push(TypeSignature::Exact(vec![
                cell_type.clone(),
                cell_type2.clone(),
            ]));
        }
    }

    Signature::one_of(signatures, Volatility::Stable)
}

fn signature_of_cell_and_resolution() -> Signature {
    let mut signatures = Vec::with_capacity(CELL_TYPES.len() * RESOLUTION_TYPES.len());
    for cell_type in CELL_TYPES.as_slice() {
        for resolution_type in RESOLUTION_TYPES.as_slice() {
            signatures.push(TypeSignature::Exact(vec![
                cell_type.clone(),
                resolution_type.clone(),
            ]));
        }
    }
    Signature::one_of(signatures, Volatility::Stable)
}

fn signature_of_cell_and_distance() -> Signature {
    let mut signatures = Vec::with_capacity(CELL_TYPES.len() * DISTANCE_TYPES.len());
    for cell_type in CELL_TYPES.as_slice() {
        for distance_type in DISTANCE_TYPES.as_slice() {
            signatures.push(TypeSignature::Exact(vec![
                cell_type.clone(),
                distance_type.clone(),
            ]));
        }
    }
    Signature::one_of(signatures, Volatility::Stable)
}

fn cell_from_value(v: Value) -> Result<Option<CellIndex>> {
    let cell = match v {
        Value::Int64(v) => Some(
            CellIndex::try_from(v as u64)
                .map_err(|e| {
                    BoxedError::new(PlainError::new(
                        format!("H3 error: {}", e),
                        StatusCode::EngineExecuteQuery,
                    ))
                })
                .context(error::ExecuteSnafu)?,
        ),
        Value::UInt64(v) => Some(
            CellIndex::try_from(v)
                .map_err(|e| {
                    BoxedError::new(PlainError::new(
                        format!("H3 error: {}", e),
                        StatusCode::EngineExecuteQuery,
                    ))
                })
                .context(error::ExecuteSnafu)?,
        ),
        Value::String(s) => Some(
            CellIndex::from_str(s.as_utf8())
                .map_err(|e| {
                    BoxedError::new(PlainError::new(
                        format!("H3 error: {}", e),
                        StatusCode::EngineExecuteQuery,
                    ))
                })
                .context(error::ExecuteSnafu)?,
        ),
        _ => None,
    };
    Ok(cell)
}

/// extract cell array from all possible types including:
/// - int64 list
/// - uint64 list
/// - string list
/// - comma-separated string
fn cells_from_value(v: Value) -> Result<Vec<CellIndex>> {
    match v {
        Value::List(list) => match list.datatype() {
            ConcreteDataType::Int64(_) => list
                .items()
                .iter()
                .map(|v| {
                    if let Value::Int64(v) = v {
                        CellIndex::try_from(*v as u64)
                            .map_err(|e| {
                                BoxedError::new(PlainError::new(
                                    format!("H3 error: {}", e),
                                    StatusCode::EngineExecuteQuery,
                                ))
                            })
                            .context(error::ExecuteSnafu)
                    } else {
                        Err(BoxedError::new(PlainError::new(
                            "Invalid data type in array".to_string(),
                            StatusCode::EngineExecuteQuery,
                        )))
                        .context(error::ExecuteSnafu)
                    }
                })
                .collect::<Result<Vec<CellIndex>>>(),
            ConcreteDataType::UInt64(_) => list
                .items()
                .iter()
                .map(|v| {
                    if let Value::UInt64(v) = v {
                        CellIndex::try_from(*v)
                            .map_err(|e| {
                                BoxedError::new(PlainError::new(
                                    format!("H3 error: {}", e),
                                    StatusCode::EngineExecuteQuery,
                                ))
                            })
                            .context(error::ExecuteSnafu)
                    } else {
                        Err(BoxedError::new(PlainError::new(
                            "Invalid data type in array".to_string(),
                            StatusCode::EngineExecuteQuery,
                        )))
                        .context(error::ExecuteSnafu)
                    }
                })
                .collect::<Result<Vec<CellIndex>>>(),
            ConcreteDataType::String(_) => list
                .items()
                .iter()
                .map(|v| {
                    if let Value::String(v) = v {
                        CellIndex::from_str(v.as_utf8().trim())
                            .map_err(|e| {
                                BoxedError::new(PlainError::new(
                                    format!("H3 error: {}", e),
                                    StatusCode::EngineExecuteQuery,
                                ))
                            })
                            .context(error::ExecuteSnafu)
                    } else {
                        Err(BoxedError::new(PlainError::new(
                            "Invalid data type in array".to_string(),
                            StatusCode::EngineExecuteQuery,
                        )))
                        .context(error::ExecuteSnafu)
                    }
                })
                .collect::<Result<Vec<CellIndex>>>(),
            _ => Ok(vec![]),
        },
        Value::String(csv) => {
            let str_seq = csv.as_utf8().split(',');
            str_seq
                .map(|v| {
                    CellIndex::from_str(v.trim())
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("H3 error: {}", e),
                                StatusCode::EngineExecuteQuery,
                            ))
                        })
                        .context(error::ExecuteSnafu)
                })
                .collect::<Result<Vec<CellIndex>>>()
        }
        _ => Ok(vec![]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_h3_euclidean_distance() {
        let point_this = LatLng::new(42.3521, -72.1235).expect("incorrect lat lng");
        let point_that = LatLng::new(42.45, -72.1260).expect("incorrect lat lng");

        let dist = H3CellDistanceEuclideanDegree::distance(point_this, point_that);
        assert_eq!(dist, 0.09793191512474639);
    }
}
