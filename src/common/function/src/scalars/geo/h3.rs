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
use std::sync::{Arc, LazyLock};

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error;
use datafusion::arrow::array::{
    Array, AsArray, BooleanBuilder, Float64Builder, Int32Builder, ListBuilder, StringViewArray,
    StringViewBuilder, UInt8Builder, UInt64Builder,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{Float64Type, Int64Type, UInt8Type, UInt64Type};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::type_coercion::aggregates::INTEGERS;
use datafusion_expr::{ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use datatypes::arrow::datatypes::{DataType, Field};
use derive_more::Display;
use h3o::{CellIndex, LatLng, Resolution};
use snafu::prelude::*;

use crate::function::{Function, extract_args};
use crate::scalars::geo::helpers;

static CELL_TYPES: LazyLock<Vec<DataType>> =
    LazyLock::new(|| vec![DataType::Int64, DataType::UInt64, DataType::Utf8]);

static COORDINATE_TYPES: LazyLock<Vec<DataType>> =
    LazyLock::new(|| vec![DataType::Float32, DataType::Float64]);

static RESOLUTION_TYPES: &[DataType] = INTEGERS;

static DISTANCE_TYPES: &[DataType] = INTEGERS;

static POSITION_TYPES: &[DataType] = INTEGERS;

/// Function that returns [h3] encoding cellid for a given geospatial coordinate.
///
/// [h3]: https://h3geo.org/
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3LatLngToCell {
    signature: Signature,
}

impl Default for H3LatLngToCell {
    fn default() -> Self {
        let mut signatures = Vec::new();
        for coord_type in COORDINATE_TYPES.as_slice() {
            for resolution_type in RESOLUTION_TYPES {
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

impl Function for H3LatLngToCell {
    fn name(&self) -> &str {
        "h3_latlng_to_cell"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [lat_vec, lon_vec, resolution_vec] = extract_args(self.name(), &args)?;

        let lat_vec = helpers::cast::<Float64Type>(&lat_vec)?;
        let lat_vec = lat_vec.as_primitive::<Float64Type>();
        let lon_vec = helpers::cast::<Float64Type>(&lon_vec)?;
        let lon_vec = lon_vec.as_primitive::<Float64Type>();
        let resolutions = helpers::cast::<UInt8Type>(&resolution_vec)?;
        let resolution_vec = resolutions.as_primitive::<UInt8Type>();

        let size = lat_vec.len();
        let mut builder = UInt64Builder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.is_valid(i).then(|| lat_vec.value(i));
            let lon = lon_vec.is_valid(i).then(|| lon_vec.value(i));
            let r = resolution_vec
                .is_valid(i)
                .then(|| value_to_resolution(resolution_vec.value(i)))
                .transpose()?;

            let result = match (lat, lon, r) {
                (Some(lat), Some(lon), Some(r)) => {
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

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns [h3] encoding cellid in string form for a given
/// geospatial coordinate.
///
/// [h3]: https://h3geo.org/
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3LatLngToCellString {
    signature: Signature,
}

impl Default for H3LatLngToCellString {
    fn default() -> Self {
        let mut signatures = Vec::new();
        for coord_type in COORDINATE_TYPES.as_slice() {
            for resolution_type in RESOLUTION_TYPES {
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

impl Function for H3LatLngToCellString {
    fn name(&self) -> &str {
        "h3_latlng_to_cell_string"
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
        let [lat_vec, lon_vec, resolution_vec] = extract_args(self.name(), &args)?;

        let lat_vec = helpers::cast::<Float64Type>(&lat_vec)?;
        let lat_vec = lat_vec.as_primitive::<Float64Type>();
        let lon_vec = helpers::cast::<Float64Type>(&lon_vec)?;
        let lon_vec = lon_vec.as_primitive::<Float64Type>();
        let resolutions = helpers::cast::<UInt8Type>(&resolution_vec)?;
        let resolution_vec = resolutions.as_primitive::<UInt8Type>();

        let size = lat_vec.len();
        let mut builder = StringViewBuilder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.is_valid(i).then(|| lat_vec.value(i));
            let lon = lon_vec.is_valid(i).then(|| lon_vec.value(i));
            let r = resolution_vec
                .is_valid(i)
                .then(|| value_to_resolution(resolution_vec.value(i)))
                .transpose()?;

            let result = match (lat, lon, r) {
                (Some(lat), Some(lon), Some(r)) => {
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

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that converts cell id to its string form
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellToString {
    signature: Signature,
}

impl Default for H3CellToString {
    fn default() -> Self {
        Self {
            signature: signature_of_cell(),
        }
    }
}

impl Function for H3CellToString {
    fn name(&self) -> &str {
        "h3_cell_to_string"
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
        let [cell_vec] = extract_args(self.name(), &args)?;

        let size = cell_vec.len();
        let mut builder = StringViewBuilder::with_capacity(size);

        for i in 0..size {
            let v = ScalarValue::try_from_array(&cell_vec, i)
                .and_then(cell_from_value)?
                .map(|x| x.to_string());
            builder.append_option(v);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that converts cell string id to uint64 number
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3StringToCell {
    signature: Signature,
}

impl Default for H3StringToCell {
    fn default() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Stable),
        }
    }
}

impl Function for H3StringToCell {
    fn name(&self) -> &str {
        "h3_string_to_cell"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [string_vec] = extract_args(self.name(), &args)?;
        let string_vec = compute::cast(string_vec.as_ref(), &DataType::Utf8View)?;
        let string_vec = datafusion_common::downcast_value!(string_vec, StringViewArray);

        let size = string_vec.len();
        let mut builder = UInt64Builder::with_capacity(size);

        for i in 0..size {
            let cell_id = string_vec
                .is_valid(i)
                .then(|| {
                    CellIndex::from_str(string_vec.value(i))
                        .map_err(|e| DataFusionError::Execution(format!("H3 error: {}", e)))
                        .map(Into::into)
                })
                .transpose()?;

            builder.append_option(cell_id);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns centroid latitude and longitude of given cell id
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellCenterLatLng {
    signature: Signature,
}

impl Default for H3CellCenterLatLng {
    fn default() -> Self {
        Self {
            signature: signature_of_cell(),
        }
    }
}

impl Function for H3CellCenterLatLng {
    fn name(&self) -> &str {
        "h3_cell_center_latlng"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float64,
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
        let [cell_vec] = extract_args(self.name(), &args)?;

        let size = cell_vec.len();
        let mut builder = ListBuilder::new(Float64Builder::new());

        for i in 0..size {
            let cell = ScalarValue::try_from_array(&cell_vec, i).and_then(cell_from_value)?;
            let latlng = cell.map(LatLng::from);

            if let Some(latlng) = latlng {
                builder.values().append_value(latlng.lat());
                builder.values().append_value(latlng.lng());
                builder.append(true);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns resolution of given cell id
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellResolution {
    signature: Signature,
}

impl Default for H3CellResolution {
    fn default() -> Self {
        Self {
            signature: signature_of_cell(),
        }
    }
}

impl Function for H3CellResolution {
    fn name(&self) -> &str {
        "h3_cell_resolution"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [cell_vec] = extract_args(self.name(), &args)?;

        let size = cell_vec.len();
        let mut builder = UInt8Builder::with_capacity(cell_vec.len());

        for i in 0..size {
            let cell = ScalarValue::try_from_array(&cell_vec, i).and_then(cell_from_value)?;
            let res = cell.map(|cell| cell.resolution().into());
            builder.append_option(res);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns base cell of given cell id
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellBase {
    signature: Signature,
}

impl Default for H3CellBase {
    fn default() -> Self {
        Self {
            signature: signature_of_cell(),
        }
    }
}

impl Function for H3CellBase {
    fn name(&self) -> &str {
        "h3_cell_base"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [cell_vec] = extract_args(self.name(), &args)?;

        let size = cell_vec.len();
        let mut builder = UInt8Builder::with_capacity(size);

        for i in 0..size {
            let cell = ScalarValue::try_from_array(&cell_vec, i).and_then(cell_from_value)?;
            let res = cell.map(|cell| cell.base_cell().into());

            builder.append_option(res);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that check if given cell id is a pentagon
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellIsPentagon {
    signature: Signature,
}

impl Default for H3CellIsPentagon {
    fn default() -> Self {
        Self {
            signature: signature_of_cell(),
        }
    }
}

impl Function for H3CellIsPentagon {
    fn name(&self) -> &str {
        "h3_cell_is_pentagon"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [cell_vec] = extract_args(self.name(), &args)?;

        let size = cell_vec.len();
        let mut builder = BooleanBuilder::with_capacity(size);

        for i in 0..size {
            let cell = ScalarValue::try_from_array(&cell_vec, i).and_then(cell_from_value)?;
            let res = cell.map(|cell| cell.is_pentagon());

            builder.append_option(res);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns center child cell of given cell id
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellCenterChild {
    signature: Signature,
}

impl Default for H3CellCenterChild {
    fn default() -> Self {
        Self {
            signature: signature_of_cell_and_resolution(),
        }
    }
}

impl Function for H3CellCenterChild {
    fn name(&self) -> &str {
        "h3_cell_center_child"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        calculate_cell_child_property(self.name(), args, |cell, resolution| {
            cell.center_child(resolution).map(Into::into)
        })
    }
}

/// Function that returns parent cell of given cell id and resolution
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellParent {
    signature: Signature,
}

impl Default for H3CellParent {
    fn default() -> Self {
        Self {
            signature: signature_of_cell_and_resolution(),
        }
    }
}

impl Function for H3CellParent {
    fn name(&self) -> &str {
        "h3_cell_parent"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        calculate_cell_child_property(self.name(), args, |cell, resolution| {
            cell.parent(resolution).map(Into::into)
        })
    }
}

/// Function that returns children cell list
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellToChildren {
    signature: Signature,
}

impl Default for H3CellToChildren {
    fn default() -> Self {
        Self {
            signature: signature_of_cell_and_resolution(),
        }
    }
}

impl Function for H3CellToChildren {
    fn name(&self) -> &str {
        "h3_cell_to_children"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::UInt64,
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
        let [cell_vec, res_vec] = extract_args(self.name(), &args)?;
        let resolutions = helpers::cast::<UInt8Type>(&res_vec)?;
        let resolutions = resolutions.as_primitive::<UInt8Type>();

        let size = cell_vec.len();
        let mut builder = ListBuilder::new(UInt64Builder::new());

        for i in 0..size {
            let cell = ScalarValue::try_from_array(&cell_vec, i).and_then(cell_from_value)?;
            let resolution = resolutions
                .is_valid(i)
                .then(|| value_to_resolution(resolutions.value(i)))
                .transpose()?;

            match (cell, resolution) {
                (Some(c), Some(r)) => {
                    for x in c.children(r) {
                        builder.values().append_value(u64::from(x));
                    }
                    builder.append(true);
                }
                _ => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns children cell count
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellToChildrenSize {
    signature: Signature,
}

impl Default for H3CellToChildrenSize {
    fn default() -> Self {
        Self {
            signature: signature_of_cell_and_resolution(),
        }
    }
}

impl Function for H3CellToChildrenSize {
    fn name(&self) -> &str {
        "h3_cell_to_children_size"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        calculate_cell_child_property(self.name(), args, |cell, resolution| {
            Some(cell.children_count(resolution))
        })
    }
}

/// Function that returns the cell position if its parent at given resolution
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellToChildPos {
    signature: Signature,
}

impl Default for H3CellToChildPos {
    fn default() -> Self {
        Self {
            signature: signature_of_cell_and_resolution(),
        }
    }
}

impl Function for H3CellToChildPos {
    fn name(&self) -> &str {
        "h3_cell_to_child_pos"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        calculate_cell_child_property(self.name(), args, |cell, resolution| {
            cell.child_position(resolution)
        })
    }
}

fn calculate_cell_child_property<F>(
    name: &str,
    args: ScalarFunctionArgs,
    calculator: F,
) -> datafusion_common::Result<ColumnarValue>
where
    F: Fn(CellIndex, Resolution) -> Option<u64>,
{
    let [cells, resolutions] = extract_args(name, &args)?;
    let resolutions = helpers::cast::<UInt8Type>(&resolutions)?;
    let resolutions = resolutions.as_primitive::<UInt8Type>();

    let mut builder = UInt64Builder::with_capacity(cells.len());
    for i in 0..cells.len() {
        let cell = ScalarValue::try_from_array(&cells, i).and_then(cell_from_value)?;
        let resolution = resolutions
            .is_valid(i)
            .then(|| value_to_resolution(resolutions.value(i)))
            .transpose()?;
        let v = match (cell, resolution) {
            (Some(c), Some(r)) => calculator(c, r),
            _ => None,
        };
        builder.append_option(v);
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

/// Function that returns the cell at given position of the parent at given resolution
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3ChildPosToCell {
    signature: Signature,
}

impl Default for H3ChildPosToCell {
    fn default() -> Self {
        let mut signatures =
            Vec::with_capacity(POSITION_TYPES.len() * CELL_TYPES.len() * RESOLUTION_TYPES.len());
        for position_type in POSITION_TYPES {
            for cell_type in CELL_TYPES.as_slice() {
                for resolution_type in RESOLUTION_TYPES {
                    signatures.push(TypeSignature::Exact(vec![
                        position_type.clone(),
                        cell_type.clone(),
                        resolution_type.clone(),
                    ]));
                }
            }
        }
        Self {
            signature: Signature::one_of(signatures, Volatility::Stable),
        }
    }
}

impl Function for H3ChildPosToCell {
    fn name(&self) -> &str {
        "h3_child_pos_to_cell"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [pos_vec, cell_vec, res_vec] = extract_args(self.name(), &args)?;
        let resolutions = helpers::cast::<UInt8Type>(&res_vec)?;
        let resolutions = resolutions.as_primitive::<UInt8Type>();

        let size = cell_vec.len();
        let mut builder = UInt64Builder::with_capacity(size);

        for i in 0..size {
            let cell = ScalarValue::try_from_array(&cell_vec, i).and_then(cell_from_value)?;
            let pos = ScalarValue::try_from_array(&pos_vec, i).and_then(value_to_position)?;
            let resolution = resolutions
                .is_valid(i)
                .then(|| value_to_resolution(resolutions.value(i)))
                .transpose()?;
            let result = match (cell, resolution) {
                (Some(c), Some(r)) => c.child_at(pos, r).map(u64::from),
                _ => None,
            };
            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns cells with k distances of given cell
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3GridDisk {
    signature: Signature,
}

impl Default for H3GridDisk {
    fn default() -> Self {
        Self {
            signature: signature_of_cell_and_distance(),
        }
    }
}

impl Function for H3GridDisk {
    fn name(&self) -> &str {
        "h3_grid_disk"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::UInt64,
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
        let [cell_vec, k_vec] = extract_args(self.name(), &args)?;

        let size = cell_vec.len();
        let mut builder = ListBuilder::new(UInt64Builder::new());

        for i in 0..size {
            let cell = ScalarValue::try_from_array(&cell_vec, i).and_then(cell_from_value)?;
            let k = ScalarValue::try_from_array(&k_vec, i).and_then(value_to_distance)?;

            if let Some(cell) = cell {
                for x in cell.grid_disk::<Vec<_>>(k) {
                    builder.values().append_value(u64::from(x));
                }
                builder.append(true);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns all cells within k distances of given cell
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3GridDiskDistances {
    signature: Signature,
}

impl Default for H3GridDiskDistances {
    fn default() -> Self {
        Self {
            signature: signature_of_cell_and_distance(),
        }
    }
}

impl Function for H3GridDiskDistances {
    fn name(&self) -> &str {
        "h3_grid_disk_distances"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::UInt64,
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
        let [cell_vec, k_vec] = extract_args(self.name(), &args)?;

        let size = cell_vec.len();
        let mut builder = ListBuilder::new(UInt64Builder::new());

        for i in 0..size {
            let cell = ScalarValue::try_from_array(&cell_vec, i).and_then(cell_from_value)?;
            let k = ScalarValue::try_from_array(&k_vec, i).and_then(value_to_distance)?;

            if let Some(cell) = cell {
                for (x, _) in cell.grid_disk_distances::<Vec<_>>(k) {
                    builder.values().append_value(u64::from(x));
                }
                builder.append(true);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns distance between two cells
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3GridDistance {
    signature: Signature,
}

impl Default for H3GridDistance {
    fn default() -> Self {
        Self {
            signature: signature_of_double_cells(),
        }
    }
}

impl Function for H3GridDistance {
    fn name(&self) -> &str {
        "h3_grid_distance"
    }
    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Int32)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [cell_this_vec, cell_that_vec] = extract_args(self.name(), &args)?;

        let size = cell_this_vec.len();
        let mut builder = Int32Builder::with_capacity(size);

        for i in 0..size {
            let cell_this =
                ScalarValue::try_from_array(&cell_this_vec, i).and_then(cell_from_value)?;
            let cell_that =
                ScalarValue::try_from_array(&cell_that_vec, i).and_then(cell_from_value)?;
            let result = match (cell_this, cell_that) {
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

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that returns path cells between two cells
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3GridPathCells {
    signature: Signature,
}

impl Default for H3GridPathCells {
    fn default() -> Self {
        Self {
            signature: signature_of_double_cells(),
        }
    }
}

impl Function for H3GridPathCells {
    fn name(&self) -> &str {
        "h3_grid_path_cells"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::UInt64,
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
        let [cell_this_vec, cell_that_vec] = extract_args(self.name(), &args)?;

        let size = cell_this_vec.len();
        let mut builder = ListBuilder::new(UInt64Builder::new());

        for i in 0..size {
            let cell_this =
                ScalarValue::try_from_array(&cell_this_vec, i).and_then(cell_from_value)?;
            let cell_that =
                ScalarValue::try_from_array(&cell_that_vec, i).and_then(cell_from_value)?;
            match (cell_this, cell_that) {
                (Some(cell_this), Some(cell_that)) => {
                    let cells = cell_this
                        .grid_path_cells(cell_that)
                        .map_err(|e| DataFusionError::Execution(format!("H3 error: {}", e)))?;
                    for cell in cells {
                        let cell = cell
                            .map_err(|e| DataFusionError::Execution(format!("H3 error: {}", e)))?;
                        builder.values().append_value(u64::from(cell));
                    }
                    builder.append(true);
                }
                _ => builder.append_null(),
            };
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Tests if cells contains given cells
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellContains {
    signature: Signature,
}

impl Default for H3CellContains {
    fn default() -> Self {
        let multi_cell_types = vec![
            DataType::new_list(DataType::Int64, true),
            DataType::new_list(DataType::UInt64, true),
            DataType::new_list(DataType::Utf8, true),
            DataType::Utf8,
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
        Self {
            signature: Signature::one_of(signatures, Volatility::Stable),
        }
    }
}

impl Function for H3CellContains {
    fn name(&self) -> &str {
        "h3_cells_contains"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [cells_vec, cell_this_vec] = extract_args(self.name(), &args)?;

        let size = cell_this_vec.len();
        let mut builder = BooleanBuilder::with_capacity(size);

        for i in 0..size {
            let cells = ScalarValue::try_from_array(&cells_vec, i).and_then(cells_from_value)?;
            let cell_this =
                ScalarValue::try_from_array(&cell_this_vec, i).and_then(cell_from_value)?;
            let mut result = None;
            if let (cells, Some(cell_this)) = (cells, cell_this) {
                result = Some(false);

                for cell_that in cells.iter() {
                    // get cell resolution, and find cell_this's parent at
                    //  this solution, test if cell_that equals the parent
                    let resolution = cell_that.resolution();
                    if let Some(cell_this_parent) = cell_this.parent(resolution)
                        && cell_this_parent == *cell_that
                    {
                        result = Some(true);
                        break;
                    }
                }
            }

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Get WGS84 great circle distance of two cell centroid
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellDistanceSphereKm {
    signature: Signature,
}

impl Default for H3CellDistanceSphereKm {
    fn default() -> Self {
        Self {
            signature: signature_of_double_cells(),
        }
    }
}

impl Function for H3CellDistanceSphereKm {
    fn name(&self) -> &str {
        "h3_distance_sphere_km"
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
        let [cell_this_vec, cell_that_vec] = extract_args(self.name(), &args)?;

        let size = cell_this_vec.len();
        let mut builder = Float64Builder::with_capacity(size);

        for i in 0..size {
            let cell_this =
                ScalarValue::try_from_array(&cell_this_vec, i).and_then(cell_from_value)?;
            let cell_that =
                ScalarValue::try_from_array(&cell_that_vec, i).and_then(cell_from_value)?;
            let result = match (cell_this, cell_that) {
                (Some(cell_this), Some(cell_that)) => {
                    let centroid_this = LatLng::from(cell_this);
                    let centroid_that = LatLng::from(cell_that);

                    Some(centroid_this.distance_km(centroid_that))
                }
                _ => None,
            };

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Get Euclidean distance of two cell centroid
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct H3CellDistanceEuclideanDegree {
    signature: Signature,
}

impl Default for H3CellDistanceEuclideanDegree {
    fn default() -> Self {
        Self {
            signature: signature_of_double_cells(),
        }
    }
}

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
        let [cell_this_vec, cell_that_vec] = extract_args(self.name(), &args)?;

        let size = cell_this_vec.len();
        let mut builder = Float64Builder::with_capacity(size);

        for i in 0..size {
            let cell_this =
                ScalarValue::try_from_array(&cell_this_vec, i).and_then(cell_from_value)?;
            let cell_that =
                ScalarValue::try_from_array(&cell_that_vec, i).and_then(cell_from_value)?;
            let result = match (cell_this, cell_that) {
                (Some(cell_this), Some(cell_that)) => {
                    let centroid_this = LatLng::from(cell_this);
                    let centroid_that = LatLng::from(cell_that);

                    let dist = Self::distance(centroid_this, centroid_that);
                    Some(dist)
                }
                _ => None,
            };

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn value_to_resolution(r: u8) -> datafusion_common::Result<Resolution> {
    Resolution::try_from(r).map_err(|e| DataFusionError::Execution(format!("H3 error: {}", e)))
}

macro_rules! ensure_then_coerce {
    ($compare:expr, $coerce:expr) => {{
        if !$compare {
            return Err(datafusion_common::DataFusionError::Execution(
                "Argument was outside of acceptable range".to_string(),
            ));
        }
        Ok($coerce)
    }};
}

fn value_to_position(v: ScalarValue) -> datafusion_common::Result<u64> {
    match v {
        ScalarValue::Int8(Some(v)) => ensure_then_coerce!(v >= 0, v as u64),
        ScalarValue::Int16(Some(v)) => ensure_then_coerce!(v >= 0, v as u64),
        ScalarValue::Int32(Some(v)) => ensure_then_coerce!(v >= 0, v as u64),
        ScalarValue::Int64(Some(v)) => ensure_then_coerce!(v >= 0, v as u64),
        ScalarValue::UInt8(Some(v)) => Ok(v as u64),
        ScalarValue::UInt16(Some(v)) => Ok(v as u64),
        ScalarValue::UInt32(Some(v)) => Ok(v as u64),
        ScalarValue::UInt64(Some(v)) => Ok(v),
        _ => unreachable!(),
    }
}

fn value_to_distance(v: ScalarValue) -> datafusion_common::Result<u32> {
    match v {
        ScalarValue::Int8(Some(v)) => ensure_then_coerce!(v >= 0, v as u32),
        ScalarValue::Int16(Some(v)) => ensure_then_coerce!(v >= 0, v as u32),
        ScalarValue::Int32(Some(v)) => ensure_then_coerce!(v >= 0, v as u32),
        ScalarValue::Int64(Some(v)) => ensure_then_coerce!(v >= 0, v as u32),
        ScalarValue::UInt8(Some(v)) => Ok(v as u32),
        ScalarValue::UInt16(Some(v)) => Ok(v as u32),
        ScalarValue::UInt32(Some(v)) => Ok(v),
        ScalarValue::UInt64(Some(v)) => Ok(v as u32),
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
        for resolution_type in RESOLUTION_TYPES {
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
        for distance_type in DISTANCE_TYPES {
            signatures.push(TypeSignature::Exact(vec![
                cell_type.clone(),
                distance_type.clone(),
            ]));
        }
    }
    Signature::one_of(signatures, Volatility::Stable)
}

fn cell_from_value(v: ScalarValue) -> datafusion_common::Result<Option<CellIndex>> {
    match v {
        ScalarValue::Int64(Some(v)) => Some(CellIndex::try_from(v as u64)),
        ScalarValue::UInt64(Some(v)) => Some(CellIndex::try_from(v)),
        ScalarValue::Utf8(Some(s)) => Some(CellIndex::from_str(&s)),
        _ => None,
    }
    .transpose()
    .map_err(|e| DataFusionError::Execution(format!("H3 error: {}", e)))
}

/// extract cell array from all possible types including:
/// - int64 list
/// - uint64 list
/// - string list
/// - comma-separated string
fn cells_from_value(v: ScalarValue) -> datafusion_common::Result<Vec<CellIndex>> {
    match v {
        ScalarValue::List(list) => match list.value_type() {
            DataType::Int64 => list
                .values()
                .as_primitive::<Int64Type>()
                .iter()
                .map(|v| {
                    if let Some(v) = v {
                        CellIndex::try_from(v as u64)
                            .map_err(|e| DataFusionError::Execution(format!("H3 error: {}", e)))
                    } else {
                        Err(DataFusionError::Execution(
                            "Invalid data type in array".to_string(),
                        ))
                    }
                })
                .collect::<datafusion_common::Result<Vec<CellIndex>>>(),
            DataType::UInt64 => list
                .values()
                .as_primitive::<UInt64Type>()
                .iter()
                .map(|v| {
                    if let Some(v) = v {
                        CellIndex::try_from(v)
                            .map_err(|e| DataFusionError::Execution(format!("H3 error: {}", e)))
                    } else {
                        Err(DataFusionError::Execution(
                            "Invalid data type in array".to_string(),
                        ))
                    }
                })
                .collect::<datafusion_common::Result<Vec<CellIndex>>>(),
            DataType::Utf8 => list
                .values()
                .as_string::<i32>()
                .iter()
                .map(|v| {
                    if let Some(v) = v {
                        CellIndex::from_str(v)
                            .map_err(|e| DataFusionError::Execution(format!("H3 error: {}", e)))
                    } else {
                        Err(DataFusionError::Execution(
                            "Invalid data type in array".to_string(),
                        ))
                    }
                })
                .collect::<datafusion_common::Result<Vec<CellIndex>>>(),
            _ => Ok(vec![]),
        },
        ScalarValue::Utf8(Some(csv)) => {
            let str_seq = csv.split(',');
            str_seq
                .map(|v| {
                    CellIndex::from_str(v.trim())
                        .map_err(|e| DataFusionError::Execution(format!("H3 error: {}", e)))
                })
                .collect::<datafusion_common::Result<Vec<CellIndex>>>()
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
