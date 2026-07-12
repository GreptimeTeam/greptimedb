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

use std::sync::{Arc, LazyLock};

use common_query::error::InvalidFuncArgsSnafu;
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::{Array, AsArray, StringViewBuilder, UInt64Builder};
use datafusion_common::arrow::datatypes::{DataType, Float64Type};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use derive_more::Display;
use s2::cellid::{CellID, MAX_LEVEL};
use s2::latlng::LatLng;
use snafu::ensure;

use crate::function::{Function, extract_args};
use crate::scalars::geo::helpers;
use crate::scalars::geo::helpers::ensure_and_coerce;

static CELL_TYPES: LazyLock<Vec<DataType>> =
    LazyLock::new(|| vec![DataType::Int64, DataType::UInt64]);

static COORDINATE_TYPES: LazyLock<Vec<DataType>> =
    LazyLock::new(|| vec![DataType::Float32, DataType::Float64]);

static LEVEL_TYPES: &[DataType] = datafusion_expr::type_coercion::aggregates::INTEGERS;

/// Function that returns [s2] encoding cellid for a given geospatial coordinate.
///
/// [s2]: http://s2geometry.io
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct S2LatLngToCell {
    signature: Signature,
}

impl Default for S2LatLngToCell {
    fn default() -> Self {
        let mut signatures = Vec::with_capacity(COORDINATE_TYPES.len());
        for coord_type in COORDINATE_TYPES.as_slice() {
            signatures.push(TypeSignature::Exact(vec![
                // latitude
                coord_type.clone(),
                // longitude
                coord_type.clone(),
            ]));
        }
        Self {
            signature: Signature::one_of(signatures, Volatility::Stable),
        }
    }
}

impl Function for S2LatLngToCell {
    fn name(&self) -> &str {
        "s2_latlng_to_cell"
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
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let arg0 = helpers::cast::<Float64Type>(&arg0)?;
        let lat_vec = arg0.as_primitive::<Float64Type>();
        let arg1 = helpers::cast::<Float64Type>(&arg1)?;
        let lon_vec = arg1.as_primitive::<Float64Type>();

        let size = lat_vec.len();
        let mut builder = UInt64Builder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.is_valid(i).then(|| lat_vec.value(i));
            let lon = lon_vec.is_valid(i).then(|| lon_vec.value(i));

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

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Return the level of current s2 cell
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct S2CellLevel {
    signature: Signature,
}

impl Default for S2CellLevel {
    fn default() -> Self {
        Self {
            signature: signature_of_cell(),
        }
    }
}

impl Function for S2CellLevel {
    fn name(&self) -> &str {
        "s2_cell_level"
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
        let [cell_vec] = extract_args(self.name(), &args)?;

        let size = cell_vec.len();
        let mut builder = UInt64Builder::with_capacity(size);

        for i in 0..size {
            let v = ScalarValue::try_from_array(&cell_vec, i)?;
            let v = cell_from_value(v).map(|x| x.level());

            builder.append_option(v);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Return the string presentation of the cell
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct S2CellToToken {
    signature: Signature,
}

impl Default for S2CellToToken {
    fn default() -> Self {
        Self {
            signature: signature_of_cell(),
        }
    }
}

impl Function for S2CellToToken {
    fn name(&self) -> &str {
        "s2_cell_to_token"
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
            let v = ScalarValue::try_from_array(&cell_vec, i)?;
            let v = cell_from_value(v).map(|x| x.to_token());

            builder.append_option(v.as_deref());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Return parent at given level of current s2 cell
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct S2CellParent {
    signature: Signature,
}

impl Default for S2CellParent {
    fn default() -> Self {
        Self {
            signature: signature_of_cell_and_level(),
        }
    }
}

impl Function for S2CellParent {
    fn name(&self) -> &str {
        "s2_cell_parent"
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
        let [cell_vec, levels] = extract_args(self.name(), &args)?;

        let size = cell_vec.len();
        let mut builder = UInt64Builder::with_capacity(size);

        for i in 0..size {
            let cell = ScalarValue::try_from_array(&cell_vec, i).map(cell_from_value)?;
            let level = ScalarValue::try_from_array(&levels, i).and_then(value_to_level)?;
            let result = if let (Some(cell), Some(level)) = (cell, level) {
                Some(cell.parent(level).0)
            } else {
                None
            };

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
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
        for level_type in LEVEL_TYPES {
            signatures.push(TypeSignature::Exact(vec![
                cell_type.clone(),
                level_type.clone(),
            ]));
        }
    }
    Signature::one_of(signatures, Volatility::Stable)
}

fn cell_from_value(v: ScalarValue) -> Option<CellID> {
    match v {
        ScalarValue::Int64(v) => v.map(|x| CellID(x as u64)),
        ScalarValue::UInt64(v) => v.map(CellID),
        _ => None,
    }
}

fn value_to_level(v: ScalarValue) -> datafusion_common::Result<Option<u64>> {
    match v {
        ScalarValue::Int8(Some(v)) => ensure_and_coerce!(v >= 0 && v <= MAX_LEVEL as i8, v as u64),
        ScalarValue::Int16(Some(v)) => {
            ensure_and_coerce!(v >= 0 && v <= MAX_LEVEL as i16, v as u64)
        }
        ScalarValue::Int32(Some(v)) => {
            ensure_and_coerce!(v >= 0 && v <= MAX_LEVEL as i32, v as u64)
        }
        ScalarValue::Int64(Some(v)) => {
            ensure_and_coerce!(v >= 0 && v <= MAX_LEVEL as i64, v as u64)
        }
        ScalarValue::UInt8(Some(v)) => ensure_and_coerce!(v <= MAX_LEVEL as u8, v as u64),
        ScalarValue::UInt16(Some(v)) => ensure_and_coerce!(v <= MAX_LEVEL as u16, v as u64),
        ScalarValue::UInt32(Some(v)) => ensure_and_coerce!(v <= MAX_LEVEL as u32, v as u64),
        ScalarValue::UInt64(Some(v)) => ensure_and_coerce!(v <= MAX_LEVEL, v),
        _ => Ok(None),
    }
}
