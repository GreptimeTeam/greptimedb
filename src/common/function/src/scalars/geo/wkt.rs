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

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error::{self, Result};
use datafusion_common::arrow::array::{Array, AsArray, StringViewBuilder};
use datafusion_common::arrow::datatypes::{DataType, Float64Type};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use derive_more::Display;
use geo_types::{Geometry, Point};
use snafu::ResultExt;
use wkt::{ToWkt, TryFromWkt};

use crate::function::{Function, extract_args};
use crate::scalars::geo::helpers;

static COORDINATE_TYPES: LazyLock<Vec<DataType>> =
    LazyLock::new(|| vec![DataType::Float32, DataType::Float64]);

/// Return WGS84(SRID: 4326) euclidean distance between two geometry object, in degree
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct LatLngToPointWkt;

impl Function for LatLngToPointWkt {
    fn name(&self) -> &str {
        "wkt_point_from_latlng"
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> Signature {
        let mut signatures = Vec::new();
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

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let arg0 = helpers::cast::<Float64Type>(&arg0)?;
        let lat_vec = arg0.as_primitive::<Float64Type>();
        let arg1 = helpers::cast::<Float64Type>(&arg1)?;
        let lng_vec = arg1.as_primitive::<Float64Type>();

        let size = lat_vec.len();
        let mut builder = StringViewBuilder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.is_valid(i).then(|| lat_vec.value(i));
            let lng = lng_vec.is_valid(i).then(|| lng_vec.value(i));

            let result = match (lat, lng) {
                (Some(lat), Some(lng)) => Some(Point::new(lng, lat).wkt_string()),
                _ => None,
            };

            builder.append_option(result.as_deref());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

pub(super) fn parse_wkt(s: &str) -> Result<Geometry> {
    Geometry::try_from_wkt_str(s)
        .map_err(|e| {
            BoxedError::new(PlainError::new(
                format!("Fail to parse WKT: {}", e),
                StatusCode::EngineExecuteQuery,
            ))
        })
        .context(error::ExecuteSnafu)
}
