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
use datatypes::vectors::{MutableVector, StringVectorBuilder, VectorRef};
use derive_more::Display;
use geo_types::{Geometry, Point};
use once_cell::sync::Lazy;
use snafu::ResultExt;
use wkt::{ToWkt, TryFromWkt};

use super::helpers::{ensure_columns_len, ensure_columns_n};
use crate::function::{Function, FunctionContext};

static COORDINATE_TYPES: Lazy<Vec<ConcreteDataType>> = Lazy::new(|| {
    vec![
        ConcreteDataType::float32_datatype(),
        ConcreteDataType::float64_datatype(),
    ]
});

/// Return WGS84(SRID: 4326) euclidean distance between two geometry object, in degree
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct LatLngToPointWkt;

impl Function for LatLngToPointWkt {
    fn name(&self) -> &str {
        "wkt_point_from_latlng"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
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

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 2);

        let lat_vec = &columns[0];
        let lng_vec = &columns[1];

        let size = lat_vec.len();
        let mut results = StringVectorBuilder::with_capacity(size);

        for i in 0..size {
            let lat = lat_vec.get(i).as_f64_lossy();
            let lng = lng_vec.get(i).as_f64_lossy();

            let result = match (lat, lng) {
                (Some(lat), Some(lng)) => Some(Point::new(lng, lat).wkt_string()),
                _ => None,
            };

            results.push(result.as_deref());
        }

        Ok(results.to_vector())
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
