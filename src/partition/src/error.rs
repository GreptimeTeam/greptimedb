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

use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_query::prelude::Expr;
use datafusion_common::ScalarValue;
use snafu::{Location, Snafu};
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Table route manager error"))]
    TableRouteManager {
        source: common_meta::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to get meta info from cache, error: {}", err_msg))]
    GetCache { err_msg: String, location: Location },

    #[snafu(display("Failed to find Datanode, table id: {}, region: {}", table_id, region))]
    FindDatanode {
        table_id: TableId,
        region: RegionNumber,
        location: Location,
    },

    #[snafu(display("Failed to find table routes for table id {}", table_id))]
    FindTableRoutes {
        table_id: TableId,
        location: Location,
    },

    #[snafu(display(
        "Failed to find region routes for table {}, region id: {}",
        table_id,
        region_id
    ))]
    FindRegionRoutes {
        table_id: TableId,
        region_id: u64,
        location: Location,
    },

    #[snafu(display("Failed to serialize value to json"))]
    SerializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Failed to deserialize value from json"))]
    DeserializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },

    #[snafu(display("The column '{}' does not have a default value.", column))]
    MissingDefaultValue { column: String },

    #[snafu(display("Expect {} region keys, actual {}", expect, actual))]
    RegionKeysSize {
        expect: usize,
        actual: usize,
        location: Location,
    },

    #[snafu(display("Failed to find region, reason: {}", reason))]
    FindRegion { reason: String, location: Location },

    #[snafu(display("Failed to find regions by filters: {:?}", filters))]
    FindRegions {
        filters: Vec<Expr>,
        location: Location,
    },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest { reason: String, location: Location },

    #[snafu(display("Invalid DeleteRequest, reason: {}", reason))]
    InvalidDeleteRequest { reason: String, location: Location },

    #[snafu(display("Invalid table route data, table id: {}, msg: {}", table_id, err_msg))]
    InvalidTableRouteData {
        table_id: TableId,
        err_msg: String,
        location: Location,
    },

    #[snafu(display("Failed to convert DataFusion's ScalarValue: {:?}", value))]
    ConvertScalarValue {
        value: ScalarValue,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to find leader of table id {} region {}", table_id, region_id))]
    FindLeader {
        table_id: TableId,
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Unexpected table route type: {}", err_msg))]
    UnexpectedLogicalRouteTable {
        location: Location,
        err_msg: String,
        source: common_meta::error::Error,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::GetCache { .. } | Error::FindLeader { .. } => StatusCode::StorageUnavailable,
            Error::FindRegionRoutes { .. } => StatusCode::InvalidArguments,
            Error::FindTableRoutes { .. } => StatusCode::InvalidArguments,
            Error::FindRegion { .. }
            | Error::FindRegions { .. }
            | Error::RegionKeysSize { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::InvalidDeleteRequest { .. } => StatusCode::InvalidArguments,
            Error::SerializeJson { .. } | Error::DeserializeJson { .. } => StatusCode::Internal,
            Error::InvalidTableRouteData { .. } => StatusCode::Internal,
            Error::ConvertScalarValue { .. } => StatusCode::Internal,
            Error::FindDatanode { .. } => StatusCode::InvalidArguments,
            Error::TableRouteManager { source, .. } => source.status_code(),
            Error::MissingDefaultValue { .. } => StatusCode::Internal,
            Error::UnexpectedLogicalRouteTable { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
