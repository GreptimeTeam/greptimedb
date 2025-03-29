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
use datafusion_common::ScalarValue;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::expr::PartitionExpr;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to find table route: {}", table_id))]
    TableRouteNotFound {
        table_id: TableId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table route manager error"))]
    TableRouteManager {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get meta info from cache, error: {}", err_msg))]
    GetCache {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find table routes for table id {}", table_id))]
    FindTableRoutes {
        table_id: TableId,
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize value to json"))]
    SerializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize value from json"))]
    DeserializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expect {} region keys, actual {}", expect, actual))]
    RegionKeysSize {
        expect: usize,
        actual: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid DeleteRequest, reason: {}", reason))]
    InvalidDeleteRequest {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid table route data, table id: {}, msg: {}", table_id, err_msg))]
    InvalidTableRouteData {
        table_id: TableId,
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert DataFusion's ScalarValue: {:?}", value))]
    ConvertScalarValue {
        value: ScalarValue,
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to find leader of table id {} region {}", table_id, region_id))]
    FindLeader {
        table_id: TableId,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected table route type: {}", err_msg))]
    UnexpectedLogicalRouteTable {
        #[snafu(implicit)]
        location: Location,
        err_msg: String,
        source: common_meta::error::Error,
    },

    #[snafu(display("Conjunct expr with non-expr is invalid"))]
    ConjunctExprWithNonExpr {
        expr: PartitionExpr,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unclosed value {} on column {}", value, column))]
    UnclosedValue {
        value: String,
        column: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid partition expr: {:?}", expr))]
    InvalidExpr {
        expr: PartitionExpr,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Undefined column: {}", column))]
    UndefinedColumn {
        column: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected: {err_msg}"))]
    Unexpected {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::GetCache { .. } => StatusCode::StorageUnavailable,
            Error::FindLeader { .. } => StatusCode::TableUnavailable,

            Error::ConjunctExprWithNonExpr { .. }
            | Error::UnclosedValue { .. }
            | Error::InvalidExpr { .. }
            | Error::UndefinedColumn { .. } => StatusCode::InvalidArguments,

            Error::RegionKeysSize { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::InvalidDeleteRequest { .. } => StatusCode::InvalidArguments,

            Error::ConvertScalarValue { .. }
            | Error::SerializeJson { .. }
            | Error::DeserializeJson { .. } => StatusCode::Internal,

            Error::Unexpected { .. }
            | Error::InvalidTableRouteData { .. }
            | Error::FindTableRoutes { .. }
            | Error::FindRegionRoutes { .. } => StatusCode::Unexpected,
            Error::TableRouteNotFound { .. } => StatusCode::TableNotFound,
            Error::TableRouteManager { source, .. } => source.status_code(),
            Error::UnexpectedLogicalRouteTable { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
