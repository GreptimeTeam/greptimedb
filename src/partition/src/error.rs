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

use common_error::prelude::*;
use common_query::prelude::Expr;
use datafusion_common::ScalarValue;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to get meta info from cache, error: {}", err_msg))]
    GetCache { err_msg: String, location: Location },

    #[snafu(display("Failed to request Meta, source: {}", source))]
    RequestMeta {
        #[snafu(backtrace)]
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to find Datanode, table: {} region: {:?}", table, region))]
    FindDatanode {
        table: String,
        region: RegionId,
        location: Location,
    },

    #[snafu(display("Failed to find table routes for table {}", table_name))]
    FindTableRoutes {
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to find region routes for table {}, region id: {}",
        table_name,
        region_id
    ))]
    FindRegionRoutes {
        table_name: String,
        region_id: u64,
        location: Location,
    },

    #[snafu(display("Failed to serialize value to json, source: {}", source))]
    SerializeJson {
        source: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Failed to deserialize value from json, source: {}", source))]
    DeserializeJson {
        source: serde_json::Error,
        location: Location,
    },

    #[snafu(display(
        "Failed to read column {}, could not create default value, source: {}",
        column,
        source
    ))]
    CreateDefaultToRead {
        column: String,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
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

    #[snafu(display("Failed to find partition column: {}", column_name))]
    FindPartitionColumn {
        column_name: String,
        location: Location,
    },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest { reason: String, location: Location },

    #[snafu(display("Invalid DeleteRequest, reason: {}", reason))]
    InvalidDeleteRequest { reason: String, location: Location },

    #[snafu(display(
        "Invalid table route data in meta, table name: {}, msg: {}",
        table_name,
        err_msg
    ))]
    InvalidTableRouteData {
        table_name: String,
        err_msg: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to convert DataFusion's ScalarValue: {:?}, source: {}",
        value,
        source
    ))]
    ConvertScalarValue {
        value: ScalarValue,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to find leader of table {} region {}", table_name, region_id))]
    FindLeader {
        table_name: String,
        region_id: RegionId,
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::GetCache { .. } | Error::FindLeader { .. } => StatusCode::StorageUnavailable,
            Error::FindRegionRoutes { .. } => StatusCode::InvalidArguments,
            Error::FindTableRoutes { .. } => StatusCode::InvalidArguments,
            Error::RequestMeta { source, .. } => source.status_code(),
            Error::FindRegion { .. }
            | Error::FindRegions { .. }
            | Error::RegionKeysSize { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::InvalidDeleteRequest { .. }
            | Error::FindPartitionColumn { .. } => StatusCode::InvalidArguments,
            Error::SerializeJson { .. } | Error::DeserializeJson { .. } => StatusCode::Internal,
            Error::InvalidTableRouteData { .. } => StatusCode::Internal,
            Error::ConvertScalarValue { .. } => StatusCode::Internal,
            Error::FindDatanode { .. } => StatusCode::InvalidArguments,
            Error::CreateDefaultToRead { source, .. } => source.status_code(),
            Error::MissingDefaultValue { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
