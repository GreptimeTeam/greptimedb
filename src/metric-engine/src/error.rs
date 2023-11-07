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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use store_api::storage::{RegionId, TableId};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Missing internal column {} in physical metric table", column))]
    MissingInternalColumn { column: String, location: Location },

    #[snafu(display("Failed to create mito region, region type: {}", region_type))]
    CreateMitoRegion {
        region_type: String,
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Table `{}` already exists", table_id))]
    TableAlreadyExists {
        table_id: TableId,
        location: Location,
    },

    #[snafu(display("Failed to deserialize semantic type from {}", raw))]
    DeserializeSemanticType {
        raw: String,
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Failed to decode base64 column value"))]
    DecodeColumnValue {
        #[snafu(source)]
        error: base64::DecodeError,
        location: Location,
    },

    #[snafu(display("Failed to parse table id from {}", raw))]
    ParseTableId {
        raw: String,
        #[snafu(source)]
        error: <TableId as std::str::FromStr>::Err,
        location: Location,
    },

    #[snafu(display("Mito read operation fails"))]
    MitoReadOperation {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Mito write operation fails"))]
    MitoWriteOperation {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Failed to collect record batch stream"))]
    CollectRecordBatchStream {
        source: common_recordbatch::error::Error,
        location: Location,
    },

    #[snafu(display("Internal column {} is reserved", column))]
    InternalColumnOccupied { column: String, location: Location },

    #[snafu(display("Required table option is missing"))]
    MissingRegionOption { location: Location },

    #[snafu(display("Region options are conflicted"))]
    ConflictRegionOption { location: Location },

    // TODO: remove this
    #[snafu(display("Physical table {} not found", physical_table))]
    PhysicalTableNotFound {
        physical_table: String,
        location: Location,
    },

    #[snafu(display("Physical region {} not found", region_id))]
    PhysicalRegionNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Logical table {} not found", table_id))]
    LogicalTableNotFound {
        table_id: TableId,
        location: Location,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            InternalColumnOccupied { .. }
            | MissingRegionOption { .. }
            | ConflictRegionOption { .. } => StatusCode::InvalidArguments,

            MissingInternalColumn { .. }
            | DeserializeSemanticType { .. }
            | DecodeColumnValue { .. }
            | ParseTableId { .. } => StatusCode::Unexpected,

            PhysicalTableNotFound { .. } | LogicalTableNotFound { .. } => StatusCode::TableNotFound,

            PhysicalRegionNotFound { .. } => StatusCode::RegionNotFound,

            CreateMitoRegion { source, .. }
            | MitoReadOperation { source, .. }
            | MitoWriteOperation { source, .. } => source.status_code(),

            CollectRecordBatchStream { source, .. } => source.status_code(),

            TableAlreadyExists { .. } => StatusCode::TableAlreadyExists,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
