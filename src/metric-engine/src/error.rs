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
use datatypes::prelude::ConcreteDataType;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;

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

    #[snafu(display("Failed to open mito region, region type: {}", region_type))]
    OpenMitoRegion {
        region_type: String,
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Failed to close mito region, region id: {}", region_id))]
    CloseMitoRegion {
        region_id: RegionId,
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Region `{}` already exists", region_id))]
    RegionAlreadyExists {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Failed to deserialize semantic type from {}", raw))]
    DeserializeSemanticType {
        raw: String,
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Failed to deserialize column metadata from {}", raw))]
    DeserializeColumnMetadata {
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

    #[snafu(display("Failed to parse region id from {}", raw))]
    ParseRegionId {
        raw: String,
        #[snafu(source)]
        error: <u64 as std::str::FromStr>::Err,
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

    #[snafu(display("Physical region {} not found", region_id))]
    PhysicalRegionNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Logical region {} not found", region_id))]
    LogicalRegionNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Column type mismatch. Expect string, got {:?}", column_type))]
    ColumnTypeMismatch {
        column_type: ConcreteDataType,
        location: Location,
    },

    #[snafu(display("Column {} not found in logical region {}", name, region_id))]
    ColumnNotFound {
        name: String,
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Alter request to physical region is forbidden"))]
    ForbiddenPhysicalAlter { location: Location },

    #[snafu(display("Invalid region metadata"))]
    InvalidMetadata {
        source: store_api::metadata::MetadataError,
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
            | ConflictRegionOption { .. }
            | ColumnTypeMismatch { .. } => StatusCode::InvalidArguments,

            ForbiddenPhysicalAlter { .. } => StatusCode::Unsupported,

            MissingInternalColumn { .. }
            | DeserializeSemanticType { .. }
            | DeserializeColumnMetadata { .. }
            | DecodeColumnValue { .. }
            | ParseRegionId { .. }
            | InvalidMetadata { .. } => StatusCode::Unexpected,

            PhysicalRegionNotFound { .. } | LogicalRegionNotFound { .. } => {
                StatusCode::RegionNotFound
            }

            ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            CreateMitoRegion { source, .. }
            | OpenMitoRegion { source, .. }
            | CloseMitoRegion { source, .. }
            | MitoReadOperation { source, .. }
            | MitoWriteOperation { source, .. } => source.status_code(),

            CollectRecordBatchStream { source, .. } => source.status_code(),

            RegionAlreadyExists { .. } => StatusCode::RegionAlreadyExists,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
