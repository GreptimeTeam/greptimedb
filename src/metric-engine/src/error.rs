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
use store_api::region_request::RegionRequest;
use store_api::storage::RegionId;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to create mito region, region type: {}", region_type))]
    CreateMitoRegion {
        region_type: String,
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to open mito region, region type: {}", region_type))]
    OpenMitoRegion {
        region_type: String,
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to close mito region, region id: {}", region_id))]
    CloseMitoRegion {
        region_id: RegionId,
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize column metadata from {}", raw))]
    DeserializeColumnMetadata {
        raw: String,
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize column metadata"))]
    SerializeColumnMetadata {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize region manifest info"))]
    SerializeRegionManifestInfo {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode base64 column value"))]
    DecodeColumnValue {
        #[snafu(source)]
        error: base64::DecodeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse region id from {}", raw))]
    ParseRegionId {
        raw: String,
        #[snafu(source)]
        error: <u64 as std::str::FromStr>::Err,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse region options: {}", reason))]
    ParseRegionOptions {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mito read operation fails"))]
    MitoReadOperation {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to encode primary key"))]
    EncodePrimaryKey {
        source: mito2::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mito write operation fails"))]
    MitoWriteOperation {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mito flush operation fails"))]
    MitoFlushOperation {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mito delete operation fails"))]
    MitoDeleteOperation {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mito catchup operation fails"))]
    MitoCatchupOperation {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mito sync operation fails"))]
    MitoSyncOperation {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to collect record batch stream"))]
    CollectRecordBatchStream {
        source: common_recordbatch::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Internal column {} is reserved", column))]
    InternalColumnOccupied {
        column: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Required table option is missing"))]
    MissingRegionOption {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region options are conflicted"))]
    ConflictRegionOption {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Physical region {} not found", region_id))]
    PhysicalRegionNotFound {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Logical region {} not found", region_id))]
    LogicalRegionNotFound {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Column type mismatch. Expect {:?}, got {:?}", expect, actual))]
    ColumnTypeMismatch {
        expect: ConcreteDataType,
        actual: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Column {} not found in logical region {}", name, region_id))]
    ColumnNotFound {
        name: String,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Alter request to physical region is forbidden"))]
    ForbiddenPhysicalAlter {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid region metadata"))]
    InvalidMetadata {
        source: store_api::metadata::MetadataError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Physical region {} is busy, there are still some logical regions using it",
        region_id
    ))]
    PhysicalRegionBusy {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported region request: {}", request))]
    UnsupportedRegionRequest {
        request: Box<RegionRequest>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported alter kind: {}", kind))]
    UnsupportedAlterKind {
        kind: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Multiple field column found: {} and {}", previous, current))]
    MultipleFieldColumn {
        previous: String,
        current: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Adding field column {} to physical table", name))]
    AddingFieldColumn {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No field column found"))]
    NoFieldColumn {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to set SKIPPING index option"))]
    SetSkippingIndexOption {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected request: {}", reason))]
    UnexpectedRequest {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected metric manifest info, region: {}", region_id))]
    MetricManifestInfo {
        region_id: RegionId,
        #[snafu(implicit)]
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
            | ColumnTypeMismatch { .. }
            | PhysicalRegionBusy { .. }
            | MultipleFieldColumn { .. }
            | NoFieldColumn { .. }
            | AddingFieldColumn { .. }
            | ParseRegionOptions { .. }
            | UnexpectedRequest { .. }
            | UnsupportedAlterKind { .. } => StatusCode::InvalidArguments,

            ForbiddenPhysicalAlter { .. } | UnsupportedRegionRequest { .. } => {
                StatusCode::Unsupported
            }

            DeserializeColumnMetadata { .. }
            | SerializeColumnMetadata { .. }
            | DecodeColumnValue { .. }
            | ParseRegionId { .. }
            | InvalidMetadata { .. }
            | SetSkippingIndexOption { .. }
            | SerializeRegionManifestInfo { .. } => StatusCode::Unexpected,

            PhysicalRegionNotFound { .. } | LogicalRegionNotFound { .. } => {
                StatusCode::RegionNotFound
            }

            ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            CreateMitoRegion { source, .. }
            | OpenMitoRegion { source, .. }
            | CloseMitoRegion { source, .. }
            | MitoReadOperation { source, .. }
            | MitoWriteOperation { source, .. }
            | MitoCatchupOperation { source, .. }
            | MitoFlushOperation { source, .. }
            | MitoDeleteOperation { source, .. }
            | MitoSyncOperation { source, .. } => source.status_code(),

            EncodePrimaryKey { source, .. } => source.status_code(),

            CollectRecordBatchStream { source, .. } => source.status_code(),

            MetricManifestInfo { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
