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
use std::sync::Arc;

use common_datasource::compression::CompressionType;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use datatypes::arrow::error::ArrowError;
use datatypes::prelude::ConcreteDataType;
use prost::{DecodeError, EncodeError};
use snafu::{Location, Snafu};
use store_api::manifest::ManifestVersion;
use store_api::storage::RegionId;

use crate::worker::WorkerId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("OpenDAL operator failed. Location: {}, source: {}", location, source))]
    OpenDal {
        location: Location,
        source: object_store::Error,
    },

    #[snafu(display(
        "Fail to compress object by {}, path: {}, source: {}",
        compress_type,
        path,
        source
    ))]
    CompressObject {
        compress_type: CompressionType,
        path: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Fail to decompress object by {}, path: {}, source: {}",
        compress_type,
        path,
        source
    ))]
    DecompressObject {
        compress_type: CompressionType,
        path: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Failed to ser/de json object. Location: {}, source: {}",
        location,
        source
    ))]
    SerdeJson {
        location: Location,
        source: serde_json::Error,
    },

    #[snafu(display("Invalid scan index, start: {}, end: {}", start, end))]
    InvalidScanIndex {
        start: ManifestVersion,
        end: ManifestVersion,
        location: Location,
    },

    #[snafu(display("Invalid UTF-8 content. Location: {}, source: {}", location, source))]
    Utf8 {
        location: Location,
        source: std::str::Utf8Error,
    },

    #[snafu(display("Cannot find RegionMetadata. Location: {}", location))]
    RegionMetadataNotFound { location: Location },

    #[snafu(display("Failed to join handle, location: {}, source: {}", location, source))]
    Join {
        source: common_runtime::JoinError,
        location: Location,
    },

    #[snafu(display("Worker {} is stopped, location: {}", id, location))]
    WorkerStopped { id: WorkerId, location: Location },

    #[snafu(display("Failed to recv result, location: {}, source: {}", location, source))]
    Recv {
        source: tokio::sync::oneshot::error::RecvError,
        location: Location,
    },

    #[snafu(display("Invalid metadata, {}, location: {}", reason, location))]
    InvalidMeta { reason: String, location: Location },

    #[snafu(display("Invalid schema, source: {}, location: {}", source, location))]
    InvalidSchema {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Invalid region metadata, source: {}, location: {}", source, location))]
    InvalidMetadata {
        source: store_api::metadata::MetadataError,
        location: Location,
    },

    #[snafu(display("Region {} already exists, location: {}", region_id, location))]
    RegionExists {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display(
        "Failed to create RecordBatch from vectors, location: {}, source: {}",
        location,
        source
    ))]
    NewRecordBatch {
        location: Location,
        source: ArrowError,
    },

    #[snafu(display(
        "Failed to write to buffer, location: {}, source: {}",
        location,
        source
    ))]
    WriteBuffer {
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display(
        "Failed to write parquet file, path: {}, location: {}, source: {}",
        path,
        location,
        source
    ))]
    WriteParquet {
        path: String,
        location: Location,
        source: parquet::errors::ParquetError,
    },

    #[snafu(display(
        "Failed to read parquet file, path: {}, location: {}, source: {}",
        path,
        location,
        source
    ))]
    ReadParquet {
        path: String,
        source: parquet::errors::ParquetError,
        location: Location,
    },

    #[snafu(display("Region {} not found, location: {}", region_id, location))]
    RegionNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display(
        "Region {} is corrupted, reason: {}, location: {}",
        region_id,
        reason,
        location
    ))]
    RegionCorrupted {
        region_id: RegionId,
        reason: String,
        location: Location,
    },

    #[snafu(display("Invalid request to region {}, reason: {}", region_id, reason))]
    InvalidRequest {
        region_id: RegionId,
        reason: String,
        location: Location,
    },

    /// An error type to indicate that schema is changed and we need
    /// to fill default values again.
    #[snafu(display("Need to fill default value for region {}", region_id))]
    FillDefault {
        region_id: RegionId,
        // The error is for internal use so we don't need a location.
    },

    #[snafu(display(
        "Failed to create default value for column {} of region {}",
        column,
        region_id
    ))]
    CreateDefault {
        region_id: RegionId,
        column: String,
        source: datatypes::Error,
    },

    #[snafu(display(
        "Failed to encode WAL entry, region_id: {}, location: {}, source: {}",
        region_id,
        location,
        source
    ))]
    EncodeWal {
        region_id: RegionId,
        location: Location,
        source: EncodeError,
    },

    #[snafu(display("Failed to write WAL, location: {}, source: {}", location, source))]
    WriteWal {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display(
        "Failed to read WAL, region_id: {}, location: {}, source: {}",
        region_id,
        location,
        source
    ))]
    ReadWal {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display(
        "Failed to decode WAL entry, region_id: {}, location: {}, source: {}",
        region_id,
        location,
        source
    ))]
    DecodeWal {
        region_id: RegionId,
        location: Location,
        source: DecodeError,
    },

    #[snafu(display(
        "Failed to delete WAL, region_id: {}, location: {}, source: {}",
        region_id,
        location,
        source
    ))]
    DeleteWal {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    // Shared error for each writer in the write group.
    #[snafu(display("Failed to write region, source: {}", source))]
    WriteGroup { source: Arc<Error> },

    #[snafu(display(
        "Row length mismatch, expect: {}, actual: {}, location: {}",
        expect,
        actual,
        location
    ))]
    RowLengthMismatch {
        expect: usize,
        actual: usize,
        location: Location,
    },

    #[snafu(display("Row value mismatches field data type"))]
    FieldTypeMismatch { source: datatypes::error::Error },

    #[snafu(display("Failed to serialize field, location: {}", location))]
    SerializeField {
        source: memcomparable::Error,
        location: Location,
    },

    #[snafu(display(
        "Data type: {} does not support serialization/deserialization, location: {}",
        data_type,
        location
    ))]
    NotSupportedField {
        data_type: ConcreteDataType,
        location: Location,
    },

    #[snafu(display(
        "Failed to deserialize field, source: {}, location: {}",
        source,
        location
    ))]
    DeserializeField {
        source: memcomparable::Error,
        location: Location,
    },

    #[snafu(display(
        "Invalid parquet SST file {}, location: {}, reason: {}",
        file,
        location,
        reason
    ))]
    InvalidParquet {
        file: String,
        reason: String,
        location: Location,
    },

    #[snafu(display("Invalid batch, {}, location: {}", reason, location))]
    InvalidBatch { reason: String, location: Location },

    #[snafu(display("Invalid arrow record batch, {}, location: {}", reason, location))]
    InvalidRecordBatch { reason: String, location: Location },

    #[snafu(display(
        "Failed to convert array to vector, location: {}, source: {}",
        location,
        source
    ))]
    ConvertVector {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Failed to compute arrow arrays, location: {}, source: {}",
        location,
        source
    ))]
    ComputeArrow {
        location: Location,
        source: datatypes::arrow::error::ArrowError,
    },

    #[snafu(display("Failed to compute vector, location: {}, source: {}", location, source))]
    ComputeVector {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Primary key length mismatch, expect: {}, actual: {}, location: {}",
        expect,
        actual,
        location
    ))]
    PrimaryKeyLengthMismatch {
        expect: usize,
        actual: usize,
        location: Location,
    },

    #[snafu(display("Failed to sort values source: {}, location: {}", source, location))]
    SortValues {
        source: ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to compact values, source: {}, location: {}", source, location))]
    CompactValues {
        source: datatypes::error::Error,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Returns true if we need to fill default value for a region.
    pub(crate) fn is_fill_default(&self) -> bool {
        matches!(self, Error::FillDefault { .. })
    }
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            OpenDal { .. }
            | WriteParquet { .. }
            | ReadParquet { .. }
            | WriteWal { .. }
            | ReadWal { .. }
            | DeleteWal { .. } => StatusCode::StorageUnavailable,
            CompressObject { .. }
            | DecompressObject { .. }
            | SerdeJson { .. }
            | Utf8 { .. }
            | RegionExists { .. }
            | NewRecordBatch { .. }
            | RegionNotFound { .. }
            | RegionCorrupted { .. }
            | CreateDefault { .. }
            | InvalidParquet { .. } => StatusCode::Unexpected,
            InvalidScanIndex { .. }
            | InvalidMeta { .. }
            | InvalidSchema { .. }
            | InvalidRequest { .. }
            | FillDefault { .. }
            | InvalidMetadata { .. } => StatusCode::InvalidArguments,
            RegionMetadataNotFound { .. }
            | Join { .. }
            | WorkerStopped { .. }
            | Recv { .. }
            | EncodeWal { .. }
            | DecodeWal { .. } => StatusCode::Internal,
            WriteBuffer { source, .. } => source.status_code(),
            WriteGroup { source, .. } => source.status_code(),
            RowLengthMismatch { .. } => StatusCode::InvalidArguments,
            FieldTypeMismatch { source, .. } => source.status_code(),
            SerializeField { .. } => StatusCode::Internal,
            NotSupportedField { .. } => StatusCode::Unsupported,
            DeserializeField { .. } => StatusCode::Unexpected,
            InvalidBatch { .. } => StatusCode::InvalidArguments,
            InvalidRecordBatch { .. } => StatusCode::InvalidArguments,
            ConvertVector { source, .. } => source.status_code(),
            ComputeArrow { .. } => StatusCode::Internal,
            ComputeVector { .. } => StatusCode::Internal,
            PrimaryKeyLengthMismatch { .. } => StatusCode::InvalidArguments,
            SortValues { .. } => StatusCode::Unexpected,
            CompactValues { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
