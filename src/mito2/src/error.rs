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
use common_macro::stack_trace_debug;
use common_runtime::JoinError;
use datatypes::arrow::error::ArrowError;
use datatypes::prelude::ConcreteDataType;
use object_store::ErrorKind;
use prost::{DecodeError, EncodeError};
use snafu::{Location, Snafu};
use store_api::manifest::ManifestVersion;
use store_api::storage::RegionId;

use crate::sst::file::FileId;
use crate::worker::WorkerId;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display(
        "Failed to set region {} to writable, it was expected to replayed to {}, but actually replayed to {}",
        region_id, expected_last_entry_id, replayed_last_entry_id
    ))]
    UnexpectedReplay {
        location: Location,
        region_id: RegionId,
        expected_last_entry_id: u64,
        replayed_last_entry_id: u64,
    },

    #[snafu(display("OpenDAL operator failed"))]
    OpenDal {
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Fail to compress object by {}, path: {}", compress_type, path))]
    CompressObject {
        compress_type: CompressionType,
        path: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Fail to decompress object by {}, path: {}", compress_type, path))]
    DecompressObject {
        compress_type: CompressionType,
        path: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to ser/de json object"))]
    SerdeJson {
        location: Location,
        #[snafu(source)]
        error: serde_json::Error,
    },

    #[snafu(display("Invalid scan index, start: {}, end: {}", start, end))]
    InvalidScanIndex {
        start: ManifestVersion,
        end: ManifestVersion,
        location: Location,
    },

    #[snafu(display("Invalid UTF-8 content"))]
    Utf8 {
        location: Location,
        #[snafu(source)]
        error: std::str::Utf8Error,
    },

    #[snafu(display("Cannot find RegionMetadata"))]
    RegionMetadataNotFound { location: Location },

    #[snafu(display("Failed to join handle"))]
    Join {
        #[snafu(source)]
        error: common_runtime::JoinError,
        location: Location,
    },

    #[snafu(display("Worker {} is stopped", id))]
    WorkerStopped { id: WorkerId, location: Location },

    #[snafu(display("Failed to recv result"))]
    Recv {
        #[snafu(source)]
        error: tokio::sync::oneshot::error::RecvError,
        location: Location,
    },

    #[snafu(display("Invalid metadata, {}", reason))]
    InvalidMeta { reason: String, location: Location },

    #[snafu(display("Invalid region metadata"))]
    InvalidMetadata {
        source: store_api::metadata::MetadataError,
        location: Location,
    },

    #[snafu(display("Failed to create RecordBatch from vectors"))]
    NewRecordBatch {
        location: Location,
        #[snafu(source)]
        error: ArrowError,
    },

    #[snafu(display("Failed to write to buffer"))]
    WriteBuffer {
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to read parquet file, path: {}", path))]
    ReadParquet {
        path: String,
        #[snafu(source)]
        error: parquet::errors::ParquetError,
        location: Location,
    },

    #[snafu(display("Region {} not found", region_id))]
    RegionNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Object store not found: {}", object_store))]
    ObjectStoreNotFound {
        object_store: String,
        location: Location,
    },

    #[snafu(display("Region {} is corrupted, reason: {}", region_id, reason))]
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

    #[snafu(display(
        "Failed to convert ConcreteDataType to ColumnDataType, reason: {}",
        reason
    ))]
    ConvertColumnDataType {
        reason: String,
        source: api::error::Error,
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

    #[snafu(display("Failed to encode WAL entry, region_id: {}", region_id))]
    EncodeWal {
        region_id: RegionId,
        location: Location,
        #[snafu(source)]
        error: EncodeError,
    },

    #[snafu(display("Failed to write WAL"))]
    WriteWal {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to read WAL, region_id: {}", region_id))]
    ReadWal {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to decode WAL entry, region_id: {}", region_id))]
    DecodeWal {
        region_id: RegionId,
        location: Location,
        #[snafu(source)]
        error: DecodeError,
    },

    #[snafu(display("Failed to delete WAL, region_id: {}", region_id))]
    DeleteWal {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    // Shared error for each writer in the write group.
    #[snafu(display("Failed to write region"))]
    WriteGroup { source: Arc<Error> },

    #[snafu(display("Row value mismatches field data type"))]
    FieldTypeMismatch { source: datatypes::error::Error },

    #[snafu(display("Failed to serialize field"))]
    SerializeField {
        #[snafu(source)]
        error: memcomparable::Error,
        location: Location,
    },

    #[snafu(display(
        "Data type: {} does not support serialization/deserialization",
        data_type,
    ))]
    NotSupportedField {
        data_type: ConcreteDataType,
        location: Location,
    },

    #[snafu(display("Failed to deserialize field"))]
    DeserializeField {
        #[snafu(source)]
        error: memcomparable::Error,
        location: Location,
    },

    #[snafu(display("Invalid parquet SST file {}, reason: {}", file, reason))]
    InvalidParquet {
        file: String,
        reason: String,
        location: Location,
    },

    #[snafu(display("Invalid batch, {}", reason))]
    InvalidBatch { reason: String, location: Location },

    #[snafu(display("Invalid arrow record batch, {}", reason))]
    InvalidRecordBatch { reason: String, location: Location },

    #[snafu(display("Failed to convert array to vector"))]
    ConvertVector {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to compute arrow arrays"))]
    ComputeArrow {
        location: Location,
        #[snafu(source)]
        error: datatypes::arrow::error::ArrowError,
    },

    #[snafu(display("Failed to compute vector"))]
    ComputeVector {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Primary key length mismatch, expect: {}, actual: {}", expect, actual))]
    PrimaryKeyLengthMismatch {
        expect: usize,
        actual: usize,
        location: Location,
    },

    #[snafu(display("Invalid sender",))]
    InvalidSender { location: Location },

    #[snafu(display("Invalid scheduler state"))]
    InvalidSchedulerState { location: Location },

    #[snafu(display("Failed to stop scheduler"))]
    StopScheduler {
        #[snafu(source)]
        error: JoinError,
        location: Location,
    },

    #[snafu(display("Failed to delete SST file, file id: {}", file_id))]
    DeleteSst {
        file_id: FileId,
        #[snafu(source)]
        error: object_store::Error,
        location: Location,
    },

    #[snafu(display("Failed to flush region {}", region_id))]
    FlushRegion {
        region_id: RegionId,
        source: Arc<Error>,
        location: Location,
    },

    #[snafu(display("Region {} is dropped", region_id))]
    RegionDropped {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Region {} is closed", region_id))]
    RegionClosed {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Region {} is truncated", region_id))]
    RegionTruncated {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display(
        "Engine write buffer is full, rejecting write requests of region {}",
        region_id,
    ))]
    RejectWrite {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Failed to compact region {}", region_id))]
    CompactRegion {
        region_id: RegionId,
        source: Arc<Error>,
        location: Location,
    },

    #[snafu(display(
        "Failed to compat readers for region {}, reason: {}",
        region_id,
        reason,
    ))]
    CompatReader {
        region_id: RegionId,
        reason: String,
        location: Location,
    },

    #[snafu(display("Invalue region req"))]
    InvalidRegionRequest {
        source: store_api::metadata::MetadataError,
        location: Location,
    },

    #[snafu(display("Region {} is read only", region_id))]
    RegionReadonly {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Invalid options"))]
    JsonOptions {
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },

    #[snafu(display(
        "Empty region directory, region_id: {}, region_dir: {}",
        region_id,
        region_dir,
    ))]
    EmptyRegionDir {
        region_id: RegionId,
        region_dir: String,
        location: Location,
    },

    #[snafu(display("Empty manifest directory, manifest_dir: {}", manifest_dir,))]
    EmptyManifestDir {
        manifest_dir: String,
        location: Location,
    },

    #[snafu(display("Failed to read arrow record batch from parquet file {}", path))]
    ArrowReader {
        path: String,
        #[snafu(source)]
        error: ArrowError,
        location: Location,
    },

    #[snafu(display("Invalid file metadata"))]
    ConvertMetaData {
        location: Location,
        #[snafu(source)]
        error: parquet::errors::ParquetError,
    },

    #[snafu(display("Column not found, column: {column}"))]
    ColumnNotFound { column: String, location: Location },

    #[snafu(display("Failed to build index applier"))]
    BuildIndexApplier {
        #[snafu(source)]
        source: index::inverted_index::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to convert value"))]
    ConvertValue {
        #[snafu(source)]
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to apply index"))]
    ApplyIndex {
        #[snafu(source)]
        source: index::inverted_index::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to read puffin metadata"))]
    PuffinReadMetadata {
        #[snafu(source)]
        source: puffin::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to read puffin blob"))]
    PuffinReadBlob {
        #[snafu(source)]
        source: puffin::error::Error,
        location: Location,
    },

    #[snafu(display("Blob type not found, blob_type: {blob_type}"))]
    PuffinBlobTypeNotFound {
        blob_type: String,
        location: Location,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Returns true if we need to fill default value for a region.
    pub(crate) fn is_fill_default(&self) -> bool {
        matches!(self, Error::FillDefault { .. })
    }

    /// Returns true if the file is not found on the object store.
    pub(crate) fn is_object_not_found(&self) -> bool {
        match self {
            Error::OpenDal { error, .. } => error.kind() == ErrorKind::NotFound,
            _ => false,
        }
    }
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            OpenDal { .. }
            | ReadParquet { .. }
            | WriteWal { .. }
            | ReadWal { .. }
            | DeleteWal { .. } => StatusCode::StorageUnavailable,
            CompressObject { .. }
            | DecompressObject { .. }
            | SerdeJson { .. }
            | Utf8 { .. }
            | NewRecordBatch { .. }
            | RegionCorrupted { .. }
            | CreateDefault { .. }
            | InvalidParquet { .. }
            | PuffinBlobTypeNotFound { .. }
            | UnexpectedReplay { .. } => StatusCode::Unexpected,
            RegionNotFound { .. } => StatusCode::RegionNotFound,
            ObjectStoreNotFound { .. }
            | InvalidScanIndex { .. }
            | InvalidMeta { .. }
            | InvalidRequest { .. }
            | FillDefault { .. }
            | ConvertColumnDataType { .. }
            | ColumnNotFound { .. }
            | InvalidMetadata { .. } => StatusCode::InvalidArguments,
            RegionMetadataNotFound { .. }
            | Join { .. }
            | WorkerStopped { .. }
            | Recv { .. }
            | EncodeWal { .. }
            | DecodeWal { .. } => StatusCode::Internal,
            WriteBuffer { source, .. } => source.status_code(),
            WriteGroup { source, .. } => source.status_code(),
            FieldTypeMismatch { source, .. } => source.status_code(),
            SerializeField { .. } => StatusCode::Internal,
            NotSupportedField { .. } => StatusCode::Unsupported,
            DeserializeField { .. } => StatusCode::Unexpected,
            InvalidBatch { .. } => StatusCode::InvalidArguments,
            InvalidRecordBatch { .. } => StatusCode::InvalidArguments,
            ConvertVector { source, .. } => source.status_code(),
            ConvertMetaData { .. } => StatusCode::Internal,
            ComputeArrow { .. } => StatusCode::Internal,
            ComputeVector { .. } => StatusCode::Internal,
            PrimaryKeyLengthMismatch { .. } => StatusCode::InvalidArguments,
            InvalidSender { .. } => StatusCode::InvalidArguments,
            InvalidSchedulerState { .. } => StatusCode::InvalidArguments,
            StopScheduler { .. } => StatusCode::Internal,
            DeleteSst { .. } => StatusCode::StorageUnavailable,
            FlushRegion { source, .. } => source.status_code(),
            RegionDropped { .. } => StatusCode::Cancelled,
            RegionClosed { .. } => StatusCode::Cancelled,
            RegionTruncated { .. } => StatusCode::Cancelled,
            RejectWrite { .. } => StatusCode::StorageUnavailable,
            CompactRegion { source, .. } => source.status_code(),
            CompatReader { .. } => StatusCode::Unexpected,
            InvalidRegionRequest { source, .. } => source.status_code(),
            RegionReadonly { .. } => StatusCode::RegionReadonly,
            JsonOptions { .. } => StatusCode::InvalidArguments,
            EmptyRegionDir { .. } | EmptyManifestDir { .. } => StatusCode::RegionNotFound,
            ArrowReader { .. } => StatusCode::StorageUnavailable,
            ConvertValue { source, .. } => source.status_code(),
            BuildIndexApplier { source, .. } | ApplyIndex { source, .. } => source.status_code(),
            PuffinReadMetadata { source, .. } | PuffinReadBlob { source, .. } => {
                source.status_code()
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
