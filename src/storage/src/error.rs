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
use std::io::Error as IoError;
use std::str::Utf8Error;

use common_datasource::compression::CompressionType;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_runtime::error::Error as RuntimeError;
use datatypes::arrow::error::ArrowError;
use datatypes::prelude::ConcreteDataType;
use object_store::ErrorKind;
use serde_json::error::Error as JsonError;
use snafu::{Location, Snafu};
use store_api::manifest::action::ProtocolVersion;
use store_api::manifest::ManifestVersion;
use store_api::storage::{RegionId, SequenceNumber};
use tokio::task::JoinError;

use crate::metadata::Error as MetadataError;
use crate::write_batch;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Invalid region descriptor, region: {}", region))]
    InvalidRegionDesc {
        region: String,
        location: Location,
        source: MetadataError,
    },

    #[snafu(display("Missing column {} in write batch", column))]
    BatchMissingColumn { column: String, location: Location },

    #[snafu(display("Failed to write parquet file"))]
    WriteParquet {
        #[snafu(source)]
        error: parquet::errors::ParquetError,
        location: Location,
    },

    #[snafu(display("Failed to write to buffer"))]
    WriteBuffer {
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to create RecordBatch from vectors"))]
    NewRecordBatch {
        location: Location,
        #[snafu(source)]
        error: ArrowError,
    },

    #[snafu(display("Fail to read object from path: {}", path))]
    ReadObject {
        path: String,
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Fail to write object into path: {}", path))]
    WriteObject {
        path: String,
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Fail to delete object from path: {}", path))]
    DeleteObject {
        path: String,
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

    #[snafu(display("Fail to list objects in path: {}", path))]
    ListObjects {
        path: String,
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Fail to create str from bytes"))]
    Utf8 {
        location: Location,
        #[snafu(source)]
        error: Utf8Error,
    },

    #[snafu(display("Fail to encode object into json "))]
    EncodeJson {
        location: Location,
        #[snafu(source)]
        error: JsonError,
    },

    #[snafu(display("Fail to decode object from json "))]
    DecodeJson {
        location: Location,
        #[snafu(source)]
        error: JsonError,
    },

    #[snafu(display("Invalid scan index, start: {}, end: {}", start, end))]
    InvalidScanIndex {
        start: ManifestVersion,
        end: ManifestVersion,
        location: Location,
    },

    #[snafu(display("Failed to write WAL, WAL region_id: {}", region_id))]
    WriteWal {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to encode WAL header"))]
    EncodeWalHeader {
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to decode WAL header"))]
    DecodeWalHeader {
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to wait flushing, region_id: {}", region_id))]
    WaitFlush {
        region_id: RegionId,
        #[snafu(source)]
        error: tokio::sync::oneshot::error::RecvError,
        location: Location,
    },

    #[snafu(display(
        "Manifest protocol forbid to read, min_version: {}, supported_version: {}",
        min_version,
        supported_version
    ))]
    ManifestProtocolForbidRead {
        min_version: ProtocolVersion,
        supported_version: ProtocolVersion,
        location: Location,
    },

    #[snafu(display(
        "Manifest protocol forbid to write, min_version: {}, supported_version: {}",
        min_version,
        supported_version
    ))]
    ManifestProtocolForbidWrite {
        min_version: ProtocolVersion,
        supported_version: ProtocolVersion,
        location: Location,
    },

    #[snafu(display("Failed to decode action list, {}", msg))]
    DecodeMetaActionList { msg: String, location: Location },

    #[snafu(display("Failed to read line, err"))]
    Readline {
        #[snafu(source)]
        error: IoError,
    },

    #[snafu(display("Failed to read Parquet file: {}", file))]
    ReadParquet {
        file: String,
        #[snafu(source)]
        error: parquet::errors::ParquetError,
        location: Location,
    },

    #[snafu(display("Region is under {} state, cannot proceed operation", state))]
    InvalidRegionState {
        state: &'static str,
        location: Location,
    },

    #[snafu(display("Failed to read WAL, region_id: {}", region_id))]
    ReadWal {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to mark WAL as obsolete, region id: {}", region_id))]
    MarkWalObsolete {
        region_id: u64,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("WAL data corrupted, region_id: {}, message: {}", region_id, message))]
    WalDataCorrupted {
        region_id: RegionId,
        message: String,
        location: Location,
    },

    #[snafu(display("Failed to delete WAL namespace, region id: {}", region_id))]
    DeleteWalNamespace {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display(
        "Sequence of region should increase monotonically (should be {} < {})",
        prev,
        given
    ))]
    SequenceNotMonotonic {
        prev: SequenceNumber,
        given: SequenceNumber,
        location: Location,
    },

    #[snafu(display("Failed to convert store schema, file: {}", file))]
    ConvertStoreSchema {
        file: String,
        location: Location,
        source: MetadataError,
    },

    #[snafu(display("Invalid raw region metadata, region: {}", region))]
    InvalidRawRegion {
        region: String,
        location: Location,
        source: MetadataError,
    },

    #[snafu(display("Try to write the closed region"))]
    ClosedRegion { location: Location },

    #[snafu(display("Invalid projection"))]
    InvalidProjection {
        location: Location,
        source: MetadataError,
    },

    #[snafu(display("Failed to push data to batch builder"))]
    PushBatch {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to build batch, {}", msg))]
    BuildBatch { msg: String, location: Location },

    #[snafu(display("Failed to filter column {}", name))]
    FilterColumn {
        name: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid alter request"))]
    InvalidAlterRequest {
        location: Location,
        source: MetadataError,
    },

    #[snafu(display("Failed to alter metadata"))]
    AlterMetadata {
        location: Location,
        source: MetadataError,
    },

    #[snafu(display("Failed to create default value for column {}", name))]
    CreateDefault {
        name: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Not allowed to write data with version {} to schema with version {}",
        data_version,
        schema_version
    ))]
    WriteToOldVersion {
        /// Schema version of data to write.
        data_version: u32,
        schema_version: u32,
        location: Location,
    },

    #[snafu(display("Column {} not in schema with version {}", column, version))]
    NotInSchemaToCompat {
        column: String,
        version: u32,
        location: Location,
    },

    #[snafu(display("Incompatible schema to read, reason: {}", reason))]
    CompatRead { reason: String, location: Location },

    #[snafu(display("Failed to read column {}, could not create default value", column))]
    CreateDefaultToRead {
        column: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to read column {}, no proper default value for it", column))]
    NoDefaultToRead { column: String, location: Location },

    #[snafu(display("Failed to convert arrow chunk to batch, name: {}", name))]
    ConvertChunk {
        name: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Unknown column {}", name))]
    UnknownColumn { name: String, location: Location },

    #[snafu(display("Failed to create record batch for write batch"))]
    CreateRecordBatch {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display(
        "Request is too large, max is {}, current is {}",
        write_batch::MAX_BATCH_SIZE,
        num_rows
    ))]
    RequestTooLarge { num_rows: usize, location: Location },

    #[snafu(display(
        "Type of column {} does not match type in schema, expect {:?}, given {:?}",
        name,
        expect,
        given
    ))]
    TypeMismatch {
        name: String,
        expect: ConcreteDataType,
        given: ConcreteDataType,
        location: Location,
    },

    #[snafu(display("Column {} is not null but input has null", name))]
    HasNull { name: String, location: Location },

    #[snafu(display(
        "Length of column {} not equals to other columns, expect {}, given {}",
        name,
        expect,
        given
    ))]
    UnequalLengths {
        name: String,
        expect: usize,
        given: usize,
        location: Location,
    },

    #[snafu(display("Failed to decode write batch, corrupted data {}", message))]
    BatchCorrupted { message: String, location: Location },

    #[snafu(display("Failed to decode arrow data"))]
    DecodeArrow {
        location: Location,
        #[snafu(source)]
        error: ArrowError,
    },

    #[snafu(display("Failed to encode arrow data"))]
    EncodeArrow {
        location: Location,
        #[snafu(source)]
        error: ArrowError,
    },

    #[snafu(display("Failed to parse schema"))]
    ParseSchema {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("More columns than expected in the request"))]
    MoreColumnThanExpected { location: Location },

    #[snafu(display("Failed to decode parquet file time range, msg: {}", msg))]
    DecodeParquetTimeRange { msg: String, location: Location },

    #[snafu(display("Scheduler rate limited, msg: {}", msg))]
    RateLimited { msg: String },

    #[snafu(display("Cannot schedule request, scheduler's already stopped"))]
    IllegalSchedulerState { location: Location },

    #[snafu(display("Failed to start manifest gc task"))]
    StartManifestGcTask {
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to stop manifest gc task"))]
    StopManifestGcTask {
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to stop scheduler"))]
    StopScheduler {
        #[snafu(source)]
        error: JoinError,
        location: Location,
    },

    #[snafu(display("Failed to delete SST file"))]
    DeleteSst {
        #[snafu(source)]
        error: object_store::Error,
        location: Location,
    },

    #[snafu(display("Failed to calculate SST expire time"))]
    TtlCalculation {
        location: Location,
        source: common_time::error::Error,
    },

    #[snafu(display("Failed to create a checkpoint: {}", msg))]
    ManifestCheckpoint { msg: String, location: Location },

    #[snafu(display("The compaction task is cancelled, region_id: {}", region_id))]
    CompactTaskCancel {
        region_id: RegionId,
        #[snafu(source)]
        error: tokio::sync::oneshot::error::RecvError,
    },

    #[snafu(display(
        "The flush request is duplicate, region_id: {}, sequence: {}",
        region_id,
        sequence
    ))]
    DuplicateFlush {
        region_id: RegionId,
        sequence: SequenceNumber,
        location: Location,
    },

    #[snafu(display("Failed to start picking task for flush"))]
    StartPickTask {
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to stop picking task for flush"))]
    StopPickTask {
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to convert columns to rows"))]
    ConvertColumnsToRows {
        #[snafu(source)]
        error: ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to sort arrays"))]
    SortArrays {
        #[snafu(source)]
        error: ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to build scan predicate"))]
    BuildPredicate {
        source: table::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to join spawned tasks"))]
    JoinError {
        #[snafu(source)]
        error: JoinError,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Returns true if the error is the object path to delete
    /// doesn't exist.
    pub(crate) fn is_object_to_delete_not_found(&self) -> bool {
        if let Error::DeleteObject { error, .. } = self {
            error.kind() == ErrorKind::NotFound
        } else {
            false
        }
    }
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            InvalidScanIndex { .. }
            | BatchMissingColumn { .. }
            | InvalidProjection { .. }
            | BuildBatch { .. }
            | NotInSchemaToCompat { .. }
            | WriteToOldVersion { .. }
            | CreateRecordBatch { .. }
            | RequestTooLarge { .. }
            | TypeMismatch { .. }
            | HasNull { .. }
            | UnequalLengths { .. }
            | MoreColumnThanExpected { .. } => StatusCode::InvalidArguments,

            Utf8 { .. }
            | EncodeJson { .. }
            | DecodeJson { .. }
            | WaitFlush { .. }
            | DecodeMetaActionList { .. }
            | Readline { .. }
            | WalDataCorrupted { .. }
            | SequenceNotMonotonic { .. }
            | ConvertStoreSchema { .. }
            | InvalidRawRegion { .. }
            | ClosedRegion { .. }
            | FilterColumn { .. }
            | AlterMetadata { .. }
            | CompatRead { .. }
            | CreateDefaultToRead { .. }
            | NoDefaultToRead { .. }
            | NewRecordBatch { .. }
            | BatchCorrupted { .. }
            | DecodeArrow { .. }
            | EncodeArrow { .. }
            | ManifestCheckpoint { .. }
            | CompressObject { .. }
            | DecompressObject { .. }
            | ParseSchema { .. } => StatusCode::Unexpected,

            WriteParquet { .. }
            | ReadObject { .. }
            | WriteObject { .. }
            | ListObjects { .. }
            | DeleteObject { .. }
            | WriteWal { .. }
            | DecodeWalHeader { .. }
            | EncodeWalHeader { .. }
            | ManifestProtocolForbidRead { .. }
            | ManifestProtocolForbidWrite { .. }
            | ReadParquet { .. }
            | InvalidRegionState { .. }
            | ReadWal { .. } => StatusCode::StorageUnavailable,

            UnknownColumn { .. } => StatusCode::TableColumnNotFound,

            InvalidAlterRequest { source, .. } | InvalidRegionDesc { source, .. } => {
                source.status_code()
            }
            WriteBuffer { source, .. } => source.status_code(),
            PushBatch { source, .. } => source.status_code(),
            CreateDefault { source, .. } => source.status_code(),
            ConvertChunk { source, .. } => source.status_code(),
            MarkWalObsolete { source, .. } => source.status_code(),
            DeleteWalNamespace { source, .. } => source.status_code(),
            DecodeParquetTimeRange { .. } => StatusCode::Unexpected,
            RateLimited { .. } | StopScheduler { .. } | CompactTaskCancel { .. } => {
                StatusCode::Internal
            }
            DeleteSst { .. } => StatusCode::StorageUnavailable,

            StartManifestGcTask { .. }
            | StopManifestGcTask { .. }
            | IllegalSchedulerState { .. }
            | DuplicateFlush { .. }
            | StartPickTask { .. }
            | StopPickTask { .. } => StatusCode::Unexpected,

            TtlCalculation { source, .. } => source.status_code(),
            ConvertColumnsToRows { .. } | SortArrays { .. } => StatusCode::Unexpected,
            BuildPredicate { source, .. } => source.status_code(),
            JoinError { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
