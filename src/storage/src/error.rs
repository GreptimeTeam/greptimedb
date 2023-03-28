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

use common_error::prelude::*;
use common_runtime::error::Error as RuntimeError;
use datatypes::arrow::error::ArrowError;
use datatypes::prelude::ConcreteDataType;
use serde_json::error::Error as JsonError;
use store_api::manifest::action::ProtocolVersion;
use store_api::manifest::ManifestVersion;
use store_api::storage::{RegionId, SequenceNumber};
use tokio::task::JoinError;

use crate::metadata::Error as MetadataError;
use crate::write_batch;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid region descriptor, region: {}, source: {}", region, source))]
    InvalidRegionDesc {
        region: String,
        #[snafu(backtrace)]
        source: MetadataError,
    },

    #[snafu(display("Missing column {} in write batch", column))]
    BatchMissingColumn {
        column: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to write parquet file, source: {}", source))]
    WriteParquet {
        source: parquet::errors::ParquetError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to create RecordBatch from vectors, source: {}", source))]
    NewRecordBatch {
        backtrace: Backtrace,
        source: ArrowError,
    },

    #[snafu(display("Fail to read object from path: {}, source: {}", path, source))]
    ReadObject {
        path: String,
        backtrace: Backtrace,
        source: object_store::Error,
    },

    #[snafu(display("Fail to write object into path: {}, source: {}", path, source))]
    WriteObject {
        path: String,
        backtrace: Backtrace,
        source: object_store::Error,
    },

    #[snafu(display("Fail to delete object from path: {}, source: {}", path, source))]
    DeleteObject {
        path: String,
        backtrace: Backtrace,
        source: object_store::Error,
    },

    #[snafu(display("Fail to list objects in path: {}, source: {}", path, source))]
    ListObjects {
        path: String,
        backtrace: Backtrace,
        source: object_store::Error,
    },

    #[snafu(display("Fail to create str from bytes, source: {}", source))]
    Utf8 {
        backtrace: Backtrace,
        source: Utf8Error,
    },

    #[snafu(display("Fail to encode object into json , source: {}", source))]
    EncodeJson {
        backtrace: Backtrace,
        source: JsonError,
    },

    #[snafu(display("Fail to decode object from json , source: {}", source))]
    DecodeJson {
        backtrace: Backtrace,
        source: JsonError,
    },

    #[snafu(display("Invalid scan index, start: {}, end: {}", start, end))]
    InvalidScanIndex {
        start: ManifestVersion,
        end: ManifestVersion,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to write WAL, WAL region_id: {}, source: {}",
        region_id,
        source
    ))]
    WriteWal {
        region_id: RegionId,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to encode WAL header, source {}", source))]
    EncodeWalHeader {
        backtrace: Backtrace,
        source: std::io::Error,
    },

    #[snafu(display("Failed to decode WAL header, source {}", source))]
    DecodeWalHeader {
        backtrace: Backtrace,
        source: std::io::Error,
    },

    #[snafu(display("Failed to join task, source: {}", source))]
    JoinTask {
        source: common_runtime::JoinError,
        backtrace: Backtrace,
    },

    #[snafu(display("Task already cancelled"))]
    Cancelled { backtrace: Backtrace },

    #[snafu(display("Failed to cancel flush, source: {}", source))]
    CancelFlush {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display(
        "Manifest protocol forbid to read, min_version: {}, supported_version: {}",
        min_version,
        supported_version
    ))]
    ManifestProtocolForbidRead {
        min_version: ProtocolVersion,
        supported_version: ProtocolVersion,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Manifest protocol forbid to write, min_version: {}, supported_version: {}",
        min_version,
        supported_version
    ))]
    ManifestProtocolForbidWrite {
        min_version: ProtocolVersion,
        supported_version: ProtocolVersion,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode action list, {}", msg))]
    DecodeMetaActionList { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to read line, err: {}", source))]
    Readline { source: IoError },

    #[snafu(display("Failed to read Parquet file: {}, source: {}", file, source))]
    ReadParquet {
        file: String,
        source: parquet::errors::ParquetError,
        backtrace: Backtrace,
    },

    #[snafu(display("Region is under {} state, cannot proceed operation", state))]
    InvalidRegionState {
        state: &'static str,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to read WAL, region_id: {}, source: {}", region_id, source))]
    ReadWal {
        region_id: RegionId,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display(
        "Failed to mark WAL as obsolete, region id: {}, source: {}",
        region_id,
        source
    ))]
    MarkWalObsolete {
        region_id: u64,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("WAL data corrupted, region_id: {}, message: {}", region_id, message))]
    WalDataCorrupted {
        region_id: RegionId,
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Sequence of region should increase monotonically (should be {} < {})",
        prev,
        given
    ))]
    SequenceNotMonotonic {
        prev: SequenceNumber,
        given: SequenceNumber,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert store schema, file: {}, source: {}", file, source))]
    ConvertStoreSchema {
        file: String,
        #[snafu(backtrace)]
        source: MetadataError,
    },

    #[snafu(display("Invalid raw region metadata, region: {}, source: {}", region, source))]
    InvalidRawRegion {
        region: String,
        #[snafu(backtrace)]
        source: MetadataError,
    },

    #[snafu(display("Try to write the closed region"))]
    ClosedRegion { backtrace: Backtrace },

    #[snafu(display("Invalid projection, source: {}", source))]
    InvalidProjection {
        #[snafu(backtrace)]
        source: MetadataError,
    },

    #[snafu(display("Failed to push data to batch builder, source: {}", source))]
    PushBatch {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to build batch, {}", msg))]
    BuildBatch { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to filter column {}, source: {}", name, source))]
    FilterColumn {
        name: String,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid alter request, source: {}", source))]
    InvalidAlterRequest {
        #[snafu(backtrace)]
        source: MetadataError,
    },

    #[snafu(display("Failed to alter metadata, source: {}", source))]
    AlterMetadata {
        #[snafu(backtrace)]
        source: MetadataError,
    },

    #[snafu(display(
        "Failed to create default value for column {}, source: {}",
        name,
        source
    ))]
    CreateDefault {
        name: String,
        #[snafu(backtrace)]
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
        backtrace: Backtrace,
    },

    #[snafu(display("Column {} not in schema with version {}", column, version))]
    NotInSchemaToCompat {
        column: String,
        version: u32,
        backtrace: Backtrace,
    },

    #[snafu(display("Incompatible schema to read, reason: {}", reason))]
    CompatRead {
        reason: String,
        backtrace: Backtrace,
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

    #[snafu(display("Failed to read column {}, no proper default value for it", column))]
    NoDefaultToRead {
        column: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to convert arrow chunk to batch, name: {}, source: {}",
        name,
        source
    ))]
    ConvertChunk {
        name: String,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Unknown column {}", name))]
    UnknownColumn { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to create record batch for write batch, source:{}", source))]
    CreateRecordBatch {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display(
        "Request is too large, max is {}, current is {}",
        write_batch::MAX_BATCH_SIZE,
        num_rows
    ))]
    RequestTooLarge {
        num_rows: usize,
        backtrace: Backtrace,
    },

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
        backtrace: Backtrace,
    },

    #[snafu(display("Column {} is not null but input has null", name))]
    HasNull { name: String, backtrace: Backtrace },

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
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode write batch, corrupted data {}", message))]
    BatchCorrupted {
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode arrow data, source: {}", source))]
    DecodeArrow {
        backtrace: Backtrace,
        source: ArrowError,
    },

    #[snafu(display("Failed to encode arrow data, source: {}", source))]
    EncodeArrow {
        backtrace: Backtrace,
        source: ArrowError,
    },

    #[snafu(display("Failed to parse schema, source: {}", source))]
    ParseSchema {
        backtrace: Backtrace,
        source: datatypes::error::Error,
    },

    #[snafu(display("More columns than expected in the request"))]
    MoreColumnThanExpected { backtrace: Backtrace },

    #[snafu(display("Failed to decode parquet file time range, msg: {}", msg))]
    DecodeParquetTimeRange { msg: String, backtrace: Backtrace },

    #[snafu(display("Scheduler rate limited, msg: {}", msg))]
    RateLimited { msg: String, backtrace: Backtrace },

    #[snafu(display("Cannot schedule request, scheduler's already stopped"))]
    IllegalSchedulerState { backtrace: Backtrace },

    #[snafu(display("Failed to start manifest gc task"))]
    StartManifestGcTask {
        #[snafu(backtrace)]
        source: RuntimeError,
    },

    #[snafu(display("Failed to stop manifest gc task"))]
    StopManifestGcTask {
        #[snafu(backtrace)]
        source: RuntimeError,
    },

    #[snafu(display("Failed to stop scheduler, source: {}", source))]
    StopScheduler {
        source: JoinError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to delete SST file, source: {}", source))]
    DeleteSst {
        source: object_store::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to calculate SST expire time, source: {}", source))]
    TtlCalculation {
        #[snafu(backtrace)]
        source: common_time::error::Error,
    },

    #[snafu(display("Failed to create a checkpoint: {}", msg))]
    ManifestCheckpoint { msg: String, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

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
            | JoinTask { .. }
            | Cancelled { .. }
            | CancelFlush { .. }
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
            PushBatch { source, .. } => source.status_code(),
            CreateDefault { source, .. } => source.status_code(),
            ConvertChunk { source, .. } => source.status_code(),
            MarkWalObsolete { source, .. } => source.status_code(),
            DecodeParquetTimeRange { .. } => StatusCode::Unexpected,
            RateLimited { .. } => StatusCode::Internal,
            StopScheduler { .. } => StatusCode::Internal,
            DeleteSst { .. } => StatusCode::StorageUnavailable,

            StartManifestGcTask { .. }
            | StopManifestGcTask { .. }
            | IllegalSchedulerState { .. } => StatusCode::Unexpected,

            TtlCalculation { source, .. } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use common_error::prelude::StatusCode::*;
    use snafu::GenerateImplicitData;

    use super::*;

    fn throw_metadata_error() -> std::result::Result<(), MetadataError> {
        Err(MetadataError::CfIdExists {
            id: 1,
            backtrace: Backtrace::generate(),
        })
    }

    #[test]
    fn test_invalid_region_desc_error() {
        let err = throw_metadata_error()
            .context(InvalidRegionDescSnafu { region: "hello" })
            .err()
            .unwrap();

        assert_eq!(StatusCode::InvalidArguments, err.status_code());
        assert!(err.backtrace_opt().is_some());
    }

    #[test]
    pub fn test_arrow_error() {
        fn throw_arrow_error() -> std::result::Result<(), ArrowError> {
            Err(ArrowError::IoError("Lorem ipsum".to_string()))
        }

        let error = throw_arrow_error()
            .context(NewRecordBatchSnafu)
            .err()
            .unwrap();
        assert_eq!(Unexpected, error.status_code());
        assert!(error.backtrace_opt().is_some());
    }
}
