use std::any::Any;
use std::io::Error as IoError;
use std::str::Utf8Error;

use common_error::prelude::*;
use datatypes::arrow;
use datatypes::arrow::error::ArrowError;
use serde_json::error::Error as JsonError;
use store_api::manifest::action::ProtocolVersion;
use store_api::manifest::ManifestVersion;
use store_api::storage::SequenceNumber;

use crate::metadata::Error as MetadataError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid region descriptor, region: {}, source: {}", region, source))]
    InvalidRegionDesc {
        region: String,
        #[snafu(backtrace)]
        source: MetadataError,
    },

    #[snafu(display("Invalid schema of input data, region: {}", region))]
    InvalidInputSchema {
        region: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing column {} in write batch", column))]
    BatchMissingColumn {
        column: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing timestamp in write batch"))]
    BatchMissingTimestamp { backtrace: Backtrace },

    #[snafu(display("Failed to write columns, source: {}", source))]
    FlushIo {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to write parquet file, source: {}", source))]
    WriteParquet {
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to read object from path: {}, source: {}", path, source))]
    ReadObject {
        path: String,
        backtrace: Backtrace,
        source: IoError,
    },

    #[snafu(display("Fail to write object into path: {}, source: {}", path, source))]
    WriteObject {
        path: String,
        backtrace: Backtrace,
        source: IoError,
    },

    #[snafu(display("Fail to delete object from path: {}, source: {}", path, source))]
    DeleteObject {
        path: String,
        backtrace: Backtrace,
        source: IoError,
    },

    #[snafu(display("Fail to list objects in path: {}, source: {}", path, source))]
    ListObjects {
        path: String,
        backtrace: Backtrace,
        source: IoError,
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

    #[snafu(display("Failed to write WAL, WAL name: {}, source: {}", name, source))]
    WriteWal {
        name: String,
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

    #[snafu(display("Invalid timestamp in write batch, source: {}", source))]
    InvalidTimestamp { source: crate::write_batch::Error },

    #[snafu(display("Task already cancelled"))]
    Cancelled { backtrace: Backtrace },

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
        source: ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("IO failed while reading Parquet file: {}, source: {}", file, source))]
    ReadParquetIo {
        file: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Parquet file schema is invalid, source: {}", source))]
    InvalidParquetSchema {
        #[snafu(backtrace)]
        source: crate::schema::Error,
    },

    #[snafu(display("Region is under {} state, cannot proceed operation", state))]
    InvalidRegionState {
        state: &'static str,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to read WAL, name: {}, source: {}", name, source))]
    ReadWal {
        name: String,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("WAL data corrupted, name: {}, message: {}", name, message))]
    WalDataCorrupted {
        name: String,
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Region version not found in manifest, the region: {}", region_name))]
    VersionNotFound {
        region_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Sequence of region should increase monotonically ({} > {})",
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
        source: crate::schema::Error,
    },

    #[snafu(display("Invalid raw region metadata, region: {}, source: {}", region, source))]
    InvalidRawRegion {
        region: String,
        #[snafu(backtrace)]
        source: MetadataError,
    },

    #[snafu(display("Invalid projection, source: {}", source))]
    InvalidProjection {
        #[snafu(backtrace)]
        source: crate::schema::Error,
    },

    #[snafu(display("Failed to push data to batch builder, source: {}", source))]
    PushBatch {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to build batch, {}", msg))]
    BuildBatch { msg: String, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            InvalidScanIndex { .. }
            | InvalidRegionDesc { .. }
            | InvalidInputSchema { .. }
            | BatchMissingColumn { .. }
            | BatchMissingTimestamp { .. }
            | InvalidTimestamp { .. }
            | InvalidProjection { .. }
            | BuildBatch { .. } => StatusCode::InvalidArguments,

            Utf8 { .. }
            | EncodeJson { .. }
            | DecodeJson { .. }
            | JoinTask { .. }
            | Cancelled { .. }
            | DecodeMetaActionList { .. }
            | Readline { .. }
            | InvalidParquetSchema { .. }
            | WalDataCorrupted { .. }
            | VersionNotFound { .. }
            | SequenceNotMonotonic { .. }
            | ConvertStoreSchema { .. }
            | InvalidRawRegion { .. } => StatusCode::Unexpected,

            FlushIo { .. }
            | WriteParquet { .. }
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
            | ReadParquetIo { .. }
            | InvalidRegionState { .. }
            | ReadWal { .. } => StatusCode::StorageUnavailable,

            PushBatch { source, .. } => source.status_code(),
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
    use datatypes::arrow::error::ArrowError;
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
    pub fn test_flush_error() {
        fn throw_io_error() -> std::result::Result<(), std::io::Error> {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "writer is closed",
            ))
        }

        let error = throw_io_error().context(FlushIoSnafu).err().unwrap();
        assert_eq!(StatusCode::StorageUnavailable, error.status_code());
        assert!(error.backtrace_opt().is_some());
    }

    #[test]
    pub fn test_arrow_error() {
        fn throw_arrow_error() -> std::result::Result<(), ArrowError> {
            Err(ArrowError::ExternalFormat("Lorem ipsum".to_string()))
        }

        let error = throw_arrow_error()
            .context(WriteParquetSnafu)
            .err()
            .unwrap();
        assert_eq!(StorageUnavailable, error.status_code());
        assert!(error.backtrace_opt().is_some());
    }
}
