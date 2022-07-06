use std::any::Any;
use std::io::Error as IoError;
use std::string::FromUtf8Error;

use common_error::prelude::*;
use datatypes::arrow;
use serde_json::error::Error as JsonError;

use crate::metadata::Error as MetadataError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
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

    #[snafu(display("Fail to read object from path: {}, err: {}", path, source))]
    ReadObject { path: String, source: IoError },

    #[snafu(display("Fail to write object into path: {}, err: {}", path, source))]
    WriteObject { path: String, source: IoError },

    #[snafu(display("Fail to delete object from path: {}, err: {}", path, source))]
    DeleteObject { path: String, source: IoError },

    #[snafu(display("Fail to list objects in path: {}, err: {}", path, source))]
    ListObjects { path: String, source: IoError },

    #[snafu(display("Fail to create string from bytes, err: {}", source))]
    FromUtf8 { source: FromUtf8Error },

    #[snafu(display("Fail to encode object into json , err: {}", source))]
    EncodeJson { source: JsonError },

    #[snafu(display("Fail to decode object from json , err: {}", source))]
    DecodeJson { source: JsonError },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            InvalidRegionDesc { .. } | InvalidInputSchema { .. } | BatchMissingColumn { .. } => {
                StatusCode::InvalidArguments
            }

            FromUtf8 { .. } | EncodeJson { .. } | DecodeJson { .. } => StatusCode::Unexpected,

            Error::FlushIo { .. }
            | Error::WriteParquet { .. }
            | ReadObject { .. }
            | WriteObject { .. }
            | ListObjects { .. }
            | DeleteObject { .. } => StatusCode::StorageUnavailable,
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

    use common_error::prelude::StatusCode::Internal;
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
        assert_eq!(StatusCode::Internal, error.status_code());
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
        assert_eq!(Internal, error.status_code());
        assert!(error.backtrace_opt().is_some());
    }
}
