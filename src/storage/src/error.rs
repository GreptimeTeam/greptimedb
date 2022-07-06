use std::any::Any;

use common_error::prelude::*;
use datatypes::arrow;

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

    #[snafu(display("Error while writing columns, source: {}", source))]
    FlushIo {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Arrow error, source: {}", source))]
    Arrow {
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            InvalidRegionDesc { .. } | InvalidInputSchema { .. } | BatchMissingColumn { .. } => {
                StatusCode::InvalidArguments
            }
            Error::FlushIo { .. } | Error::Arrow { .. } => StatusCode::Internal,
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

        let error = throw_arrow_error().context(ArrowSnafu).err().unwrap();
        assert_eq!(Internal, error.status_code());
        assert!(error.backtrace_opt().is_some());
    }
}
