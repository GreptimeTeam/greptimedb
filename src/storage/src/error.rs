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
    FlushIo { source: std::io::Error },

    #[snafu(display("Arrow error, source: {}", source))]
    Arrow { source: arrow::error::ArrowError },
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
}
