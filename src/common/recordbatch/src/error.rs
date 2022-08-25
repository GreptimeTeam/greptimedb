//! Error of record batch.
use std::any::Any;

use common_error::ext::BoxedError;
use common_error::prelude::*;
common_error::define_opaque_error!(Error);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("Fail to create datafusion record batch, source: {}", source))]
    NewDfRecordBatch {
        source: datatypes::arrow::error::ArrowError,
        backtrace: Backtrace,
    },
    #[snafu(display("Data types error, source: {}", source))]
    DataTypes {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },
    #[snafu(display("External error, source: {}", source))]
    External {
        #[snafu(backtrace)]
        source: BoxedError,
    },
}

impl ErrorExt for InnerError {
    fn status_code(&self) -> StatusCode {
        match self {
            InnerError::NewDfRecordBatch { .. } => StatusCode::InvalidArguments,
            InnerError::DataTypes { .. } => StatusCode::Internal,
            InnerError::External { .. } => StatusCode::Unknown,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<InnerError> for Error {
    fn from(e: InnerError) -> Error {
        Error::new(e)
    }
}
