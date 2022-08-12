use std::any::Any;
use std::io;

use common_error::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Internal error: {}", err_msg))]
    Internal { err_msg: String },

    #[snafu(display("Internal IO error, source: {}", source))]
    InternalIo { source: io::Error },

    #[snafu(display("Tokio IO error: {}, source: {}", err_msg, source))]
    TokioIo { err_msg: String, source: io::Error },

    #[snafu(display("Runtime resource error, source: {}", source))]
    RuntimeResource {
        source: common_runtime::error::Error,
    },

    #[snafu(display("Failed to convert vector, source: {}", source))]
    VectorConversion { source: datatypes::error::Error },

    #[snafu(display("Failed to collect recordbatch, source: {}", source))]
    CollectRecordbatch {
        source: common_recordbatch::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::Internal { .. } | Error::InternalIo { .. } | Error::TokioIo { .. } => {
                StatusCode::Unexpected
            }
            Error::VectorConversion { .. } | Error::CollectRecordbatch { .. } => {
                StatusCode::Internal
            }
            Error::RuntimeResource { .. } => StatusCode::RuntimeResourcesExhausted,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::InternalIo { source: e }
    }
}
