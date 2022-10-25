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

    #[snafu(display("Failed to create RecordBatches, reason: {}", reason))]
    CreateRecordBatches {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert Arrow schema, source: {}", source))]
    SchemaConversion {
        source: datatypes::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to poll stream, source: {}", source))]
    PollStream {
        source: datatypes::arrow::error::ArrowError,
        backtrace: Backtrace,
    },
}

impl ErrorExt for InnerError {
    fn status_code(&self) -> StatusCode {
        match self {
            InnerError::NewDfRecordBatch { .. } => StatusCode::InvalidArguments,

            InnerError::DataTypes { .. }
            | InnerError::CreateRecordBatches { .. }
            | InnerError::PollStream { .. } => StatusCode::Internal,

            InnerError::External { source } => source.status_code(),

            InnerError::SchemaConversion { source, .. } => source.status_code(),
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
