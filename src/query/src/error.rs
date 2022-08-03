use std::any::Any;

use common_error::prelude::*;
use datafusion::error::DataFusionError;

common_error::define_opaque_error!(Error);

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}

impl From<catalog::error::Error> for Error {
    fn from(e: catalog::error::Error) -> Self {
        Error::new(e)
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("Table execution error: {}", source))]
    TableExecution {
        source: table::error::Error,
        backtrace: Backtrace,
    },
}

impl ErrorExt for InnerError {
    fn status_code(&self) -> StatusCode {
        match self {
            InnerError::TableExecution { .. } => StatusCode::EngineExecuteQuery,
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
    fn from(err: InnerError) -> Self {
        Self::new(err)
    }
}
