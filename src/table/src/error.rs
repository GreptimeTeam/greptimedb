use common_error::prelude::*;
use datafusion::error::DataFusionError;

common_error::define_opaque_error!(Error);

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        Self::External(Box::new(e))
    }
}

/// Default error implementation of table.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("Datafusion error: {}", source))]
    Datafusion {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Not expected to run ExecutionPlan more than once"))]
    ExecuteRepeatedly { backtrace: Backtrace },
}

impl ErrorExt for InnerError {
    fn backtrace_opt(&self) -> Option<&snafu::Backtrace> {
        ErrorCompat::backtrace(self)
    }
}

impl From<InnerError> for Error {
    fn from(err: InnerError) -> Self {
        Self::new(err)
    }
}

impl From<InnerError> for DataFusionError {
    fn from(e: InnerError) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}
