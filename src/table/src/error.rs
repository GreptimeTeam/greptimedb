use std::any::Any;

use common_error::prelude::*;
use common_recordbatch::error::Error as RecordBatchError;
use datafusion::error::DataFusionError;
use datatypes::arrow::error::ArrowError;

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

    #[snafu(display("Missing column when insert, column: {}", name))]
    MissingColumn { name: String, backtrace: Backtrace },

    #[snafu(display("Not expected to run ExecutionPlan more than once"))]
    ExecuteRepeatedly { backtrace: Backtrace },

    #[snafu(display("Poll stream failed, source: {}", source))]
    PollStream { source: ArrowError },
}

impl ErrorExt for InnerError {
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

impl From<InnerError> for DataFusionError {
    fn from(e: InnerError) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}

impl From<InnerError> for RecordBatchError {
    fn from(e: InnerError) -> RecordBatchError {
        RecordBatchError::new(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn throw_df_error() -> Result<()> {
        Err(DataFusionError::NotImplemented("table test".to_string())).context(DatafusionSnafu)?
    }

    fn throw_repeatedly() -> Result<()> {
        ExecuteRepeatedlySnafu {}.fail()?
    }

    #[test]
    fn test_error() {
        let err = throw_df_error().err().unwrap();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::Unknown, err.status_code());

        let err = throw_repeatedly().err().unwrap();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::Unknown, err.status_code());
    }
}
