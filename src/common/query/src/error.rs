use std::any::Any;

use arrow::datatypes::DataType as ArrowDatatype;
use common_error::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::error::Error as DataTypeError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to execute function, source: {}", source))]
    ExecuteFunction { source: DataFusionError },
    #[snafu(display("Fail to cast arrow array into vector: {:?}, {}", data_type, source))]
    IntoVector {
        source: DataTypeError,
        data_type: ArrowDatatype,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ExecuteFunction { source: _ } => StatusCode::EngineExecuteQuery,
            Error::IntoVector { source, .. } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}
