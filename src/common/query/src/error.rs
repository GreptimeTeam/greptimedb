use std::any::Any;

use arrow::datatypes::DataType as ArrowDatatype;
use common_error::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::error::Error as DataTypeError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to execute function, source: {}", source))]
    ExecuteFunction {
        source: DataFusionError,
        backtrace: Backtrace,
    },
    #[snafu(display("Fail to cast scalar value into vector: {}", source))]
    FromScalarValue {
        #[snafu(backtrace)]
        source: DataTypeError,
    },
    #[snafu(display("Fail to cast arrow array into vector: {:?}, {}", data_type, source))]
    IntoVector {
        #[snafu(backtrace)]
        source: DataTypeError,
        data_type: ArrowDatatype,
    },
    #[snafu(display("Common error: {}, {}", msg, backtrace))]
    External { msg: String, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ExecuteFunction { .. } => StatusCode::EngineExecuteQuery,
            Error::IntoVector { source, .. } => source.status_code(),
            Error::FromScalarValue { source } => source.status_code(),
            Error::External { .. } => StatusCode::Internal,
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

#[cfg(test)]
mod tests {
    use snafu::GenerateImplicitData;

    use super::*;

    fn throw_df_error() -> std::result::Result<(), DataFusionError> {
        Err(DataFusionError::NotImplemented("test".to_string()))
    }

    fn assert_error(err: &Error, code: StatusCode) {
        assert_eq!(code, err.status_code());
        assert!(err.backtrace_opt().is_some());
    }

    #[test]
    fn test_datafusion_as_source() {
        let err = throw_df_error()
            .context(ExecuteFunctionSnafu {})
            .err()
            .unwrap();
        assert_error(&err, StatusCode::EngineExecuteQuery);
    }

    fn raise_datatype_error() -> std::result::Result<(), DataTypeError> {
        Err(DataTypeError::Conversion {
            from: "test".to_string(),
            backtrace: Backtrace::generate(),
        })
    }

    #[test]
    fn test_into_vector_error() {
        let err = raise_datatype_error()
            .context(IntoVectorSnafu {
                data_type: ArrowDatatype::Int32,
            })
            .err()
            .unwrap();
        assert!(err.backtrace_opt().is_some());
        let datatype_err = raise_datatype_error().err().unwrap();
        assert_eq!(datatype_err.status_code(), err.status_code());
    }
}
