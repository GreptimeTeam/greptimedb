use std::any::Any;

use arrow::datatypes::DataType as ArrowDatatype;
use common_error::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::error::Error as DataTypeError;
use statrs::StatsError;

common_error::define_opaque_error!(Error);

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("Fail to execute function, source: {}", source))]
    ExecuteFunction {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to generate function, source: {}", source))]
    GenerateFunction {
        source: StatsError,
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

    #[snafu(display("Failed to create accumulator: {}", err_msg))]
    CreateAccumulator { err_msg: String },

    #[snafu(display("Failed to downcast vector: {}", err_msg))]
    DowncastVector { err_msg: String },

    #[snafu(display("Bad accumulator implementation: {}", err_msg))]
    BadAccumulatorImpl {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid inputs: {}", err_msg))]
    InvalidInputs {
        #[snafu(backtrace)]
        source: DataTypeError,
        err_msg: String,
    },

    #[snafu(display(
        "Illegal input_types status, check if DataFusion has changed its UDAF execution logic"
    ))]
    InvalidInputState { backtrace: Backtrace },

    #[snafu(display("unexpected: not constant column"))]
    InvalidInputCol { backtrace: Backtrace },

    #[snafu(display("Not expected to run ExecutionPlan more than once"))]
    ExecuteRepeatedly { backtrace: Backtrace },

    #[snafu(display("Runtime is required when executing DataFusion's plan."))]
    MissingRuntime { backtrace: Backtrace },

    #[snafu(display("General DataFusion error, source: {}", source))]
    GeneralDataFusion {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to execute DataFusion ExecutionPlan, source: {}", source))]
    DataFusionExecutionPlan {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("General RecordBatch error, source: {}", source))]
    GeneralRecordBatch {
        source: common_recordbatch::error::Error,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for InnerError {
    fn status_code(&self) -> StatusCode {
        match self {
            InnerError::ExecuteFunction { .. }
            | InnerError::GenerateFunction { .. }
            | InnerError::CreateAccumulator { .. }
            | InnerError::DowncastVector { .. }
            | InnerError::InvalidInputState { .. }
            | InnerError::InvalidInputCol { .. }
            | InnerError::BadAccumulatorImpl { .. } => StatusCode::EngineExecuteQuery,
            InnerError::InvalidInputs { source, .. } => source.status_code(),
            InnerError::IntoVector { source, .. } => source.status_code(),
            InnerError::FromScalarValue { source } => source.status_code(),

            InnerError::ExecuteRepeatedly { .. }
            | InnerError::MissingRuntime { .. }
            | InnerError::GeneralDataFusion { .. }
            | InnerError::DataFusionExecutionPlan { .. } => StatusCode::Unexpected,

            InnerError::GeneralRecordBatch { source, .. } => source.status_code(),
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
        let inner_err = err.as_any().downcast_ref::<InnerError>().unwrap();
        assert_eq!(code, inner_err.status_code());
        assert!(inner_err.backtrace_opt().is_some());
    }

    #[test]
    fn test_datafusion_as_source() {
        let err: Error = throw_df_error()
            .context(ExecuteFunctionSnafu)
            .err()
            .unwrap()
            .into();
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
        let err: Error = raise_datatype_error()
            .context(IntoVectorSnafu {
                data_type: ArrowDatatype::Int32,
            })
            .err()
            .unwrap()
            .into();
        assert!(err.backtrace_opt().is_some());
        let datatype_err = raise_datatype_error().err().unwrap();
        assert_eq!(datatype_err.status_code(), err.status_code());
    }
}
