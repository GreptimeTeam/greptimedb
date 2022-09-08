use std::any::Any;
use std::sync::Arc;

use api::serde::DecodeError;
use common_error::prelude::*;
use datafusion::physical_plan::ExecutionPlan;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Connect failed to {}, source: {}", url, source))]
    ConnectFailed {
        url: String,
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing {}, expected {}, actual {}", name, expected, actual))]
    MissingResult {
        name: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("Missing result header"))]
    MissingHeader,

    #[snafu(display("Tonic internal error, source: {}", source))]
    TonicStatus {
        source: tonic::Status,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to decode select result, source: {}", source))]
    DecodeSelect { source: DecodeError },

    #[snafu(display("Error occurred on the data node, code: {}, msg: {}", code, msg))]
    Datanode { code: u32, msg: String },

    #[snafu(display("Failed to encode physical plan: {:?}, source: {}", physical, source))]
    EncodePhysical {
        physical: Arc<dyn ExecutionPlan>,
        #[snafu(backtrace)]
        source: common_grpc::Error,
    },

    #[snafu(display("Mutate result has failure {}", failure))]
    MutateFailure { failure: u32, backtrace: Backtrace },

    #[snafu(display("Invalid column proto: {}", err_msg))]
    InvalidColumnProto {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Column datatype error, source: {}", source))]
    ColumnDataType {
        #[snafu(backtrace)]
        source: api::error::Error,
    },

    #[snafu(display("Failed to create vector, source: {}", source))]
    CreateVector {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ConnectFailed { .. }
            | Error::MissingResult { .. }
            | Error::MissingHeader { .. }
            | Error::TonicStatus { .. }
            | Error::DecodeSelect { .. }
            | Error::Datanode { .. }
            | Error::EncodePhysical { .. }
            | Error::MutateFailure { .. }
            | Error::InvalidColumnProto { .. }
            | Error::ColumnDataType { .. } => StatusCode::Internal,
            Error::CreateVector { source } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
