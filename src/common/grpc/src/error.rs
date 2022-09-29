use std::any::Any;

use api::DecodeError;
use common_error::prelude::{ErrorExt, StatusCode};
use datafusion::error::DataFusionError;
use snafu::{Backtrace, ErrorCompat, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unexpected empty physical plan type: {}", name))]
    EmptyPhysicalPlan { name: String, backtrace: Backtrace },

    #[snafu(display("Unexpected empty physical expr: {}", name))]
    EmptyPhysicalExpr { name: String, backtrace: Backtrace },

    #[snafu(display("Unsupported datafusion execution plan: {}", name))]
    UnsupportedDfPlan { name: String, backtrace: Backtrace },

    #[snafu(display("Unsupported datafusion physical expr: {}", name))]
    UnsupportedDfExpr { name: String, backtrace: Backtrace },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField { field: String, backtrace: Backtrace },

    #[snafu(display("Failed to new datafusion projection exec, source: {}", source))]
    NewProjection {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode physical plan node, source: {}", source))]
    DecodePhysicalPlanNode {
        source: DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Write type mismatch, column name: {}, expected: {}, actual: {}",
        column_name,
        expected,
        actual
    ))]
    TypeMismatch {
        column_name: String,
        expected: String,
        actual: String,
        backtrace: Backtrace,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::EmptyPhysicalPlan { .. }
            | Error::EmptyPhysicalExpr { .. }
            | Error::MissingField { .. }
            | Error::TypeMismatch { .. } => StatusCode::InvalidArguments,
            Error::UnsupportedDfPlan { .. } | Error::UnsupportedDfExpr { .. } => {
                StatusCode::Unsupported
            }
            Error::NewProjection { .. } | Error::DecodePhysicalPlanNode { .. } => {
                StatusCode::Internal
            }
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
