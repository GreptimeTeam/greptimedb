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

    #[snafu(display("Failed to create gRPC channel, source: {}", source))]
    CreateChannel {
        source: tonic::transport::Error,
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
            Error::NewProjection { .. }
            | Error::DecodePhysicalPlanNode { .. }
            | Error::CreateChannel { .. } => StatusCode::Internal,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use snafu::{OptionExt, ResultExt};

    use super::*;

    type StdResult<E> = std::result::Result<(), E>;

    fn throw_none_option() -> Option<String> {
        None
    }

    #[test]
    fn test_empty_physical_plan_error() {
        let e = throw_none_option()
            .context(EmptyPhysicalPlanSnafu { name: "test" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_empty_physical_expr_error() {
        let e = throw_none_option()
            .context(EmptyPhysicalExprSnafu { name: "test" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_unsupported_df_plan_error() {
        let e = throw_none_option()
            .context(UnsupportedDfPlanSnafu { name: "test" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Unsupported);
    }

    #[test]
    fn test_unsupported_df_expr_error() {
        let e = throw_none_option()
            .context(UnsupportedDfExprSnafu { name: "test" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Unsupported);
    }

    #[test]
    fn test_missing_field_error() {
        let e = throw_none_option()
            .context(MissingFieldSnafu { field: "test" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_new_projection_error() {
        fn throw_df_error() -> StdResult<DataFusionError> {
            Err(DataFusionError::NotImplemented("".to_string()))
        }

        let e = throw_df_error().context(NewProjectionSnafu).err().unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_decode_physical_plan_node_error() {
        fn throw_decode_error() -> StdResult<DecodeError> {
            Err(DecodeError::new("test"))
        }

        let e = throw_decode_error()
            .context(DecodePhysicalPlanNodeSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_type_mismatch_error() {
        let e = throw_none_option()
            .context(TypeMismatchSnafu {
                column_name: "",
                expected: "",
                actual: "",
            })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_create_channel_error() {
        fn throw_tonic_error() -> StdResult<tonic::transport::Error> {
            tonic::transport::Endpoint::new("http//http").map(|_| ())
        }

        let e = throw_tonic_error()
            .context(CreateChannelSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }
}
