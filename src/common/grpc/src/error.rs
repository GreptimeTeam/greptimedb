use api::DecodeError;
use datafusion::error::DataFusionError;
use snafu::{Backtrace, Snafu};

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

    #[snafu(display("Write type mismatch, column name: {}", column_name))]
    TypeMismatch {
        column_name: String,
        backtrace: Backtrace,
    },
}
