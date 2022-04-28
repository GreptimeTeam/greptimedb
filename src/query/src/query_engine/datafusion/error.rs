use common_error::ext::ErrorExt;
use datafusion::error::DataFusionError;
use snafu::{Backtrace, ErrorCompat, Snafu};

use crate::error::Error;

/// Inner error of datafusion based query engine.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("{}: {}", msg, source))]
    Datafusion {
        msg: &'static str,
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("PhysicalPlan downcast failed"))]
    PhysicalPlanDowncast { backtrace: Backtrace },

    // The sql error already contains the SQL.
    #[snafu(display("Cannot parse SQL, source: {}", source))]
    ParseSql { source: sql::errors::ParserError },

    #[snafu(display("Cannot plan SQL: {}, source: {}", sql, source))]
    Planner {
        sql: String,
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Execution error: {}", message))]
    Execution { message: String },
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
