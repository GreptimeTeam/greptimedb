use common_error::prelude::*;
use datafusion::error::DataFusionError;

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
    PlanSql {
        sql: String,
        source: DataFusionError,
        backtrace: Backtrace,
    },
}

impl ErrorExt for InnerError {
    fn status_code(&self) -> StatusCode {
        use InnerError::*;

        match self {
            ParseSql { source, .. } => source.status_code(),
            Datafusion { .. } | PhysicalPlanDowncast { .. } | PlanSql { .. } => {
                StatusCode::Internal
            }
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }
}

impl From<InnerError> for Error {
    fn from(err: InnerError) -> Self {
        Self::new(err)
    }
}
