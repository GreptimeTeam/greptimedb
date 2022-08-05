use std::any::Any;

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
    ParseSql {
        #[snafu(backtrace)]
        source: sql::error::Error,
    },

    #[snafu(display("Cannot plan SQL: {}, source: {}", sql, source))]
    PlanSql {
        sql: String,
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to convert arrow schema, source: {}", source))]
    ConvertSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to convert table schema, source: {}", source))]
    TableSchemaMismatch {
        #[snafu(backtrace)]
        source: table::error::Error,
    },
}

impl ErrorExt for InnerError {
    fn status_code(&self) -> StatusCode {
        use InnerError::*;

        match self {
            // TODO(yingwen): Further categorize datafusion error.
            Datafusion { .. } => StatusCode::EngineExecuteQuery,
            // This downcast should not fail in usual case.
            PhysicalPlanDowncast { .. } | ConvertSchema { .. } | TableSchemaMismatch { .. } => {
                StatusCode::Unexpected
            }
            ParseSql { source, .. } => source.status_code(),
            PlanSql { .. } => StatusCode::PlanQuery,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<InnerError> for catalog::error::Error {
    fn from(e: InnerError) -> Self {
        catalog::error::Error::new(e)
    }
}

impl From<InnerError> for Error {
    fn from(err: InnerError) -> Self {
        Self::new(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn throw_df_error() -> Result<(), DataFusionError> {
        Err(DataFusionError::NotImplemented("test".to_string()))
    }

    fn assert_error(err: &InnerError, code: StatusCode) {
        assert_eq!(code, err.status_code());
        assert!(err.backtrace_opt().is_some());
    }

    #[test]
    fn test_datafusion_as_source() {
        let err = throw_df_error()
            .context(DatafusionSnafu { msg: "test df" })
            .err()
            .unwrap();
        assert_error(&err, StatusCode::EngineExecuteQuery);

        let err = throw_df_error()
            .context(PlanSqlSnafu { sql: "" })
            .err()
            .unwrap();
        assert_error(&err, StatusCode::PlanQuery);

        let res: Result<(), InnerError> = PhysicalPlanDowncastSnafu {}.fail();
        let err = res.err().unwrap();
        assert_error(&err, StatusCode::Unexpected);
    }

    fn raise_sql_error() -> Result<(), sql::error::Error> {
        Err(sql::error::Error::Unsupported {
            sql: "".to_string(),
            keyword: "".to_string(),
        })
    }

    #[test]
    fn test_parse_error() {
        let err = raise_sql_error().context(ParseSqlSnafu).err().unwrap();
        assert!(err.backtrace_opt().is_none());
        let sql_err = raise_sql_error().err().unwrap();
        assert_eq!(sql_err.status_code(), err.status_code());
    }
}
