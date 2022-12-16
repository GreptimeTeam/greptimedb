// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    #[snafu(display("The SQL string has multiple statements, sql: {}", sql))]
    MultipleStatements { sql: String, backtrace: Backtrace },

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

    #[snafu(display(
        "Failed to convert DataFusion's recordbatch stream, source: {}",
        source
    ))]
    ConvertDfRecordBatchStream {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to execute physical plan, source: {}", source))]
    ExecutePhysicalPlan {
        #[snafu(backtrace)]
        source: common_query::error::Error,
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
            ConvertDfRecordBatchStream { source } => source.status_code(),
            ExecutePhysicalPlan { source } => source.status_code(),
            MultipleStatements { .. } => StatusCode::InvalidArguments,
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
        catalog::error::Error::RegisterTable {
            source: BoxedError::new(e),
        }
    }
}

impl From<InnerError> for Error {
    fn from(err: InnerError) -> Self {
        Self::new(err)
    }
}

#[cfg(test)]
mod tests {
    use common_error::mock::MockError;

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

    #[test]
    pub fn test_from_inner_error() {
        let err = InnerError::TableSchemaMismatch {
            source: table::error::Error::new(MockError::new(StatusCode::Unexpected)),
        };

        let catalog_error = catalog::error::Error::from(err);
        // [InnerError]  to [catalog::error::Error] is considered as Internal error
        assert_eq!(StatusCode::Internal, catalog_error.status_code());
    }
}
