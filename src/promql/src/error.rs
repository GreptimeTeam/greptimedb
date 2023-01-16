// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use common_error::prelude::*;
use datafusion::error::DataFusionError;
use promql_parser::parser::{Expr as PromExpr, TokenType};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr { name: String, backtrace: Backtrace },

    #[snafu(display("Unexpected token: {}", token))]
    UnexpectedToken {
        token: TokenType,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal error during build DataFusion plan, error: {}", source))]
    DataFusionPlanning {
        source: datafusion::error::DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Unexpected plan or expression: {}", desc))]
    UnexpectedPlanExpr { desc: String, backtrace: Backtrace },

    #[snafu(display("Unknown table type, downcast failed"))]
    UnknownTable { backtrace: Backtrace },

    #[snafu(display("Cannot find time index column in table {}", table))]
    TimeIndexNotFound { table: String, backtrace: Backtrace },

    #[snafu(display("Cannot find value columns in table {}", table))]
    ValueNotFound { table: String, backtrace: Backtrace },

    #[snafu(display("Cannot find label in table {}, source: {}", table, source))]
    LabelNotFound {
        table: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Cannot find the table {}", table))]
    TableNotFound {
        table: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Cannot accept multiple vector as function input, PromQL expr: {:?}",
        expr
    ))]
    MultipleVector {
        expr: PromExpr,
        backtrace: Backtrace,
    },

    #[snafu(display("Expect a PromQL expr but not found, input expr: {:?}", expr))]
    ExpectExpr {
        expr: PromExpr,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "Illegal range: offset {}, length {}, array len {}",
        offset,
        length,
        len
    ))]
    IllegalRange {
        offset: u32,
        length: u32,
        len: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Empty range is not expected"))]
    EmptyRange { backtrace: Backtrace },

    #[snafu(display(
        "Table (metric) name not found, this indicates a procedure error in PromQL planner"
    ))]
    TableNameNotFound { backtrace: Backtrace },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            TimeIndexNotFound { .. }
            | ValueNotFound { .. }
            | UnsupportedExpr { .. }
            | UnexpectedToken { .. }
            | MultipleVector { .. }
            | LabelNotFound { .. }
            | ExpectExpr { .. } => StatusCode::InvalidArguments,
            UnknownTable { .. }
            | TableNotFound { .. }
            | DataFusionPlanning { .. }
            | UnexpectedPlanExpr { .. }
            | IllegalRange { .. }
            | EmptyRange { .. }
            | TableNameNotFound { .. } => StatusCode::Internal,
        }
    }
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(err: Error) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

pub(crate) fn ensure(
    predicate: bool,
    error: DataFusionError,
) -> std::result::Result<(), DataFusionError> {
    if predicate {
        Ok(())
    } else {
        Err(error)
    }
}
