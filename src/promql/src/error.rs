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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datafusion::error::DataFusionError;
use promql_parser::parser::{Expr as PromExpr, TokenType, VectorMatchCardinality};
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr { name: String, location: Location },

    #[snafu(display("Unsupported vector matches: {:?}", name))]
    UnsupportedVectorMatch {
        name: VectorMatchCardinality,
        location: Location,
    },

    #[snafu(display("Unexpected token: {:?}", token))]
    UnexpectedToken {
        token: TokenType,
        location: Location,
    },

    #[snafu(display("Internal error during building DataFusion plan"))]
    DataFusionPlanning {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        location: Location,
    },

    #[snafu(display("Unexpected plan or expression: {}", desc))]
    UnexpectedPlanExpr { desc: String, location: Location },

    #[snafu(display("Unknown table type, downcast failed"))]
    UnknownTable { location: Location },

    #[snafu(display("Cannot find time index column in table {}", table))]
    TimeIndexNotFound { table: String, location: Location },

    #[snafu(display("Cannot find value columns in table {}", table))]
    ValueNotFound { table: String, location: Location },

    #[snafu(display(
        "Cannot accept multiple vector as function input, PromQL expr: {:?}",
        expr,
    ))]
    MultipleVector { expr: PromExpr, location: Location },

    #[snafu(display("Expect a PromQL expr but not found, input expr: {:?}", expr))]
    ExpectExpr { expr: PromExpr, location: Location },
    #[snafu(display(
        "Illegal range: offset {}, length {}, array len {}",
        offset,
        length,
        len,
    ))]
    IllegalRange {
        offset: u32,
        length: u32,
        len: usize,
        location: Location,
    },

    #[snafu(display("Failed to deserialize"))]
    Deserialize {
        #[snafu(source)]
        error: prost::DecodeError,
        location: Location,
    },

    #[snafu(display("Empty range is not expected"))]
    EmptyRange { location: Location },

    #[snafu(display(
        "Table (metric) name not found, this indicates a procedure error in PromQL planner"
    ))]
    TableNameNotFound { location: Location },

    #[snafu(display("General catalog error: "))]
    Catalog {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Expect a range selector, but not found"))]
    ExpectRangeSelector { location: Location },

    #[snafu(display("Zero range in range selector"))]
    ZeroRangeSelector { location: Location },

    #[snafu(display("Cannot find column {col}"))]
    ColumnNotFound { col: String, location: Location },

    #[snafu(display("Found multiple metric matchers in selector"))]
    MultipleMetricMatchers { location: Location },

    #[snafu(display("Expect a metric matcher, but not found"))]
    NoMetricMatcher { location: Location },

    #[snafu(display("Invalid function argument for {}", fn_name))]
    FunctionInvalidArgument { fn_name: String, location: Location },

    #[snafu(display(
        "Attempt to combine two tables with different column sets, left: {:?}, right: {:?}",
        left,
        right
    ))]
    CombineTableColumnMismatch {
        left: Vec<String>,
        right: Vec<String>,
        location: Location,
    },
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
            | ExpectExpr { .. }
            | ExpectRangeSelector { .. }
            | ZeroRangeSelector { .. }
            | ColumnNotFound { .. }
            | Deserialize { .. }
            | FunctionInvalidArgument { .. }
            | UnsupportedVectorMatch { .. }
            | CombineTableColumnMismatch { .. }
            | DataFusionPlanning { .. }
            | UnexpectedPlanExpr { .. }
            | IllegalRange { .. } => StatusCode::InvalidArguments,

            UnknownTable { .. } | EmptyRange { .. } => StatusCode::Internal,

            TableNameNotFound { .. } => StatusCode::TableNotFound,

            MultipleMetricMatchers { .. } | NoMetricMatcher { .. } => StatusCode::InvalidSyntax,

            Catalog { source, .. } => source.status_code(),
        }
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
