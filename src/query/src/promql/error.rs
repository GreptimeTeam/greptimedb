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
use common_time::timestamp::TimeUnit;
use datafusion::error::DataFusionError;
use promql::error::Error as PromqlError;
use promql_parser::parser::token::TokenType;
use promql_parser::parser::{Expr as PromExpr, VectorMatchCardinality};
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported vector matches: {:?}", name))]
    UnsupportedVectorMatch {
        name: VectorMatchCardinality,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected token: {:?}", token))]
    UnexpectedToken {
        token: TokenType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Internal error during building DataFusion plan"))]
    DataFusionPlanning {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected plan or expression: {}", desc))]
    UnexpectedPlanExpr {
        desc: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unknown table type, downcast failed"))]
    UnknownTable {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot find time index column in table {}", table))]
    TimeIndexNotFound {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot find value columns in table {}", table))]
    ValueNotFound {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create PromQL plan node"))]
    PromqlPlanNode {
        #[snafu(source)]
        source: PromqlError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Cannot accept multiple vector as function input, PromQL expr: {:?}",
        expr,
    ))]
    MultipleVector {
        expr: PromExpr,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Table (metric) name not found, this indicates a procedure error in PromQL planner"
    ))]
    TableNameNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("General catalog error: "))]
    Catalog {
        #[snafu(implicit)]
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Expect a range selector, but not found"))]
    ExpectRangeSelector {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Zero range in range selector"))]
    ZeroRangeSelector {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "The end time must be greater than start time, start: {:?}, end: {:?}",
        start,
        end
    ))]
    InvalidTimeRange {
        start: i64,
        end: i64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot find column {col}"))]
    ColumnNotFound {
        col: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Found multiple metric matchers in selector"))]
    MultipleMetricMatchers {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expect a metric matcher, but not found"))]
    NoMetricMatcher {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid function argument for {}", fn_name))]
    FunctionInvalidArgument {
        fn_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Attempt to combine two tables with different column sets, left: {:?}, right: {:?}",
        left,
        right
    ))]
    CombineTableColumnMismatch {
        left: Vec<String>,
        right: Vec<String>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Multi fields calculation is not supported in {}", operator))]
    MultiFieldsNotSupported {
        operator: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Matcher operator {matcher_op} is not supported for {matcher}"))]
    UnsupportedMatcherOp {
        matcher_op: String,
        matcher: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Timestamp out of range: {} of {:?}", timestamp, unit))]
    TimestampOutOfRange {
        timestamp: i64,
        unit: TimeUnit,
        #[snafu(implicit)]
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
            | ExpectRangeSelector { .. }
            | ZeroRangeSelector { .. }
            | InvalidTimeRange { .. }
            | ColumnNotFound { .. }
            | FunctionInvalidArgument { .. }
            | UnsupportedVectorMatch { .. }
            | CombineTableColumnMismatch { .. }
            | UnexpectedPlanExpr { .. }
            | UnsupportedMatcherOp { .. }
            | TimestampOutOfRange { .. } => StatusCode::InvalidArguments,

            UnknownTable { .. } => StatusCode::Internal,

            PromqlPlanNode { source, .. } => source.status_code(),

            DataFusionPlanning { .. } => StatusCode::PlanQuery,

            TableNameNotFound { .. } => StatusCode::TableNotFound,

            MultipleMetricMatchers { .. } | NoMetricMatcher { .. } => StatusCode::InvalidSyntax,

            MultiFieldsNotSupported { .. } => StatusCode::Unsupported,
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
