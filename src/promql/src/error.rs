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
use promql_parser::parser::Expr as PromExpr;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr { name: String, backtrace: Backtrace },

    #[snafu(display("DataFusion error: {}", source))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unexpected plan or expression: {}", desc))]
    UnexpectedPlanExpr { desc: String, backtrace: Backtrace },

    #[snafu(display("Unknown table type, downcast failed"))]
    UnknownTable { backtrace: Backtrace },

    #[snafu(display("Cannot find time index column"))]
    NoTimeIndex { backtrace: Backtrace },

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
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            NoTimeIndex { .. }
            | UnsupportedExpr { .. }
            | MultipleVector { .. }
            | ExpectExpr { .. } => StatusCode::InvalidArguments,
            UnknownTable { .. }
            | DataFusion { .. }
            | UnexpectedPlanExpr { .. }
            | IllegalRange { .. }
            | EmptyRange { .. } => StatusCode::Internal,
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
