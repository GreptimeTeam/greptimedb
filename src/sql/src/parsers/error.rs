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

use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use sqlparser::parser::ParserError;

/// TQL parser & evaluation errors.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum TQLError {
    #[snafu(display("Failed to parse TQL expression"))]
    Parser {
        #[snafu(source)]
        error: ParserError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to evaluate TQL expression: {}", msg))]
    Evaluation { msg: String },

    #[snafu(display("Failed to convert TQL expression to logical expression"))]
    ConvertToLogicalExpression {
        #[snafu(source)]
        error: Box<crate::error::Error>,
        #[snafu(implicit)]
        location: Location,
    },
}
