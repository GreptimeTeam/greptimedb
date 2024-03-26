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

use datafusion_common::DataFusionError;
use sqlparser::parser::ParserError;

pub enum TQLError {
    Parser(String),
    Simplification(String),
    Evaluation(String),
}

impl From<ParserError> for TQLError {
    fn from(err: ParserError) -> Self {
        TQLError::Parser(err.to_string())
    }
}

impl From<DataFusionError> for TQLError {
    fn from(err: DataFusionError) -> Self {
        match err {
            DataFusionError::SQL(parser_err) => TQLError::Parser(parser_err.to_string()),
            DataFusionError::Plan(plan_err) => TQLError::Evaluation(plan_err),
            unspecified => {
                TQLError::Evaluation(format!("Failed to evaluate due to: {unspecified:?}"))
            }
        }
    }
}

impl From<TQLError> for ParserError {
    fn from(tql_err: TQLError) -> Self {
        match tql_err {
            TQLError::Parser(s) => {
                ParserError::ParserError(format!("Failed to parse the query: {s}"))
            }
            TQLError::Simplification(s) => {
                ParserError::ParserError(format!("Failed to simplify the query: {s}"))
            }
            TQLError::Evaluation(s) => {
                ParserError::ParserError(format!("Failed to evaluate the query: {s}"))
            }
        }
    }
}
