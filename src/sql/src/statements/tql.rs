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

use sqlparser_derive::{Visit, VisitMut};

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum Tql {
    Eval(TqlEval),
    Explain(TqlExplain),
    Analyze(TqlAnalyze),
}

/// TQL EVAL (<start>, <end>, <step>, [lookback]) <promql>
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct TqlEval {
    pub start: String,
    pub end: String,
    pub step: String,
    pub lookback: Option<String>,
    pub query: String,
}

/// TQL EXPLAIN [VERBOSE] [<start>, <end>, <step>, [lookback]] <promql>
/// doesn't execute the query but tells how the query would be executed (similar to SQL EXPLAIN).
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct TqlExplain {
    pub start: String,
    pub end: String,
    pub step: String,
    pub lookback: Option<String>,
    pub query: String,
    pub is_verbose: bool,
}

/// TQL ANALYZE [VERBOSE] (<start>, <end>, <step>, [lookback]) <promql>
/// executes the plan and tells the detailed per-step execution time (similar to SQL ANALYZE).
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct TqlAnalyze {
    pub start: String,
    pub end: String,
    pub step: String,
    pub lookback: Option<String>,
    pub query: String,
    pub is_verbose: bool,
}

/// Intermediate structure used to unify parameter mappings for various TQL operations.
/// This struct serves as a common parameter container for parsing TQL queries
/// and constructing corresponding TQL operations: `TqlEval`, `TqlAnalyze` or `TqlExplain`.
#[derive(Debug)]
pub struct TqlParameters {
    start: String,
    end: String,
    step: String,
    lookback: Option<String>,
    query: String,
    pub is_verbose: bool,
}

impl TqlParameters {
    pub fn new(
        start: String,
        end: String,
        step: String,
        lookback: Option<String>,
        query: String,
    ) -> Self {
        TqlParameters {
            start,
            end,
            step,
            lookback,
            query,
            is_verbose: false,
        }
    }
}

impl From<TqlParameters> for TqlEval {
    fn from(params: TqlParameters) -> Self {
        TqlEval {
            start: params.start,
            end: params.end,
            step: params.step,
            lookback: params.lookback,
            query: params.query,
        }
    }
}

impl From<TqlParameters> for TqlExplain {
    fn from(params: TqlParameters) -> Self {
        TqlExplain {
            start: params.start,
            end: params.end,
            step: params.step,
            query: params.query,
            lookback: params.lookback,
            is_verbose: params.is_verbose,
        }
    }
}

impl From<TqlParameters> for TqlAnalyze {
    fn from(params: TqlParameters) -> Self {
        TqlAnalyze {
            start: params.start,
            end: params.end,
            step: params.step,
            query: params.query,
            lookback: params.lookback,
            is_verbose: params.is_verbose,
        }
    }
}
