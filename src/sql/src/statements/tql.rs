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

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct TqlEval {
    pub start: String,
    pub end: String,
    pub step: String,
    pub query: String,
}

/// TQL EXPLAIN (like SQL EXPLAIN): doesn't execute the query but tells how the query would be executed.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct TqlExplain {
    pub start: String,
    pub end: String,
    pub step: String,
    pub query: String,
}

/// TQL ANALYZE (like SQL ANALYZE): executes the plan and tells the detailed per-step execution time.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct TqlAnalyze {
    pub start: String,
    pub end: String,
    pub step: String,
    pub query: String,
}
