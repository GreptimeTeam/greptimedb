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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tql {
    Eval(TqlEval),
    Explain(TqlExplain),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TqlEval {
    pub start: String,
    pub end: String,
    pub step: String,
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TqlExplain {
    pub query: String,
}
