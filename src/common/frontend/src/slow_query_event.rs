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

use session::context::QueryContextRef;

#[derive(Debug)]
pub struct SlowQueryEvent {
    pub cost: u64,
    pub threshold: u64,
    pub query: String,
    pub is_promql: bool,
    pub query_ctx: QueryContextRef,
    pub promql_range: Option<u64>,
    pub promql_step: Option<u64>,
    pub promql_start: Option<i64>,
    pub promql_end: Option<i64>,
}
