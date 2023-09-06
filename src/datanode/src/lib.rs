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

#![feature(assert_matches)]
#![feature(trait_upcasting)]

use query::query_engine::SqlStatementExecutor;

pub mod alive_keeper;
pub mod datanode;
pub mod error;
mod greptimedb_telemetry;
pub mod heartbeat;
pub mod metrics;
pub mod region_server;
pub mod server;
mod store;
#[cfg(test)]
#[allow(dead_code)]
mod tests;

// TODO(ruihang): remove this
pub struct Instance;

#[async_trait::async_trait]
impl SqlStatementExecutor for Instance {
    async fn execute_sql(
        &self,
        _stmt: sql::statements::statement::Statement,
        _query_ctx: session::context::QueryContextRef,
    ) -> query::error::Result<common_query::Output> {
        unreachable!()
    }
}
