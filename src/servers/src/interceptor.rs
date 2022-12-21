// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;

use common_query::Output;
use query::plan::LogicalPlan;
use session::context::QueryContextRef;
use sql::statements::statement::Statement;

use crate::error::Result;

/// SqlQueryInterceptor can track life cycle of a sql query and customize or
/// abort its execution at given point.
pub trait SqlQueryInterceptor {
    /// Called before a query string is parsed into sql statements.
    /// The implementation is allowed to change the sql string if needed.
    fn pre_parsing<'a>(&self, query: &'a str, _query_ctx: QueryContextRef) -> Result<Cow<'a, str>> {
        Ok(Cow::Borrowed(query))
    }

    /// Called after sql is parsed into statements. This interceptor is called
    /// on each statement and the implementation can alter the statement or
    /// abort execution by raising an error.
    fn post_parsing(&self, statement: Statement, _query_ctx: QueryContextRef) -> Result<Statement> {
        Ok(statement)
    }

    /// Called before sql is actually executed.
    fn pre_execute(
        &self,
        _statement: Statement,
        _plan: Option<&LogicalPlan>,
        _query_ctx: QueryContextRef,
    ) -> Result<()> {
        Ok(())
    }

    /// Called after execution finished. The implementation can modify the
    /// output if needed.
    fn post_execute(&self, output: Output, _query_ctx: QueryContextRef) -> Result<Output> {
        Ok(output)
    }
}
