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

pub trait SqlQueryInterceptor {
    fn pre_parsing<'a>(&self, query: &'a str, _query_ctx: QueryContextRef) -> Result<Cow<'a, str>> {
        Ok(Cow::Borrowed(query))
    }

    fn pre_planning(&self, statement: Statement, _query_ctx: QueryContextRef) -> Result<Statement> {
        Ok(statement)
    }

    fn pre_execute(&self, _plan: &LogicalPlan, _query_ctx: QueryContextRef) -> Result<()> {
        Ok(())
    }

    fn post_execute(&self, output: Output, _query_ctx: QueryContextRef) -> Result<Output> {
        Ok(output)
    }
}
