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

use std::sync::Arc;

use common_error::prelude::ErrorExt;
use common_query::Output;
use query::parser::{QueryLanguage, QueryStatement};
use query::plan::LogicalPlan;
use session::context::QueryContextRef;

/// SqlQueryInterceptor can track life cycle of a sql query and customize or
/// abort its execution at given point.
pub trait SqlQueryInterceptor {
    type Error: ErrorExt;

    /// Called before a query is parsed into statement.
    /// The implementation is allowed to change the query if needed.
    fn pre_parsing<'a>(
        &self,
        query: QueryLanguage,
        _query_ctx: QueryContextRef,
    ) -> Result<QueryLanguage, Self::Error> {
        Ok(query)
    }

    /// Called after query is parsed into statement. This interceptor can alter
    /// the statement or abort execution by raising an error.
    fn post_parsing(
        &self,
        statement: QueryStatement,
        _query_ctx: QueryContextRef,
    ) -> Result<QueryStatement, Self::Error> {
        Ok(statement)
    }

    /// Called before sql is actually executed. This hook is not called at the moment.
    fn pre_execute(
        &self,
        _statement: &QueryStatement,
        _plan: Option<&LogicalPlan>,
        _query_ctx: QueryContextRef,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Called after execution finished. The implementation can modify the
    /// output if needed.
    fn post_execute(
        &self,
        output: Output,
        _query_ctx: QueryContextRef,
    ) -> Result<Output, Self::Error> {
        Ok(output)
    }
}

pub type SqlQueryInterceptorRef<E> =
    Arc<dyn SqlQueryInterceptor<Error = E> + Send + Sync + 'static>;

impl<E> SqlQueryInterceptor for Option<&SqlQueryInterceptorRef<E>>
where
    E: ErrorExt,
{
    type Error = E;

    fn pre_parsing(
        &self,
        query: QueryLanguage,
        query_ctx: QueryContextRef,
    ) -> Result<QueryLanguage, Self::Error> {
        if let Some(this) = self {
            this.pre_parsing(query, query_ctx)
        } else {
            Ok(query)
        }
    }

    fn post_parsing(
        &self,
        statements: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<QueryStatement, Self::Error> {
        if let Some(this) = self {
            this.post_parsing(statements, query_ctx)
        } else {
            Ok(statements)
        }
    }

    fn pre_execute(
        &self,
        statement: &QueryStatement,
        plan: Option<&LogicalPlan>,
        query_ctx: QueryContextRef,
    ) -> Result<(), Self::Error> {
        if let Some(this) = self {
            this.pre_execute(statement, plan, query_ctx)
        } else {
            Ok(())
        }
    }

    fn post_execute(
        &self,
        output: Output,
        query_ctx: QueryContextRef,
    ) -> Result<Output, Self::Error> {
        if let Some(this) = self {
            this.post_execute(output, query_ctx)
        } else {
            Ok(output)
        }
    }
}
