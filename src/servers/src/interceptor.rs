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

use std::borrow::Cow;
use std::sync::Arc;

use api::v1::greptime_request::Request;
use common_error::ext::ErrorExt;
use common_query::Output;
use query::parser::PromQuery;
use query::plan::LogicalPlan;
use session::context::QueryContextRef;
use sql::statements::statement::Statement;

/// SqlQueryInterceptor can track life cycle of a sql query and customize or
/// abort its execution at given point.
pub trait SqlQueryInterceptor {
    type Error: ErrorExt;

    /// Called before a query string is parsed into sql statements.
    /// The implementation is allowed to change the sql string if needed.
    fn pre_parsing<'a>(
        &self,
        query: &'a str,
        _query_ctx: QueryContextRef,
    ) -> Result<Cow<'a, str>, Self::Error> {
        Ok(Cow::Borrowed(query))
    }

    /// Called after sql is parsed into statements. This interceptor is called
    /// on each statement and the implementation can alter the statement or
    /// abort execution by raising an error.
    fn post_parsing(
        &self,
        statements: Vec<Statement>,
        _query_ctx: QueryContextRef,
    ) -> Result<Vec<Statement>, Self::Error> {
        Ok(statements)
    }

    /// Called before sql is actually executed. This hook is not called at the moment.
    fn pre_execute(
        &self,
        _statement: &Statement,
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

    fn pre_parsing<'a>(
        &self,
        query: &'a str,
        query_ctx: QueryContextRef,
    ) -> Result<Cow<'a, str>, Self::Error> {
        if let Some(this) = self {
            this.pre_parsing(query, query_ctx)
        } else {
            Ok(Cow::Borrowed(query))
        }
    }

    fn post_parsing(
        &self,
        statements: Vec<Statement>,
        query_ctx: QueryContextRef,
    ) -> Result<Vec<Statement>, Self::Error> {
        if let Some(this) = self {
            this.post_parsing(statements, query_ctx)
        } else {
            Ok(statements)
        }
    }

    fn pre_execute(
        &self,
        statement: &Statement,
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

/// GrpcQueryInterceptor can track life cycle of a grpc request and customize or
/// abort its execution at given point.
pub trait GrpcQueryInterceptor {
    type Error: ErrorExt;

    /// Called before request is actually executed.
    fn pre_execute(
        &self,
        _request: &Request,
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

pub type GrpcQueryInterceptorRef<E> =
    Arc<dyn GrpcQueryInterceptor<Error = E> + Send + Sync + 'static>;

impl<E> GrpcQueryInterceptor for Option<&GrpcQueryInterceptorRef<E>>
where
    E: ErrorExt,
{
    type Error = E;

    fn pre_execute(
        &self,
        _request: &Request,
        _query_ctx: QueryContextRef,
    ) -> Result<(), Self::Error> {
        if let Some(this) = self {
            this.pre_execute(_request, _query_ctx)
        } else {
            Ok(())
        }
    }

    fn post_execute(
        &self,
        output: Output,
        _query_ctx: QueryContextRef,
    ) -> Result<Output, Self::Error> {
        if let Some(this) = self {
            this.post_execute(output, _query_ctx)
        } else {
            Ok(output)
        }
    }
}

/// PromQueryInterceptor can track life cycle of a prometheus request and customize or
/// abort its execution at given point.
pub trait PromQueryInterceptor {
    type Error: ErrorExt;

    /// Called before request is actually executed.
    fn pre_execute(
        &self,
        _query: &PromQuery,
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

pub type PromQueryInterceptorRef<E> =
    Arc<dyn PromQueryInterceptor<Error = E> + Send + Sync + 'static>;

impl<E> PromQueryInterceptor for Option<PromQueryInterceptorRef<E>>
where
    E: ErrorExt,
{
    type Error = E;

    fn pre_execute(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> Result<(), Self::Error> {
        if let Some(this) = self {
            this.pre_execute(query, query_ctx)
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
