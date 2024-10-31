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

use api::prom_store::remote::ReadRequest;
use api::v1::greptime_request::Request;
use api::v1::RowInsertRequests;
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_query::Output;
use datafusion_expr::LogicalPlan;
use query::parser::PromQuery;
use serde_json::Value;
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

/// ScriptInterceptor can track life cycle of a script request and customize or
/// abort its execution at given point.
pub trait ScriptInterceptor {
    type Error: ErrorExt;

    /// Called before script request is actually executed.
    fn pre_execute(&self, _name: &str, _query_ctx: QueryContextRef) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub type ScriptInterceptorRef<E> = Arc<dyn ScriptInterceptor<Error = E> + Send + Sync + 'static>;

impl<E: ErrorExt> ScriptInterceptor for Option<ScriptInterceptorRef<E>> {
    type Error = E;

    fn pre_execute(&self, name: &str, query_ctx: QueryContextRef) -> Result<(), Self::Error> {
        if let Some(this) = self {
            this.pre_execute(name, query_ctx)
        } else {
            Ok(())
        }
    }
}

/// LineProtocolInterceptor can track life cycle of a line protocol request
/// and customize or abort its execution at given point.
#[async_trait]
pub trait LineProtocolInterceptor {
    type Error: ErrorExt;

    fn pre_execute(&self, _line: &str, _query_ctx: QueryContextRef) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Called after the lines are converted to the [RowInsertRequests].
    /// We can then modify the resulting requests if needed.
    /// Typically used in some backward compatibility situation.
    async fn post_lines_conversion(
        &self,
        requests: RowInsertRequests,
        query_context: QueryContextRef,
    ) -> Result<RowInsertRequests, Self::Error> {
        let _ = query_context;
        Ok(requests)
    }
}

pub type LineProtocolInterceptorRef<E> =
    Arc<dyn LineProtocolInterceptor<Error = E> + Send + Sync + 'static>;

#[async_trait]
impl<E: ErrorExt> LineProtocolInterceptor for Option<LineProtocolInterceptorRef<E>> {
    type Error = E;

    fn pre_execute(&self, line: &str, query_ctx: QueryContextRef) -> Result<(), Self::Error> {
        if let Some(this) = self {
            this.pre_execute(line, query_ctx)
        } else {
            Ok(())
        }
    }

    async fn post_lines_conversion(
        &self,
        requests: RowInsertRequests,
        query_context: QueryContextRef,
    ) -> Result<RowInsertRequests, Self::Error> {
        if let Some(this) = self {
            this.post_lines_conversion(requests, query_context).await
        } else {
            Ok(requests)
        }
    }
}

/// OpenTelemetryProtocolInterceptor can track life cycle of an open telemetry protocol request
/// and customize or abort its execution at given point.
pub trait OpenTelemetryProtocolInterceptor {
    type Error: ErrorExt;

    fn pre_execute(&self, _query_ctx: QueryContextRef) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub type OpenTelemetryProtocolInterceptorRef<E> =
    Arc<dyn OpenTelemetryProtocolInterceptor<Error = E> + Send + Sync + 'static>;

impl<E: ErrorExt> OpenTelemetryProtocolInterceptor
    for Option<OpenTelemetryProtocolInterceptorRef<E>>
{
    type Error = E;

    fn pre_execute(&self, query_ctx: QueryContextRef) -> Result<(), Self::Error> {
        if let Some(this) = self {
            this.pre_execute(query_ctx)
        } else {
            Ok(())
        }
    }
}

/// PromStoreProtocolInterceptor can track life cycle of a prom store request
/// and customize or abort its execution at given point.
pub trait PromStoreProtocolInterceptor {
    type Error: ErrorExt;

    fn pre_write(
        &self,
        _write_req: &RowInsertRequests,
        _ctx: QueryContextRef,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn pre_read(&self, _read_req: &ReadRequest, _ctx: QueryContextRef) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub type PromStoreProtocolInterceptorRef<E> =
    Arc<dyn PromStoreProtocolInterceptor<Error = E> + Send + Sync + 'static>;

impl<E: ErrorExt> PromStoreProtocolInterceptor for Option<PromStoreProtocolInterceptorRef<E>> {
    type Error = E;

    fn pre_write(
        &self,
        write_req: &RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<(), Self::Error> {
        if let Some(this) = self {
            this.pre_write(write_req, ctx)
        } else {
            Ok(())
        }
    }

    fn pre_read(&self, read_req: &ReadRequest, ctx: QueryContextRef) -> Result<(), Self::Error> {
        if let Some(this) = self {
            this.pre_read(read_req, ctx)
        } else {
            Ok(())
        }
    }
}

/// LogIngestInterceptor can track life cycle of a log ingestion request
/// and customize or abort its execution at given point.
pub trait LogIngestInterceptor {
    type Error: ErrorExt;

    /// Called before pipeline execution.
    fn pre_pipeline(
        &self,
        values: Vec<Value>,
        _query_ctx: QueryContextRef,
    ) -> Result<Vec<Value>, Self::Error> {
        Ok(values)
    }

    /// Called before insertion.
    fn pre_ingest(
        &self,
        request: RowInsertRequests,
        _query_ctx: QueryContextRef,
    ) -> Result<RowInsertRequests, Self::Error> {
        Ok(request)
    }
}

pub type LogIngestInterceptorRef<E> =
    Arc<dyn LogIngestInterceptor<Error = E> + Send + Sync + 'static>;

impl<E> LogIngestInterceptor for Option<&LogIngestInterceptorRef<E>>
where
    E: ErrorExt,
{
    type Error = E;

    fn pre_pipeline(
        &self,
        values: Vec<Value>,
        query_ctx: QueryContextRef,
    ) -> Result<Vec<Value>, Self::Error> {
        if let Some(this) = self {
            this.pre_pipeline(values, query_ctx)
        } else {
            Ok(values)
        }
    }

    fn pre_ingest(
        &self,
        request: RowInsertRequests,
        query_ctx: QueryContextRef,
    ) -> Result<RowInsertRequests, Self::Error> {
        if let Some(this) = self {
            this.pre_ingest(request, query_ctx)
        } else {
            Ok(request)
        }
    }
}
