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

use api::v1::RowInsertRequests;
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use client::Output;
use common_error::ext::BoxedError;
use datatypes::timestamp::TimestampNanosecond;
use pipeline::pipeline_operator::PipelineOperator;
use pipeline::{Pipeline, PipelineInfo, PipelineVersion};
use servers::error::{
    AuthSnafu, Error as ServerError, ExecuteGrpcRequestSnafu, PipelineSnafu, Result as ServerResult,
};
use servers::interceptor::{LogIngestInterceptor, LogIngestInterceptorRef};
use servers::query_handler::PipelineHandler;
use session::context::{QueryContext, QueryContextRef};
use snafu::ResultExt;
use table::Table;

use crate::instance::Instance;

#[async_trait]
impl PipelineHandler for Instance {
    async fn insert(&self, log: RowInsertRequests, ctx: QueryContextRef) -> ServerResult<Output> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::LogWrite)
            .context(AuthSnafu)?;

        let log = self
            .plugins
            .get::<LogIngestInterceptorRef<ServerError>>()
            .as_ref()
            .pre_ingest(log, ctx.clone())?;

        self.handle_log_inserts(log, ctx).await
    }

    async fn get_pipeline(
        &self,
        name: &str,
        version: PipelineVersion,
        query_ctx: QueryContextRef,
    ) -> ServerResult<Arc<Pipeline>> {
        self.pipeline_operator
            .get_pipeline(query_ctx, name, version)
            .await
            .context(PipelineSnafu)
    }

    async fn insert_pipeline(
        &self,
        name: &str,
        content_type: &str,
        pipeline: &str,
        query_ctx: QueryContextRef,
    ) -> ServerResult<PipelineInfo> {
        self.pipeline_operator
            .insert_pipeline(name, content_type, pipeline, query_ctx)
            .await
            .context(PipelineSnafu)
    }

    async fn delete_pipeline(
        &self,
        name: &str,
        version: PipelineVersion,
        ctx: QueryContextRef,
    ) -> ServerResult<Option<()>> {
        self.pipeline_operator
            .delete_pipeline(name, version, ctx)
            .await
            .context(PipelineSnafu)
    }

    async fn get_table(
        &self,
        table: &str,
        query_ctx: &QueryContext,
    ) -> std::result::Result<Option<Arc<Table>>, catalog::error::Error> {
        let catalog = query_ctx.current_catalog();
        let schema = query_ctx.current_schema();
        self.catalog_manager
            .table(catalog, &schema, table, None)
            .await
    }

    fn build_pipeline(&self, pipeline: &str) -> ServerResult<Pipeline> {
        PipelineOperator::build_pipeline(pipeline).context(PipelineSnafu)
    }

    async fn get_pipeline_str(
        &self,
        name: &str,
        version: PipelineVersion,
        query_ctx: QueryContextRef,
    ) -> ServerResult<(String, TimestampNanosecond)> {
        self.pipeline_operator
            .get_pipeline_str(name, version, query_ctx)
            .await
            .context(PipelineSnafu)
            .map(|(p, _)| p)
    }
}

impl Instance {
    pub async fn handle_log_inserts(
        &self,
        log: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
        self.inserter
            .handle_log_inserts(log, ctx, self.statement_executor.as_ref())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)
    }

    pub async fn handle_trace_inserts(
        &self,
        rows: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
        self.inserter
            .handle_trace_inserts(rows, ctx, self.statement_executor.as_ref())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)
    }
}
