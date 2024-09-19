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
use pipeline::{GreptimeTransformer, Pipeline, PipelineInfo, PipelineVersion};
use servers::error::{
    AuthSnafu, Error as ServerError, ExecuteGrpcRequestSnafu, PipelineSnafu, Result as ServerResult,
};
use servers::interceptor::{LogIngestInterceptor, LogIngestInterceptorRef};
use servers::query_handler::LogHandler;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::instance::Instance;

#[async_trait]
impl LogHandler for Instance {
    async fn insert_logs(
        &self,
        log: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
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
    ) -> ServerResult<Arc<Pipeline<GreptimeTransformer>>> {
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
}
