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

use api::v1::RowInsertRequests;
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use client::Output;
use common_error::ext::BoxedError;
use pipeline::{GreptimeTransformer, Pipeline};
use servers::error::{AuthSnafu, ExecuteGrpcRequestSnafu};
use servers::query_handler::LogHandler;
use session::context::QueryContextRef;
use snafu::ResultExt;

use super::Instance;

#[async_trait]
impl LogHandler for Instance {
    async fn insert_log(
        &self,
        log: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> servers::error::Result<Output> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            // This is a bug, it should be PermissionReq::LogWrite
            .check_permission(ctx.current_user(), PermissionReq::PromStoreWrite)
            .context(AuthSnafu)?;

        self.handle_log_inserts(log, ctx).await
    }

    async fn get_pipeline(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
    ) -> servers::error::Result<Pipeline<GreptimeTransformer>> {
        self.pipeline_operator
            .get_pipeline(query_ctx, name)
            .await
            .map_err(BoxedError::new)
            .context(servers::error::GetPipelineSnafu { name })
    }

    async fn insert_pipeline(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
        content_type: &str,
        pipeline: &str,
    ) -> servers::error::Result<()> {
        self.pipeline_operator
            .insert_pipeline(query_ctx, name, content_type, pipeline)
            .await
            .map_err(BoxedError::new)
            .context(servers::error::InsertPipelineSnafu { name })?;
        Ok(())
    }

    async fn delete_pipeline(
        &self,
        _query_ctx: QueryContextRef,
        _name: &str,
    ) -> servers::error::Result<()> {
        todo!("delete_pipeline")
    }
}

impl Instance {
    pub async fn handle_log_inserts(
        &self,
        log: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> servers::error::Result<Output> {
        self.inserter
            .handle_log_inserts(log, ctx, self.statement_executor.as_ref())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)
    }
}
