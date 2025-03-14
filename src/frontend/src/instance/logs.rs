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

use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use client::Output;
use common_error::ext::BoxedError;
use log_query::LogQuery;
use server_error::Result as ServerResult;
use servers::error::{self as server_error, AuthSnafu, ExecuteQuerySnafu};
use servers::interceptor::{LogQueryInterceptor, LogQueryInterceptorRef};
use servers::query_handler::LogQueryHandler;
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::Table;
use tonic::async_trait;

use super::Instance;

#[async_trait]
impl LogQueryHandler for Instance {
    async fn query(&self, mut request: LogQuery, ctx: QueryContextRef) -> ServerResult<Output> {
        let interceptor = self
            .plugins
            .get::<LogQueryInterceptorRef<server_error::Error>>();

        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::LogQuery)
            .context(AuthSnafu)?;

        interceptor.as_ref().pre_query(&request, ctx.clone())?;

        request
            .time_filter
            .canonicalize()
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        let plan = self
            .query_engine
            .planner()
            .plan_logs_query(request, ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        let output = self
            .statement_executor
            .exec_plan(plan, ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;

        Ok(interceptor.as_ref().post_query(output, ctx.clone())?)
    }

    async fn get_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> std::result::Result<Option<Arc<Table>>, catalog::error::Error> {
        self.catalog_manager
            .table(catalog, schema, table, None)
            .await
    }

    async fn table_names(
        &self,
        catalog: &str,
        schema: &str,
    ) -> std::result::Result<Vec<String>, catalog::error::Error> {
        self.catalog_manager
            .table_names(catalog, schema, None)
            .await
    }
}
