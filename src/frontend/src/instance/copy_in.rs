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
use common_error::ext::BoxedError;
use common_query::Output;
use query::query_engine::options::{QueryOptions, validate_catalog_and_schema};
use servers::error::{AuthSnafu, ExecuteGrpcRequestSnafu, ExecuteQuerySnafu, Result};
use servers::query_handler::CopyInHandler;
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::metadata::TableInfoRef;

use crate::instance::Instance;

#[async_trait]
impl CopyInHandler for Instance {
    async fn copy_in_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        query_ctx: QueryContextRef,
    ) -> Result<Option<TableInfoRef>> {
        // Mirror SQL INSERT's catalog restriction (see `check_permission`):
        // when cross-catalog queries are disallowed, a COPY may only
        // target the current catalog. This must run against the ORIGINAL
        // query context — `begin_copy_in` switches the context to the
        // target catalog afterwards, so nothing downstream would catch
        // it.
        let disallow_cross_catalog_query = self
            .plugins
            .get::<QueryOptions>()
            .map(|opts| opts.disallow_cross_catalog_query)
            .unwrap_or_default();
        if disallow_cross_catalog_query {
            validate_catalog_and_schema(catalog, schema, &query_ctx)
                .map_err(BoxedError::new)
                .context(ExecuteQuerySnafu)?;
        }

        // A COPY writes rows like a SQL INSERT; check the write permission
        // before the client starts streaming data.
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(
                query_ctx.current_user(),
                PermissionReq::BulkInsert {
                    catalog,
                    schema,
                    table,
                },
            )
            .context(AuthSnafu)?;

        self.catalog_manager()
            .table(catalog, schema, table, None)
            .await
            .map(|table| table.map(|table| table.table_info()))
            .map_err(servers::error::Error::from)
    }

    async fn copy_in_insert(
        &self,
        requests: RowInsertRequests,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        self.handle_row_inserts(requests, query_ctx, false, false)
            .await
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)
    }
}
