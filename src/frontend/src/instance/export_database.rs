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

use auth::{
    PermissionAction, PermissionChecker, PermissionCheckerRef, PermissionReq,
    PermissionTableTarget, PermissionTableTargets,
};
use common_error::ext::BoxedError;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::error;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::StringVector;
use operator::statement::export_database::{DatabaseExportSummary, PreparedDatabaseExport};
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::ast::{Ident, ObjectName};
use sql::statements::OptionMap;
use sql::statements::copy::{Copy, CopyDatabase, CopyDatabaseArgument};
use sql::statements::statement::Statement;
use table::requests::CopyDatabaseRequest;
use tokio_util::sync::CancellationToken;

use crate::error::{PermissionSnafu, Result};
use crate::instance::Instance;

pub(crate) fn parse_metric_export_requested(options: &OptionMap) -> Result<bool> {
    match options.get("experimental_metric_export") {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(operator::error::InvalidDatabaseExportSnafu {
            reason: "experimental_metric_export must be true or false",
        }
        .build()
        .into()),
    }
}

impl Instance {
    pub(crate) fn show_metric_export_capability(&self) -> Result<Output> {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "EXPERIMENTAL_METRIC_EXPORT",
            ConcreteDataType::string_datatype(),
            false,
        )]));
        let batches = RecordBatches::try_from_columns(
            schema,
            vec![Arc::new(StringVector::from(vec![
                self.experimental_metric_export.to_string(),
            ])) as datatypes::vectors::VectorRef],
        )
        .map_err(BoxedError::new)
        .context(crate::error::ExternalSnafu)?;
        Ok(Output::new_with_record_batches(batches))
    }

    pub(crate) async fn copy_metric_database(
        &self,
        arg: CopyDatabaseArgument,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        if !self.experimental_metric_export {
            return Err(operator::error::InvalidDatabaseExportSnafu {
                reason: "experimental_metric_export is disabled on this frontend",
            }
            .build()
            .into());
        }
        let req = operator::statement::to_copy_database_request(arg, &ctx)?;
        let plan = self.prepare_database_export(req, None, &ctx).await?;
        let cancellation = CancellationToken::new();
        let _guard = cancellation.clone().drop_guard();
        let executor = self.statement_executor.clone();
        // Dropping the request only cancels; the runtime task retains and drains started I/O.
        let task = common_runtime::spawn_query(async move {
            let result = executor.export_database(plan, &cancellation, ctx).await;
            if let Err(err) = &result {
                error!(err; "Experimental database export failed after draining started work");
            }
            result
        });
        let summary = task.await.context(operator::error::JoinTaskSnafu)??;
        Ok(Output::new_with_affected_rows(summary.rows))
    }

    #[allow(dead_code)]
    async fn export_database(
        &self,
        req: CopyDatabaseRequest,
        names: Option<&[String]>,
        cancellation: &CancellationToken,
        ctx: QueryContextRef,
    ) -> Result<DatabaseExportSummary> {
        if cancellation.is_cancelled() {
            return Err(operator::error::DatabaseExportCancelledSnafu.build().into());
        }
        let plan = self.prepare_database_export(req, names, &ctx).await?;
        Ok(self
            .statement_executor
            .export_database(plan, cancellation, ctx)
            .await?)
    }

    /// Exercise the internal authenticated entry without exposing SQL/CLI activation.
    #[cfg(any(test, feature = "testing"))]
    pub async fn export_database_for_test(
        &self,
        req: CopyDatabaseRequest,
        names: Option<&[String]>,
        cancellation: &CancellationToken,
        ctx: QueryContextRef,
    ) -> Result<DatabaseExportSummary> {
        self.export_database(req, names, cancellation, ctx).await
    }

    async fn prepare_database_export(
        &self,
        req: CopyDatabaseRequest,
        names: Option<&[String]>,
        ctx: &QueryContextRef,
    ) -> Result<PreparedDatabaseExport> {
        let stmt = Statement::Copy(Copy::CopyDatabase(CopyDatabase::To(CopyDatabaseArgument {
            database_name: ObjectName::from(vec![
                Ident::new(&req.catalog_name),
                Ident::new(&req.schema_name),
            ]),
            with: req.with.clone().into(),
            connection: req.connection.clone().into(),
            location: req.location.clone(),
        })));
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission_with_context(
                ctx.current_user(),
                PermissionReq::SqlStatement(&stmt),
                Some(&ctx.current_schema()),
            )
            .context(PermissionSnafu)?;
        let tables = self
            .statement_executor
            .capture_database_export_tables(&req, names, ctx)
            .await?;
        let targets = PermissionTableTargets::resolved(
            tables
                .iter()
                .map(|table| {
                    let info = table.table_info();
                    PermissionTableTarget::new(&info.catalog_name, &info.schema_name, &info.name)
                })
                .collect(),
        );
        self.check_table_permission(ctx, PermissionReq::SqlStatement(&stmt), targets.clone())
            .context(PermissionSnafu)?;
        // COPY is classified as a write by existing permission checkers.
        self.check_table_permission(
            ctx,
            PermissionReq::Action(PermissionAction::read("database.export")),
            targets,
        )
        .context(PermissionSnafu)?;
        Ok(self
            .statement_executor
            .prepare_database_export(req, tables)
            .await?)
    }
}
