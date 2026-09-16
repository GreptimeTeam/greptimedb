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

use auth::{
    PermissionAction, PermissionChecker, PermissionCheckerRef, PermissionReq,
    PermissionTableTarget, PermissionTableTargets,
};
use operator::statement::export_database::PreparedDatabaseExport;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::ast::{Ident, ObjectName};
use sql::statements::copy::{Copy, CopyDatabase, CopyDatabaseArgument};
use sql::statements::statement::Statement;
use table::requests::CopyDatabaseRequest;

use crate::error::{PermissionSnafu, Result};
use crate::instance::Instance;

impl Instance {
    // PR04b will connect this boundary to request ownership and V2 metadata.
    #[allow(dead_code)]
    pub(crate) async fn prepare_database_export(
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
