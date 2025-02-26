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

use api::v1::alter_table_expr::Kind;
use api::v1::RenameTable;
use common_catalog::format_full_table_name;
use snafu::ensure;

use crate::ddl::alter_table::AlterTableProcedure;
use crate::error::{self, Result};
use crate::key::table_name::TableNameKey;

impl AlterTableProcedure {
    /// Checks:
    /// - The new table name doesn't exist (rename).
    /// - Table exists.
    pub(crate) async fn check_alter(&self) -> Result<()> {
        let alter_expr = &self.data.task.alter_table;
        let catalog = &alter_expr.catalog_name;
        let schema = &alter_expr.schema_name;
        let table_name = &alter_expr.table_name;
        // Safety: Checked in `AlterTableProcedure::new`.
        let alter_kind = self.data.task.alter_table.kind.as_ref().unwrap();

        let manager = &self.context.table_metadata_manager;
        if let Kind::RenameTable(RenameTable { new_table_name }) = alter_kind {
            let new_table_name_key = TableNameKey::new(catalog, schema, new_table_name);
            let exists = manager
                .table_name_manager()
                .exists(new_table_name_key)
                .await?;
            ensure!(
                !exists,
                error::TableAlreadyExistsSnafu {
                    table_name: format_full_table_name(catalog, schema, new_table_name),
                }
            )
        }

        let table_name_key = TableNameKey::new(catalog, schema, table_name);
        let exists = manager.table_name_manager().exists(table_name_key).await?;
        ensure!(
            exists,
            error::TableNotFoundSnafu {
                table_name: format_full_table_name(catalog, schema, &alter_expr.table_name),
            }
        );

        Ok(())
    }
}
