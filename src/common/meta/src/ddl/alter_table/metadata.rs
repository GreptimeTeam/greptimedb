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

use common_catalog::format_full_table_name;
use snafu::OptionExt;

use crate::ddl::alter_table::AlterTableProcedure;
use crate::error::{self, Result};

impl AlterTableProcedure {
    /// Fetches the table info.
    pub(crate) async fn fill_table_info(&mut self) -> Result<()> {
        let table_id = self.data.table_id();
        let alter_expr = &self.data.task.alter_table;
        let catalog = &alter_expr.catalog_name;
        let schema = &alter_expr.schema_name;
        let table_name = &alter_expr.table_name;

        let table_info_value = self
            .context
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await?
            .with_context(|| error::TableNotFoundSnafu {
                table_name: format_full_table_name(catalog, schema, table_name),
            })?;
        self.data.table_info_value = Some(table_info_value);
        Ok(())
    }
}
