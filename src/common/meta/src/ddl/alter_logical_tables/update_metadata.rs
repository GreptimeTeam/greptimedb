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

use common_grpc_expr::alter_expr_to_request;
use snafu::{OptionExt, ResultExt};
use table::metadata::TableInfo;

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::ddl::alter_logical_tables::executor::AlterLogicalTablesExecutor;
use crate::ddl::utils::raw_table_info;
use crate::ddl::utils::table_info::batch_update_table_info_values;
use crate::error;
use crate::error::{ConvertAlterTableRequestSnafu, Result, TableInfoNotFoundSnafu};
use crate::key::DeserializedValueWithBytes;
use crate::key::table_info::TableInfoValue;
use crate::rpc::ddl::AlterTableTask;
use crate::rpc::router::region_distribution;

impl AlterLogicalTablesProcedure {
    pub(crate) async fn update_physical_table_metadata(&mut self) -> Result<()> {
        self.fetch_physical_table_route_if_non_exist().await?;
        // Safety: must exist.
        let physical_table_info = self.data.physical_table_info.as_ref().unwrap();
        // Safety: fetched in `fetch_physical_table_route_if_non_exist`.
        let physical_table_route = self.physical_table_route.as_ref().unwrap();
        let region_distribution = region_distribution(&physical_table_route.region_routes);

        // Updates physical table's metadata.
        AlterLogicalTablesExecutor::on_alter_metadata(
            self.data.physical_table_id,
            &self.context.table_metadata_manager,
            physical_table_info,
            region_distribution,
            &self.data.physical_columns,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn update_logical_tables_metadata(&mut self) -> Result<()> {
        let physical_table_info = self
            .context
            .table_metadata_manager
            .table_info_manager()
            .get(self.data.physical_table_id)
            .await?
            .with_context(|| TableInfoNotFoundSnafu {
                table: format!("table id - {}", self.data.physical_table_id),
            })?;
        let table_info_values = self.build_update_metadata(&physical_table_info.table_info)?;
        batch_update_table_info_values(&self.context.table_metadata_manager, table_info_values)
            .await
    }

    pub(crate) fn build_update_metadata(
        &self,
        physical_table_info: &TableInfo,
    ) -> Result<Vec<(DeserializedValueWithBytes<TableInfoValue>, TableInfo)>> {
        let mut table_info_values_to_update = Vec::with_capacity(self.data.tasks.len());
        for (task, table) in self
            .data
            .tasks
            .iter()
            .zip(self.data.table_info_values.iter())
        {
            table_info_values_to_update.push(self.build_new_table_info(
                task,
                table,
                physical_table_info,
            )?);
        }

        Ok(table_info_values_to_update)
    }

    fn build_new_table_info(
        &self,
        task: &AlterTableTask,
        table: &DeserializedValueWithBytes<TableInfoValue>,
        physical_table_info: &TableInfo,
    ) -> Result<(DeserializedValueWithBytes<TableInfoValue>, TableInfo)> {
        // Builds new_meta
        // Validate the persisted mapping before the alter builder or sorting can discard it.
        // Incomplete legacy mappings are restored; complete conflicts fail closed.
        let table_info =
            validate_original_logical_table_mapping(physical_table_info, &table.table_info)?;
        let table_ref = task.table_ref();
        let request = alter_expr_to_request(
            table.table_info.ident.table_id,
            task.alter_table.clone(),
            Some(&table_info.meta),
        )
        .context(ConvertAlterTableRequestSnafu)?;
        let new_meta = table_info
            .meta
            .builder_with_alter_kind(table_ref.table, &request.alter_kind)
            .context(error::TableSnafu)?
            .build()
            .with_context(|_| error::BuildTableMetaSnafu {
                table_name: table_ref.table,
            })?;
        let version = table_info.ident.version + 1;
        let mut new_table = table_info;
        new_table.meta = new_meta;
        new_table.ident.version = version;

        new_table.sort_columns();
        raw_table_info::populate_logical_table_column_ids(physical_table_info, &mut new_table)?;

        Ok((table.clone(), new_table))
    }
}

fn validate_original_logical_table_mapping(
    physical_table_info: &TableInfo,
    table_info: &TableInfo,
) -> Result<TableInfo> {
    let mut table_info = table_info.clone();
    raw_table_info::populate_logical_table_column_ids(physical_table_info, &mut table_info)?;
    Ok(table_info)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::schema::Schema;

    use super::validate_original_logical_table_mapping;
    use crate::ddl::test_util::{
        test_create_logical_table_task, test_create_physical_table_task, test_physical_table_info,
    };
    use crate::ddl::utils::raw_table_info::populate_logical_table_column_ids;

    #[test]
    fn test_populate_logical_column_ids_after_drop() {
        let physical_table_info =
            test_physical_table_info(test_create_physical_table_task("phy").table_info);
        let mut logical_table_info = test_create_logical_table_task("logical").table_info;
        populate_logical_table_column_ids(&physical_table_info, &mut logical_table_info).unwrap();

        let columns = logical_table_info
            .meta
            .schema
            .column_schemas()
            .iter()
            .filter(|column| column.name != "cpu")
            .cloned()
            .collect();
        logical_table_info.meta.schema = Arc::new(Schema::new(columns));
        logical_table_info.sort_columns();
        assert!(logical_table_info.meta.column_ids.is_empty());

        populate_logical_table_column_ids(&physical_table_info, &mut logical_table_info).unwrap();
        assert_eq!(logical_table_info.meta.column_ids, vec![2, 0]);
    }

    #[test]
    fn test_original_mapping_conflict_is_rejected_before_alter_builder() {
        let physical_table_info =
            test_physical_table_info(test_create_physical_table_task("phy").table_info);
        let mut logical_table_info = test_create_logical_table_task("logical").table_info;
        populate_logical_table_column_ids(&physical_table_info, &mut logical_table_info).unwrap();
        logical_table_info.meta.column_ids[0] += 100;

        assert!(
            validate_original_logical_table_mapping(&physical_table_info, &logical_table_info,)
                .is_err()
        );
    }
}
