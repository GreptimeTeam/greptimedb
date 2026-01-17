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

use std::ops::Deref;

use common_telemetry::{info, warn};
use itertools::Itertools;
use snafu::OptionExt;
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;
use store_api::storage::consts::ReservedColumnId;
use table::metadata::{RawTableInfo, TableId};
use table::table_name::TableName;

use crate::cache_invalidator::Context;
use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::utils::raw_table_info;
use crate::error::{Result, TableInfoNotFoundSnafu};
use crate::instruction::CacheIdent;

impl CreateLogicalTablesProcedure {
    pub(crate) fn add_tsid_column_to_logical_tables(&mut self) {
        for (task, table_id_already_exists) in self
            .data
            .tasks
            .iter_mut()
            .zip(self.data.table_ids_already_exists.iter())
        {
            if table_id_already_exists.is_some() {
                continue;
            }
            add_tsid_column_to_raw_table_info(&mut task.table_info);
        }
    }

    pub(crate) async fn update_physical_table_metadata(&mut self) -> Result<()> {
        if self.data.physical_columns.is_empty() {
            warn!(
                "No physical columns found, leaving the physical table's schema unchanged when creating logical tables"
            );
            return Ok(());
        }

        // Fetches old physical table's info
        let physical_table_info = self
            .context
            .table_metadata_manager
            .table_info_manager()
            .get(self.data.physical_table_id)
            .await?
            .with_context(|| TableInfoNotFoundSnafu {
                table: format!("table id - {}", self.data.physical_table_id),
            })?;

        // Generates new table info
        let raw_table_info = physical_table_info.deref().table_info.clone();

        let new_table_info = raw_table_info::build_new_physical_table_info(
            raw_table_info,
            &self.data.physical_columns,
        );

        let physical_table_name = TableName::new(
            &new_table_info.catalog_name,
            &new_table_info.schema_name,
            &new_table_info.name,
        );

        // Update physical table's metadata and we don't need to touch per-region settings.
        self.context
            .table_metadata_manager
            .update_table_info(&physical_table_info, None, new_table_info)
            .await?;

        // Invalid physical table cache
        self.context
            .cache_invalidator
            .invalidate(
                &Context::default(),
                &[
                    CacheIdent::TableId(self.data.physical_table_id),
                    CacheIdent::TableName(physical_table_name),
                ],
            )
            .await?;

        Ok(())
    }

    pub(crate) async fn create_logical_tables_metadata(&mut self) -> Result<Vec<TableId>> {
        let remaining_tasks = self.data.remaining_tasks();
        let num_tables = remaining_tasks.len();

        if num_tables > 0 {
            let chunk_size = self
                .context
                .table_metadata_manager
                .create_logical_tables_metadata_chunk_size();
            if num_tables > chunk_size {
                let chunks = remaining_tasks
                    .into_iter()
                    .chunks(chunk_size)
                    .into_iter()
                    .map(|chunk| chunk.collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                for chunk in chunks {
                    self.context
                        .table_metadata_manager
                        .create_logical_tables_metadata(chunk)
                        .await?;
                }
            } else {
                self.context
                    .table_metadata_manager
                    .create_logical_tables_metadata(remaining_tasks)
                    .await?;
            }
        }

        // The `table_id` MUST be collected after the [Prepare::Prepare],
        // ensures the all `table_id`s have been allocated.
        let table_ids = self
            .data
            .tasks
            .iter()
            .map(|task| task.table_info.ident.table_id)
            .collect::<Vec<_>>();

        info!(
            "Created {num_tables} tables {table_ids:?} metadata for physical table {}",
            self.data.physical_table_id
        );

        Ok(table_ids)
    }
}

fn add_tsid_column_to_raw_table_info(table_info: &mut RawTableInfo) {
    if table_info
        .meta
        .schema
        .column_schemas
        .iter()
        .any(|col| col.name == DATA_SCHEMA_TSID_COLUMN_NAME)
    {
        return;
    }

    let should_update_column_ids =
        table_info.meta.column_ids.len() == table_info.meta.schema.column_schemas.len();
    let column_index = table_info.meta.schema.column_schemas.len();
    table_info
        .meta
        .schema
        .column_schemas
        .push(datatypes::schema::ColumnSchema::new(
            DATA_SCHEMA_TSID_COLUMN_NAME,
            datatypes::prelude::ConcreteDataType::uint64_datatype(),
            false,
        ));
    table_info.meta.primary_key_indices.push(column_index);
    if should_update_column_ids {
        table_info.meta.column_ids.push(ReservedColumnId::tsid());
    }
    table_info.sort_columns();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::test_util::test_create_logical_table_task;

    #[test]
    fn add_tsid_preserves_column_ids_when_present() {
        let mut task = test_create_logical_table_task("foo");
        let schema_len = task.table_info.meta.schema.column_schemas.len();
        task.table_info.meta.column_ids = (0..schema_len as u32).collect();

        add_tsid_column_to_raw_table_info(&mut task.table_info);

        assert_eq!(
            task.table_info.meta.column_ids.len(),
            task.table_info.meta.schema.column_schemas.len()
        );
        let name_to_ids = task.table_info.name_to_ids().unwrap();
        assert_eq!(
            name_to_ids.get(DATA_SCHEMA_TSID_COLUMN_NAME),
            Some(&ReservedColumnId::tsid())
        );
    }
}
