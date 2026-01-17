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
use table::metadata::TableId;
use table::table_name::TableName;

use crate::cache_invalidator::Context;
use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::utils::raw_table_info;
use crate::error::{Result, TableInfoNotFoundSnafu};
use crate::instruction::CacheIdent;

impl CreateLogicalTablesProcedure {
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
