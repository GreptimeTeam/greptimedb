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

use clap::Args;
use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::key::table_name::TableNameManager;
use store_api::storage::TableId;

use crate::error::InvalidArgumentsSnafu;
use crate::metadata::control::utils::get_table_id_by_name;

/// Selects a table by id or by fully qualified name.
#[derive(Debug, Clone, Default, Args)]
pub(crate) struct TableSelector {
    /// The table id to select from the metadata store.
    #[clap(long)]
    table_id: Option<u32>,

    /// The table name to select from the metadata store.
    #[clap(long)]
    table_name: Option<String>,

    /// The schema name of the table.
    #[clap(long, default_value = DEFAULT_SCHEMA_NAME)]
    schema_name: String,

    /// The catalog name of the table.
    #[clap(long, default_value = DEFAULT_CATALOG_NAME)]
    catalog_name: String,
}

impl TableSelector {
    pub(crate) fn validate(&self) -> Result<(), BoxedError> {
        if matches!(
            (&self.table_id, &self.table_name),
            (Some(_), Some(_)) | (None, None)
        ) {
            return Err(BoxedError::new(
                InvalidArgumentsSnafu {
                    msg: "You must specify either --table-id or --table-name.",
                }
                .build(),
            ));
        }

        Ok(())
    }

    pub(crate) async fn resolve_table_id(
        &self,
        table_name_manager: &TableNameManager,
    ) -> Result<Option<TableId>, BoxedError> {
        if let Some(table_id) = self.table_id {
            return Ok(Some(table_id));
        }

        get_table_id_by_name(
            table_name_manager,
            &self.catalog_name,
            &self.schema_name,
            self.table_name
                .as_deref()
                .expect("validated table selector"),
        )
        .await
    }

    pub(crate) fn formatted_table_name(&self) -> String {
        format_full_table_name(
            &self.catalog_name,
            &self.schema_name,
            self.table_name.as_deref().unwrap_or_default(),
        )
    }
}

#[cfg(test)]
impl TableSelector {
    pub(crate) fn with_table_id(table_id: u32) -> Self {
        Self {
            table_id: Some(table_id),
            table_name: None,
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        }
    }
}
