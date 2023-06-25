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

use file_table_engine::table::immutable::ImmutableFileTableOptions;
use query::sql::prepare_immutable_file_table_files_and_schema;
use snafu::ResultExt;
use sql::statements::create::CreateExternalTable;
use table::engine::TableReference;
use table::metadata::TableId;
use table::requests::{CreateTableRequest, TableOptions, IMMUTABLE_TABLE_META_KEY};

use crate::error::{self, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn create_external_to_request(
        &self,
        table_id: TableId,
        stmt: CreateExternalTable,
        table_ref: &TableReference<'_>,
    ) -> Result<CreateTableRequest> {
        let mut options = stmt.options;

        let (files, schema) =
            prepare_immutable_file_table_files_and_schema(&options, &stmt.columns)
                .await
                .context(error::PrepareImmutableTableSnafu)?;

        let meta = ImmutableFileTableOptions { files };
        let _ = options.insert(
            IMMUTABLE_TABLE_META_KEY.to_string(),
            serde_json::to_string(&meta).context(error::EncodeJsonSnafu)?,
        );

        Ok(CreateTableRequest {
            id: table_id,
            catalog_name: table_ref.catalog.to_string(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
            desc: None,
            schema,
            region_numbers: vec![0],
            primary_key_indices: vec![0],
            create_if_not_exists: stmt.if_not_exists,
            table_options: TableOptions::try_from(&options)
                .context(error::UnrecognizedTableOptionSnafu)?,
            engine: stmt.engine,
        })
    }
}
