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

//! Scan entry point: builds the remote `SELECT`, freezes the column mapping,
//! and hands the remote connection over to the streaming task.

use std::sync::Arc;

use common_recordbatch::SendableRecordBatchStream;
use snafu::{ResultExt, ensure};
use store_api::storage::ScanRequest;

use crate::connection;
use crate::error::{MissingRemoteColumnsSnafu, ProjectSchemaSnafu, Result};
use crate::mysql_stream;
use crate::region::RelationalRegion;

impl RelationalRegion {
    pub async fn query(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let options = self.relational_options()?;

        // A manifest written before the `columns` field existed decodes to an
        // empty list. The frozen remote column list is the only unambiguous way
        // to tell a real remote column from a synthetic one, so fail fast
        // rather than fall back to a name heuristic (PR3 问题 1).
        ensure!(
            !options.columns.is_empty(),
            MissingRemoteColumnsSnafu
        );

        // Eagerly parse the DSN and resolve the host, so a bad connection
        // surfaces here rather than as a first-batch stream error (PR3 问题 2).
        let opts = connection::build_mysql_opts(&options.connection).await?;

        // Honor the requested projection: the scan stream must emit the
        // projected schema — DataFusion's `MergeScan` validates it against the
        // plan and does not project for us. The SQL still selects only the
        // remote columns the projection references (synthetic columns excluded).
        let scan_schema = match request.projection.as_deref() {
            Some(projection) => Arc::new(
                self.metadata
                    .schema
                    .try_project(projection)
                    .context(ProjectSchemaSnafu)?,
            ),
            None => self.metadata.schema.clone(),
        };
        let column_schemas = scan_schema.column_schemas().to_vec();

        // Freeze the remote column mapping. A region column whose name is in
        // the frozen `columns` list is a real remote column; any other column is
        // synthetic. `scan_to_remote` and the SQL `SELECT` list are derived from
        // this single list in the same region-schema order, so they cannot drift.
        let mut select_names: Vec<String> = Vec::new();
        let scan_to_remote: Vec<Option<usize>> = column_schemas
            .iter()
            .map(|column| {
                if options.columns.contains(&column.name) {
                    let idx = select_names.len();
                    select_names.push(column.name.clone());
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();

        let sql = connection::build_select_sql(&options.remote_table, &opts, &select_names)?;

        Ok(mysql_stream::create_stream(
            opts,
            options.connection,
            options.remote_table,
            sql,
            scan_schema,
            column_schemas,
            scan_to_remote,
        ))
    }
}
