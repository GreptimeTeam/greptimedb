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

use std::str::FromStr;
use std::sync::Arc;

use client::{Output, OutputData, OutputMeta};
use common_catalog::format_full_table_name;
use common_datasource::file_format::Format;
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::{LocalFileAccess, build_backend, build_backend_for_write};
use common_telemetry::{debug, error, info, tracing};
use futures::future::try_join_all;
use object_store::Entry;
use regex::Regex;
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::requests::{CopyDatabaseRequest, CopyDirection, CopyTableRequest};
use tokio::sync::Semaphore;

use crate::error;
use crate::statement::StatementExecutor;
use crate::statement::database_copy::{
    DatabaseExportFile, database_import_source, parse_parallelism_from_option_map,
    validate_database_directory, validate_database_export_layout,
};

pub(crate) const COPY_DATABASE_TIME_START_KEY: &str = "start_time";
pub(crate) const COPY_DATABASE_TIME_END_KEY: &str = "end_time";
pub(crate) const CONTINUE_ON_ERROR_KEY: &str = "continue_on_error";

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn copy_database_to(
        &self,
        req: CopyDatabaseRequest,
        ctx: QueryContextRef,
    ) -> error::Result<Output> {
        validate_database_export_layout(&req.with)?;
        validate_database_directory(&req.location)?;
        build_backend_for_write(&req.location, &req.connection, &self.local_file_access)
            .await
            .context(error::BuildBackendSnafu)?;

        let parallelism = parse_parallelism_from_option_map(&req.with);
        info!(
            "Copy database {}.{} to dir: {}, time: {:?}, parallelism: {}",
            req.catalog_name, req.schema_name, req.location, req.time_range, parallelism
        );
        let tables = self
            .capture_database_export_tables(&req, None, &ctx)
            .await?;
        let num_tables = tables.len();

        let suffix = Format::try_from(&req.with)
            .context(error::ParseFileFormatSnafu)?
            .suffix();

        let mut tasks = Vec::with_capacity(num_tables);
        let semaphore = Arc::new(Semaphore::new(parallelism));

        for (i, table) in tables.into_iter().enumerate() {
            let table_name = table.table_info().name.clone();
            let semaphore_moved = semaphore.clone();
            let table_file = DatabaseExportFile::new(&req.location, &table_name, suffix)?.location;
            let table_no = i + 1;
            let moved_ctx = ctx.clone();
            let full_table_name =
                format_full_table_name(&req.catalog_name, &req.schema_name, &table_name);
            let copy_table_req = CopyTableRequest {
                catalog_name: req.catalog_name.clone(),
                schema_name: req.schema_name.clone(),
                table_name,
                location: table_file.clone(),
                with: req.with.clone(),
                connection: req.connection.clone(),
                pattern: None,
                direction: CopyDirection::Export,
                timestamp_range: req.time_range,
                limit: None,
            };

            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                info!(
                    "Copy table({}/{}): {} to {}",
                    table_no, num_tables, full_table_name, table_file
                );
                self.copy_captured_table_to(table, copy_table_req, moved_ctx)
                    .await
            });
        }

        let results = try_join_all(tasks).await?;
        let exported_rows = results.into_iter().sum();
        Ok(Output::new_with_affected_rows(exported_rows))
    }

    /// Imports data to database from a given location and returns total rows imported.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn copy_database_from(
        &self,
        req: CopyDatabaseRequest,
        ctx: QueryContextRef,
    ) -> error::Result<Output> {
        if let Some(layout) = req.with.get("metric_data_layout") {
            return error::InvalidCopyParameterSnafu {
                key: "metric_data_layout",
                value: layout,
            }
            .fail();
        }
        validate_database_directory(&req.location)?;

        let parallelism = parse_parallelism_from_option_map(&req.with);
        info!(
            "Copy database {}.{} from dir: {}, time: {:?}, parallelism: {}",
            req.catalog_name, req.schema_name, req.location, req.time_range, parallelism
        );
        let suffix = Format::try_from(&req.with)
            .context(error::ParseFileFormatSnafu)?
            .suffix();

        let entries = list_files_to_copy(&req, suffix, &self.local_file_access).await?;

        let continue_on_error = req
            .with
            .get(CONTINUE_ON_ERROR_KEY)
            .and_then(|v| bool::from_str(v).ok())
            .unwrap_or(false);

        let mut tasks = Vec::with_capacity(entries.len());
        let semaphore = Arc::new(Semaphore::new(parallelism));

        for e in entries {
            let (table_name, location) = match database_import_source(&req.location, e.path()) {
                Ok(source) => source,
                Err(err) => {
                    if continue_on_error {
                        error!(err; "Failed to import table from file: {:?}", e);
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            };

            let req = CopyTableRequest {
                catalog_name: req.catalog_name.clone(),
                schema_name: req.schema_name.clone(),
                table_name: table_name.clone(),
                location,
                with: req.with.clone(),
                connection: req.connection.clone(),
                pattern: None,
                direction: CopyDirection::Import,
                timestamp_range: None,
                limit: None,
            };
            let moved_ctx = ctx.clone();
            let moved_table_name = table_name.clone();
            let moved_semaphore = semaphore.clone();
            tasks.push(async move {
                let _permit = moved_semaphore.acquire().await.unwrap();
                debug!("Copy table, arg: {:?}", req);
                match self.copy_table_from(req, moved_ctx).await {
                    Ok(o) => {
                        let (rows, cost) = o.extract_rows_and_cost();
                        Ok((rows, cost))
                    }
                    Err(err) => {
                        if continue_on_error {
                            error!(err; "Failed to import file to table: {}", moved_table_name);
                            Ok((0, 0))
                        } else {
                            Err(err)
                        }
                    }
                }
            });
        }

        let results = try_join_all(tasks).await?;
        let (rows_inserted, insert_cost) = results
            .into_iter()
            .fold((0, 0), |(acc_rows, acc_cost), (rows, cost)| {
                (acc_rows + rows, acc_cost + cost)
            });

        Ok(Output::new(
            OutputData::AffectedRows(rows_inserted),
            OutputMeta::new_with_cost(insert_cost),
        ))
    }
}

/// Lists all files with expected suffix that can be imported to database.
async fn list_files_to_copy(
    req: &CopyDatabaseRequest,
    suffix: &str,
    local_file_access: &LocalFileAccess,
) -> error::Result<Vec<Entry>> {
    let object_store = build_backend(&req.location, &req.connection, local_file_access)
        .await
        .context(error::BuildBackendSnafu)?;

    let pattern = Regex::try_from(format!(".*{}", suffix)).context(error::BuildRegexSnafu)?;
    let lister = Lister::new(
        object_store.clone(),
        Source::Dir,
        "/".to_string(),
        Some(pattern),
    );
    lister.list().await.context(error::ListObjectsSnafu)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use common_datasource::object_store::LocalFileAccess;
    use object_store::ObjectStore;
    use object_store::services::Fs;
    use object_store::util::normalize_dir;
    #[cfg(not(windows))]
    use path_slash::PathExt;
    use table::requests::CopyDatabaseRequest;

    use crate::statement::copy_database::list_files_to_copy;
    use crate::statement::database_copy::database_import_source;

    #[tokio::test]
    async fn test_list_files_and_parse_table_name() {
        let dir = common_test_util::temp_dir::create_temp_dir("test_list_files_to_copy");
        let store_dir = normalize_dir(dir.path().to_str().unwrap());
        let builder = Fs::default().root(&store_dir);
        let object_store = ObjectStore::new(builder).unwrap();
        object_store.write("a.parquet", "").await.unwrap();
        object_store.write("b.parquet", "").await.unwrap();
        object_store.write("c.csv", "").await.unwrap();
        object_store.write("d", "").await.unwrap();
        object_store.write("e.f.parquet", "").await.unwrap();

        #[cfg(not(windows))]
        let location = normalize_dir(&dir.path().to_slash().unwrap());
        #[cfg(windows)]
        let location = format!("{}\\", dir.path().display());
        let request = CopyDatabaseRequest {
            catalog_name: "catalog_0".to_string(),
            schema_name: "schema_0".to_string(),
            location,
            with: [("FORMAT".to_string(), "parquet".to_string())]
                .into_iter()
                .collect(),
            connection: Default::default(),
            time_range: None,
        };
        let local_file_access = LocalFileAccess::sandboxed(dir.path()).unwrap();
        let listed = list_files_to_copy(&request, ".parquet", &local_file_access)
            .await
            .unwrap()
            .into_iter()
            .map(|e| {
                database_import_source(&request.location, e.path())
                    .unwrap()
                    .0
            })
            .collect::<HashSet<_>>();

        assert_eq!(
            ["a".to_string(), "b".to_string(), "e.f".to_string()]
                .into_iter()
                .collect::<HashSet<_>>(),
            listed
        );
    }
}
