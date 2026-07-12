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

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use client::{Output, OutputData, OutputMeta};
use common_catalog::format_full_table_name;
use common_datasource::file_format::Format;
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::build_backend;
use common_stat::get_total_cpu_cores;
use common_telemetry::{debug, error, info, tracing};
use futures::future::try_join_all;
use object_store::Entry;
use regex::Regex;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME};
use table::requests::{CopyDatabaseRequest, CopyDirection, CopyTableRequest};
use table::table_reference::TableReference;
use tokio::sync::Semaphore;

use crate::error;
use crate::error::{CatalogSnafu, InvalidCopyDatabasePathSnafu};
use crate::statement::StatementExecutor;

pub(crate) const COPY_DATABASE_TIME_START_KEY: &str = "start_time";
pub(crate) const COPY_DATABASE_TIME_END_KEY: &str = "end_time";
pub(crate) const CONTINUE_ON_ERROR_KEY: &str = "continue_on_error";
pub(crate) const PARALLELISM_KEY: &str = "parallelism";

/// Get parallelism from options, default to total CPU cores.
fn parse_parallelism_from_option_map(options: &HashMap<String, String>) -> usize {
    options
        .get(PARALLELISM_KEY)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(get_total_cpu_cores)
        .max(1)
}

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn copy_database_to(
        &self,
        req: CopyDatabaseRequest,
        ctx: QueryContextRef,
    ) -> error::Result<Output> {
        // location must end with / so that every table is exported to a file.
        ensure!(
            req.location.ends_with('/'),
            InvalidCopyDatabasePathSnafu {
                value: req.location,
            }
        );

        let parallelism = parse_parallelism_from_option_map(&req.with);
        info!(
            "Copy database {}.{} to dir: {}, time: {:?}, parallelism: {}",
            req.catalog_name, req.schema_name, req.location, req.time_range, parallelism
        );
        let table_names = self
            .catalog_manager
            .table_names(&req.catalog_name, &req.schema_name, Some(&ctx))
            .await
            .context(CatalogSnafu)?;
        let num_tables = table_names.len();

        let suffix = Format::try_from(&req.with)
            .context(error::ParseFileFormatSnafu)?
            .suffix();

        let mut tasks = Vec::with_capacity(num_tables);
        let semaphore = Arc::new(Semaphore::new(parallelism));

        for (i, table_name) in table_names.into_iter().enumerate() {
            let table = self
                .get_table(&TableReference {
                    catalog: &req.catalog_name,
                    schema: &req.schema_name,
                    table: &table_name,
                })
                .await?;
            // Only base tables, ignores views and temporary tables.
            if table.table_type() != table::metadata::TableType::Base {
                continue;
            }
            // Ignores physical tables of metric engine.
            if table.table_info().meta.engine == METRIC_ENGINE_NAME
                && !table
                    .table_info()
                    .meta
                    .options
                    .extra_options
                    .contains_key(LOGICAL_TABLE_METADATA_KEY)
            {
                continue;
            }

            let semaphore_moved = semaphore.clone();
            let mut table_file = req.location.clone();
            table_file.push_str(&table_name);
            table_file.push_str(suffix);
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
                self.copy_table_to(copy_table_req, moved_ctx).await
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
        // location must end with /
        ensure!(
            req.location.ends_with('/'),
            InvalidCopyDatabasePathSnafu {
                value: req.location,
            }
        );

        let parallelism = parse_parallelism_from_option_map(&req.with);
        info!(
            "Copy database {}.{} from dir: {}, time: {:?}, parallelism: {}",
            req.catalog_name, req.schema_name, req.location, req.time_range, parallelism
        );
        let suffix = Format::try_from(&req.with)
            .context(error::ParseFileFormatSnafu)?
            .suffix();

        let entries = list_files_to_copy(&req, suffix).await?;

        let continue_on_error = req
            .with
            .get(CONTINUE_ON_ERROR_KEY)
            .and_then(|v| bool::from_str(v).ok())
            .unwrap_or(false);

        let mut tasks = Vec::with_capacity(entries.len());
        let semaphore = Arc::new(Semaphore::new(parallelism));

        for e in entries {
            let table_name = match parse_file_name_to_copy(&e) {
                Ok(table_name) => table_name,
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
                location: format!("{}/{}", req.location, e.path()),
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

/// Parses table names from files' names.
fn parse_file_name_to_copy(e: &Entry) -> error::Result<String> {
    Path::new(e.name())
        .file_stem()
        .and_then(|os_str| os_str.to_str())
        .map(|s| s.to_string())
        .context(error::InvalidTableNameSnafu {
            table_name: e.name().to_string(),
        })
}

/// Lists all files with expected suffix that can be imported to database.
async fn list_files_to_copy(req: &CopyDatabaseRequest, suffix: &str) -> error::Result<Vec<Entry>> {
    let object_store =
        build_backend(&req.location, &req.connection).context(error::BuildBackendSnafu)?;

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
    use std::collections::{HashMap, HashSet};

    use common_stat::get_total_cpu_cores;
    use object_store::ObjectStore;
    use object_store::services::Fs;
    use object_store::util::normalize_dir;
    use path_slash::PathExt;
    use table::requests::CopyDatabaseRequest;

    use crate::statement::copy_database::{
        list_files_to_copy, parse_file_name_to_copy, parse_parallelism_from_option_map,
    };

    #[tokio::test]
    async fn test_list_files_and_parse_table_name() {
        let dir = common_test_util::temp_dir::create_temp_dir("test_list_files_to_copy");
        let store_dir = normalize_dir(dir.path().to_str().unwrap());
        let builder = Fs::default().root(&store_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();
        object_store.write("a.parquet", "").await.unwrap();
        object_store.write("b.parquet", "").await.unwrap();
        object_store.write("c.csv", "").await.unwrap();
        object_store.write("d", "").await.unwrap();
        object_store.write("e.f.parquet", "").await.unwrap();

        let location = normalize_dir(&dir.path().to_slash().unwrap());
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
        let listed = list_files_to_copy(&request, ".parquet")
            .await
            .unwrap()
            .into_iter()
            .map(|e| parse_file_name_to_copy(&e).unwrap())
            .collect::<HashSet<_>>();

        assert_eq!(
            ["a".to_string(), "b".to_string(), "e.f".to_string()]
                .into_iter()
                .collect::<HashSet<_>>(),
            listed
        );
    }

    #[test]
    fn test_parse_parallelism_from_option_map() {
        let options = HashMap::new();
        assert_eq!(
            parse_parallelism_from_option_map(&options),
            get_total_cpu_cores()
        );

        let options = HashMap::from([("parallelism".to_string(), "0".to_string())]);
        assert_eq!(parse_parallelism_from_option_map(&options), 1);
    }
}
