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

//! Request-scoped database export preparation. The frontend authorizes the entire
//! captured selection before preparation; these trusted methods do not check ACLs.

use std::collections::{BTreeMap, HashSet};

use common_datasource::file_format::Format;
use common_datasource::object_store::build_backend_for_write;
use common_meta::key::table_route::TableRouteValue;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME};
use table::TableRef;
use table::metadata::TableType;
use table::requests::CopyDatabaseRequest;
use table::table_reference::TableReference;

use crate::error::{self, InvalidDatabaseExportSnafu, Result};
use crate::statement::StatementExecutor;
use crate::statement::copy_database::is_directory_location;
use crate::statement::export_logical_tables::LogicalTableExport;

/// A validated request-scoped selection, not a metadata snapshot or an ACL token.
pub struct PreparedDatabaseExport {
    request: CopyDatabaseRequest,
    jobs: Vec<DatabaseExportJob>,
    output_files: Vec<String>,
}

enum DatabaseExportJob {
    Ordinary(TableRef),
    Metric(LogicalTableExport),
}

impl StatementExecutor {
    /// Capture each selected data table once. Views, temporary and Metric physical
    /// tables do not have data outputs. `None` selects the whole schema.
    pub async fn capture_database_export_tables(
        &self,
        req: &CopyDatabaseRequest,
        names: Option<&[String]>,
        ctx: &QueryContextRef,
    ) -> Result<Vec<TableRef>> {
        let mut names = match names {
            Some(names) => names.to_vec(),
            None => self
                .catalog_manager
                .table_names(&req.catalog_name, &req.schema_name, Some(ctx))
                .await
                .context(error::CatalogSnafu)?,
        };
        names.sort();
        let mut tables = Vec::new();
        for name in names {
            let table = self
                .get_table(&TableReference::full(
                    &req.catalog_name,
                    &req.schema_name,
                    &name,
                ))
                .await?;
            let info = table.table_info();
            if table.table_type() == TableType::Base
                && (info.meta.engine != METRIC_ENGINE_NAME
                    || info
                        .meta
                        .options
                        .extra_options
                        .contains_key(LOGICAL_TABLE_METADATA_KEY))
            {
                tables.push(table);
            }
        }
        Ok(tables)
    }

    /// Validate all jobs and destinations before executing any query or writer.
    /// Callers must authorize every captured table before calling this method.
    pub async fn prepare_database_export(
        &self,
        req: CopyDatabaseRequest,
        tables: Vec<TableRef>,
    ) -> Result<PreparedDatabaseExport> {
        ensure!(
            is_directory_location(&req.location),
            error::InvalidCopyDatabasePathSnafu {
                value: &req.location,
            }
        );
        let format = Format::try_from(&req.with).context(error::ParseFileFormatSnafu)?;
        ensure!(
            matches!(format, Format::Parquet(_)),
            error::UnsupportedFormatSnafu { format }
        );
        let mut filenames = HashSet::new();
        let mut output_files = Vec::with_capacity(tables.len());
        let mut logical = Vec::new();
        let mut jobs = Vec::new();
        for table in tables {
            let info = table.table_info();
            let name = &info.name;
            ensure!(
                !name.is_empty()
                    && name != "."
                    && name != ".."
                    && !name.contains(['/', '\\', '\0', '?', '#', '%', ':'])
                    && filenames.insert(format!("{name}.parquet")),
                InvalidDatabaseExportSnafu {
                    reason: format!("unsafe or duplicate output name: {name}")
                }
            );
            ensure!(
                info.catalog_name == req.catalog_name
                    && info.schema_name == req.schema_name
                    && table.table_type() == TableType::Base,
                InvalidDatabaseExportSnafu {
                    reason: "expected base tables in the selected schema"
                }
            );
            output_files.push(format!("{}{name}.parquet", req.location));
            if info.meta.engine == METRIC_ENGINE_NAME {
                ensure!(
                    info.meta
                        .options
                        .extra_options
                        .contains_key(LOGICAL_TABLE_METADATA_KEY),
                    InvalidDatabaseExportSnafu {
                        reason: "expected a Metric logical table"
                    }
                );
                logical.push(table);
            } else {
                jobs.push(DatabaseExportJob::Ordinary(table));
            }
        }
        let ids = logical
            .iter()
            .map(|t| t.table_info().table_id())
            .collect::<Vec<_>>();
        let routes = self
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .batch_get(&ids)
            .await
            .context(error::TableMetadataManagerSnafu)?;
        let mut groups = BTreeMap::<_, Vec<TableRef>>::new();
        for (table, route) in logical.into_iter().zip(routes) {
            let Some(TableRouteValue::Logical(route)) = route else {
                return InvalidDatabaseExportSnafu {
                    reason: format!(
                        "missing or non-logical route for {}",
                        table.table_info().table_id()
                    ),
                }
                .fail();
            };
            groups
                .entry(route.physical_table_id())
                .or_default()
                .push(table);
        }
        let physical_ids = groups.keys().copied().collect::<Vec<_>>();
        let mut physical = self
            .catalog_manager
            .tables_by_ids(&req.catalog_name, &req.schema_name, &physical_ids)
            .await
            .context(error::CatalogSnafu)?
            .into_iter()
            .map(|t| (t.table_info().table_id(), t))
            .collect::<BTreeMap<_, _>>();
        for (id, tables) in groups {
            let table = physical
                .remove(&id)
                .with_context(|| InvalidDatabaseExportSnafu {
                    reason: format!("missing physical table {id} in the selected schema"),
                })?;
            jobs.push(DatabaseExportJob::Metric(LogicalTableExport::try_new(
                table, &tables,
            )?));
        }
        build_backend_for_write(&req.location, &req.connection, &self.local_file_access)
            .await
            .context(error::BuildBackendSnafu)?;
        Ok(PreparedDatabaseExport {
            request: req,
            jobs,
            output_files,
        })
    }
}
