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

use std::path::Path;

use common_datasource::file_format::Format;
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::build_backend;
use common_query::Output;
use common_telemetry::{debug, info, tracing};
use object_store::Entry;
use regex::Regex;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{ensure, ResultExt};
use table::requests::{CopyDatabaseRequest, CopyDirection, CopyTableRequest};

use crate::error;
use crate::error::{CatalogSnafu, InvalidCopyParameterSnafu};
use crate::statement::StatementExecutor;

pub(crate) const COPY_DATABASE_TIME_START_KEY: &str = "start_time";
pub(crate) const COPY_DATABASE_TIME_END_KEY: &str = "end_time";

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn copy_database_to(&self, req: CopyDatabaseRequest) -> error::Result<Output> {
        // location must end with / so that every table is exported to a file.
        ensure!(
            req.location.ends_with('/'),
            InvalidCopyParameterSnafu {
                key: "location",
                value: req.location,
            }
        );

        info!(
            "Copy database {}.{} to dir: {},. time: {:?}",
            req.catalog_name, req.schema_name, req.location, req.time_range
        );
        let table_names = self
            .catalog_manager
            .table_names(&req.catalog_name, &req.schema_name)
            .await
            .context(CatalogSnafu)?;

        let suffix = Format::try_from(&req.with)
            .context(error::ParseFileFormatSnafu)?
            .suffix();

        let mut exported_rows = 0;
        for table_name in table_names {
            // TODO(hl): remove this hardcode once we've removed numbers table.
            if table_name == "numbers" {
                continue;
            }
            let mut table_file = req.location.clone();
            table_file.push_str(&table_name);
            table_file.push_str(suffix);
            info!(
                "Copy table: {}.{}.{} to {}",
                req.catalog_name, req.schema_name, table_name, table_file
            );

            let exported = self
                .copy_table_to(
                    CopyTableRequest {
                        catalog_name: req.catalog_name.clone(),
                        schema_name: req.schema_name.clone(),
                        table_name,
                        location: table_file,
                        with: req.with.clone(),
                        connection: req.connection.clone(),
                        pattern: None,
                        direction: CopyDirection::Export,
                        timestamp_range: req.time_range,
                    },
                    QueryContextBuilder::default().build(),
                )
                .await?;
            exported_rows += exported;
        }
        Ok(Output::AffectedRows(exported_rows))
    }

    /// Copies database from a given directory.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn copy_database_from(
        &self,
        req: CopyDatabaseRequest,
        ctx: QueryContextRef,
    ) -> error::Result<Output> {
        // location must end with /
        ensure!(
            req.location.ends_with('/'),
            InvalidCopyParameterSnafu {
                key: "location",
                value: req.location,
            }
        );

        info!(
            "Copy database {}.{} from dir: {}, time: {:?}",
            req.catalog_name, req.schema_name, req.location, req.time_range
        );
        let suffix = Format::try_from(&req.with)
            .context(error::ParseFileFormatSnafu)?
            .suffix();

        let entries = list_files_to_copy(&req, suffix).await?;

        let mut rows_inserted = 0;
        for e in entries {
            let table_name = parse_file_name_to_copy(&e)?;
            let req = CopyTableRequest {
                catalog_name: req.catalog_name.clone(),
                schema_name: req.schema_name.clone(),
                table_name,
                location: format!("{}/{}", req.location, e.path()),
                with: req.with.clone(),
                connection: req.connection.clone(),
                pattern: None,
                direction: CopyDirection::Import,
                timestamp_range: None,
            };
            debug!("Copy table, arg: {:?}", req);
            rows_inserted += self.copy_table_from(req, ctx.clone()).await?;
        }
        Ok(Output::AffectedRows(rows_inserted))
    }
}

/// Parses table names from files' names.
fn parse_file_name_to_copy(e: &Entry) -> error::Result<String> {
    Path::new(e.name())
        .file_stem()
        .and_then(|os_str| os_str.to_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            error::InvalidTableNameSnafu {
                table_name: e.name().to_string(),
            }
            .build()
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
