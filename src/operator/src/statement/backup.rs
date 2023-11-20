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

use common_datasource::file_format::Format;
use common_query::Output;
use common_telemetry::{info, tracing};
use session::context::QueryContextBuilder;
use snafu::{ensure, ResultExt};
use table::requests::{CopyDatabaseRequest, CopyDirection, CopyTableRequest};

use crate::error;
use crate::error::{CatalogSnafu, InvalidCopyParameterSnafu};
use crate::statement::StatementExecutor;

pub(crate) const COPY_DATABASE_TIME_START_KEY: &str = "start_time";
pub(crate) const COPY_DATABASE_TIME_END_KEY: &str = "end_time";

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn copy_database(&self, req: CopyDatabaseRequest) -> error::Result<Output> {
        // location must end with / so that every table is exported to a file.
        ensure!(
            req.location.ends_with('/'),
            InvalidCopyParameterSnafu {
                key: "location",
                value: req.location,
            }
        );

        info!(
            "Copy database {}.{}, dir: {},. time: {:?}",
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
}
