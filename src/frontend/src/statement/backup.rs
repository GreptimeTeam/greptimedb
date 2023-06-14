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

use common_query::Output;
use snafu::{OptionExt, ResultExt};
use table::requests::{BackupDatabaseRequest, CopyDirection, CopyTableRequest};

use crate::error;
use crate::error::{CatalogNotFoundSnafu, CatalogSnafu, SchemaNotFoundSnafu};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    pub(crate) async fn backup_database(
        &self,
        req: BackupDatabaseRequest,
    ) -> error::Result<Output> {
        let schema = self
            .catalog_manager
            .catalog(&req.catalog_name)
            .await
            .context(CatalogSnafu)?
            .context(CatalogNotFoundSnafu {
                catalog_name: &req.catalog_name,
            })?
            .schema(&req.schema_name)
            .await
            .context(CatalogSnafu)?
            .context(SchemaNotFoundSnafu {
                schema_info: &req.schema_name,
            })?;

        let table_names = schema.table_names().await.context(CatalogSnafu)?;

        for table_name in table_names {
            self.copy_table_to(CopyTableRequest {
                catalog_name: req.catalog_name.clone(),
                schema_name: req.schema_name.clone(),
                table_name,
                location: "".to_string(),
                with: Default::default(),
                connection: Default::default(),
                pattern: None,
                direction: CopyDirection::Export,
                timestamp_range: req.time_range,
            })
            .await
            .unwrap(); // TODO(hl): remove this unwrap
        }

        todo!()
    }
}
