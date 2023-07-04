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

// The `tables` table in system catalog keeps a record of all tables created by user.

use std::sync::Arc;

use common_telemetry::logging;
use snafu::ResultExt;
use table::metadata::TableId;
use table::Table;

use crate::error::{self, InsertCatalogRecordSnafu, Result as CatalogResult};
use crate::system::{
    build_schema_insert_request, build_table_deletion_request, build_table_insert_request,
    SystemCatalogTable,
};
use crate::DeregisterTableRequest;

pub struct InformationSchema {
    pub system: Arc<SystemCatalogTable>,
}

pub struct SystemCatalog {
    pub information_schema: Arc<InformationSchema>,
}

impl SystemCatalog {
    pub(crate) fn new(system: SystemCatalogTable) -> Self {
        let schema = InformationSchema {
            system: Arc::new(system),
        };
        Self {
            information_schema: Arc::new(schema),
        }
    }

    pub async fn register_table(
        &self,
        catalog: String,
        schema: String,
        table_name: String,
        table_id: TableId,
        engine: String,
    ) -> crate::error::Result<usize> {
        let request = build_table_insert_request(catalog, schema, table_name, table_id, engine);
        self.information_schema
            .system
            .insert(request)
            .await
            .context(InsertCatalogRecordSnafu)
    }

    pub(crate) async fn deregister_table(
        &self,
        request: &DeregisterTableRequest,
        table_id: TableId,
    ) -> CatalogResult<()> {
        self.information_schema
            .system
            .insert(build_table_deletion_request(request, table_id))
            .await
            .map(|x| {
                if x != 1 {
                    let table = common_catalog::format_full_table_name(
                        &request.catalog,
                        &request.schema,
                        &request.table_name
                    );
                    logging::warn!("Failed to delete table record from information_schema, unexpected returned result: {x}, table: {table}");
                }
            })
            .with_context(|_| error::DeregisterTableSnafu {
                request: request.clone(),
            })
    }

    pub async fn register_schema(
        &self,
        catalog: String,
        schema: String,
    ) -> crate::error::Result<usize> {
        let request = build_schema_insert_request(catalog, schema);
        self.information_schema
            .system
            .insert(request)
            .await
            .context(InsertCatalogRecordSnafu)
    }
}
