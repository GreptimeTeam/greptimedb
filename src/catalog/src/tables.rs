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

use table::metadata::TableId;

use crate::system::SystemCatalogTable;

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
        self.information_schema
            .system
            .register_table(catalog, schema, table_name, table_id, engine)
            .await
    }

    pub async fn register_schema(
        &self,
        catalog: String,
        schema: String,
    ) -> crate::error::Result<usize> {
        self.information_schema
            .system
            .register_schema(catalog, schema)
            .await
    }
}
