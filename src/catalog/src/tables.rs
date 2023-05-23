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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::consts::{INFORMATION_SCHEMA_NAME, SYSTEM_CATALOG_TABLE_NAME};
use snafu::ResultExt;
use table::metadata::TableId;
use table::{Table, TableRef};

use crate::error::{self, Error, InsertCatalogRecordSnafu, Result as CatalogResult};
use crate::system::{
    build_schema_insert_request, build_table_deletion_request, build_table_insert_request,
    SystemCatalogTable,
};
use crate::{CatalogProvider, DeregisterTableRequest, SchemaProvider, SchemaProviderRef};

pub struct InformationSchema {
    pub system: Arc<SystemCatalogTable>,
}

#[async_trait]
impl SchemaProvider for InformationSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn table_names(&self) -> Result<Vec<String>, Error> {
        Ok(vec![SYSTEM_CATALOG_TABLE_NAME.to_string()])
    }

    async fn table(&self, name: &str) -> Result<Option<TableRef>, Error> {
        if name.eq_ignore_ascii_case(SYSTEM_CATALOG_TABLE_NAME) {
            Ok(Some(self.system.clone()))
        } else {
            Ok(None)
        }
    }

    async fn table_exist(&self, name: &str) -> Result<bool, Error> {
        Ok(name.eq_ignore_ascii_case(SYSTEM_CATALOG_TABLE_NAME))
    }
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
    ) -> CatalogResult<bool> {
        self.information_schema
            .system
            .delete(build_table_deletion_request(request, table_id))
            .await
            .map(|x| x == 1)
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

#[async_trait::async_trait]
impl CatalogProvider for SystemCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn schema_names(&self) -> Result<Vec<String>, Error> {
        Ok(vec![INFORMATION_SCHEMA_NAME.to_string()])
    }

    async fn register_schema(
        &self,
        _name: String,
        _schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>, Error> {
        panic!("System catalog does not support registering schema!")
    }

    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>, Error> {
        if name.eq_ignore_ascii_case(INFORMATION_SCHEMA_NAME) {
            Ok(Some(self.information_schema.clone()))
        } else {
            Ok(None)
        }
    }
}
