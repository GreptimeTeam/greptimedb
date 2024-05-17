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

mod pg_class;
mod table_names;

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use store_api::storage::{ScanRequest, TableId};
use table::metadata::TableType;
use table::TableRef;

use self::table_names::PG_CLASS;
use crate::error::Result;
use crate::CatalogManager;

/// The column name for the OID column.
/// The OID column is a unique identifier of type u32 for each object in the database.
const OID_COLUMN_NAME: &str = "oid";

/// [`PGCatalogProvider`] is the provider for a schema named `pg_catalog`, it is not a catalog.
pub struct PGCatalogProvider {
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    tables: HashMap<String, TableRef>,
}

impl PGCatalogProvider {
    pub fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        let mut provider = Self {
            catalog_name,
            catalog_manager,
            tables: HashMap::new(),
        };
        provider.build_tables();
        provider
    }

    fn build_tables(&mut self) {
        // SECURITY NOTE:
        // Follow the same security rules as [InformationSchemaProvider::build_tables].
        if self.catalog_name == DEFAULT_CATALOG_NAME {
            self.tables.insert(
                table_names::PG_CLASS.to_string(),
                self.build_table(PG_CLASS).unwrap(),
            );
        }
    }

    fn build_table(&self, name: &str) -> Option<TableRef> {
        // self.
        todo!()
    }

    pub fn table_names(&self) -> Vec<String> {
        // TODO(j0hn50n133): replace hardcoded table names with collected table names
        vec![
            table_names::PG_DATABASE.to_string(),
            table_names::PG_NAMESPACE.to_string(),
            table_names::PG_CLASS.to_string(),
        ]
    }

    pub fn pg_catalog_table(&self, name: &str) -> Option<PGCatalogTableRef> {
        match name.to_ascii_lowercase().as_str() {
            table_names::PG_CLASS => todo!(),
            _ => None,
        }
    }

    /// Returns the [TableRef] by table name.
    pub fn table(&self, name: &str) -> Option<TableRef> {
        todo!()
    }
}

/// The trait for all tables in the `pg_catalog` schema.
trait PGCatalogTable {
    fn table_id(&self) -> TableId;

    fn table_name(&self) -> &'static str;

    fn schema(&self) -> SchemaRef;

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream>;

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
}

type PGCatalogTableRef = Arc<dyn PGCatalogTable + Send + Sync>;
