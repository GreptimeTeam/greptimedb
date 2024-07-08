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

mod pg_catalog_memory_table;
mod pg_class;
mod table_names;

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::{ColumnSchema, SchemaRef};
use lazy_static::lazy_static;
use store_api::storage::{ScanRequest, TableId};
use table::metadata::TableType;
use table::TableRef;

use super::memory_table::tables::u32_column;
use crate::error::Result;
use crate::CatalogManager;

lazy_static! {
    static ref MEMORY_TABLES: &'static [&'static str] = &[table_names::PG_TYPE];
}

/// The column name for the OID column.
/// The OID column is a unique identifier of type u32 for each object in the database.
const OID_COLUMN_NAME: &str = "oid";

fn oid_column() -> ColumnSchema {
    u32_column(OID_COLUMN_NAME)
}

/// [`PGCatalogProvider`] is the provider for a schema named `pg_catalog`, it is not a catalog.
pub struct PGCatalogProvider {
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    tables: HashMap<String, TableRef>,
}

// TODO(j0hn50n133): Not sure whether to avoid duplication with `information_schema` or not.
macro_rules! setup_memory_table {
    ($name: expr) => {
        paste! {
            {
                let (schema, columns) = get_schema_columns($name);
                Some(Arc::new(MemoryTable::new(
                    consts::[<PG_CATALOG_ $name  _TABLE_ID>],
                    $name,
                    schema,
                    columns
                )) as _)
            }
        }
    };
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
        // Must follow the same security rules as [`InformationSchemaProvider::build_tables`].
        let mut tables = HashMap::new();
        for name in MEMORY_TABLES.iter() {
            tables.insert(name.to_string(), self.build_table(name).expect(name));
        }
        self.tables = tables;
    }

    fn build_table(&self, name: &str) -> Option<TableRef> {
        self.pg_catalog_table(name).map(|table| todo!())
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut tables = self.tables.values().clone().collect::<Vec<_>>();
        tables.sort_by(|t1, t2| {
            t1.table_info()
                .table_id()
                .partial_cmp(&t2.table_info().table_id())
                .unwrap()
        });
        tables
            .into_iter()
            .map(|t| t.table_info().name.clone())
            .collect()
    }

    fn pg_catalog_table(&self, name: &str) -> Option<PGCatalogTableRef> {
        match name.to_ascii_lowercase().as_str() {
            table_names::PG_TYPE => todo!(),
            _ => None,
        }
    }

    /// Returns the [TableRef] by table name.
    pub fn table(&self, name: &str) -> Option<TableRef> {
        self.tables.get(name).cloned()
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
