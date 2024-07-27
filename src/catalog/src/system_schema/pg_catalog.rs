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
mod pg_namespace;
mod table_names;

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use common_catalog::consts::{self, PG_CATALOG_NAME};
use datatypes::schema::ColumnSchema;
use lazy_static::lazy_static;
use paste::paste;
use pg_catalog_memory_table::get_schema_columns;
use pg_class::PGClass;
use pg_namespace::PGNamespace;
use table::TableRef;
pub use table_names::*;

use super::memory_table::MemoryTable;
use super::utils::tables::u32_column;
use super::{SystemSchemaProvider, SystemSchemaProviderInner, SystemTableRef};
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

impl SystemSchemaProvider for PGCatalogProvider {
    fn tables(&self) -> &HashMap<String, TableRef> {
        assert!(!self.tables.is_empty());

        &self.tables
    }
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
        // TODO(J0HN50N133): modeling the table_name as a enum type to get rid of expect/unwrap here
        // It's safe to unwrap here because we are sure that the constants have been handle correctly inside system_table.
        for name in MEMORY_TABLES.iter() {
            tables.insert(name.to_string(), self.build_table(name).expect(name));
        }
        tables.insert(
            PG_NAMESPACE.to_string(),
            self.build_table(PG_NAMESPACE).expect(PG_NAMESPACE),
        );
        tables.insert(
            PG_CLASS.to_string(),
            self.build_table(PG_CLASS).expect(PG_NAMESPACE),
        );
        self.tables = tables;
    }
}

impl SystemSchemaProviderInner for PGCatalogProvider {
    fn schema_name() -> &'static str {
        PG_CATALOG_NAME
    }

    fn system_table(&self, name: &str) -> Option<SystemTableRef> {
        match name {
            table_names::PG_TYPE => setup_memory_table!(PG_TYPE),
            table_names::PG_NAMESPACE => Some(Arc::new(PGNamespace::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            ))),
            table_names::PG_CLASS => Some(Arc::new(PGClass::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            ))),
            _ => None,
        }
    }

    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }
}
