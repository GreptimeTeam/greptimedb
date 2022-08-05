// The `tables` table in system catalog keeps a record of all tables created by user.

use std::any::Any;
use std::sync::Arc;

use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use table::{Table, TableRef};

use crate::consts::{INFORMATION_SCHEMA_NAME, SYSTEM_CATALOG_TABLE_NAME};
use crate::system::SystemCatalogTable;
use crate::{CatalogListRef, CatalogProvider, SchemaProvider};

/// Tables holds all tables created by user.
pub struct Tables {
    schema: SchemaRef,
    catalogs: CatalogListRef,
}

impl Tables {
    pub fn new(catalogs: CatalogListRef) -> Self {
        Self {
            schema: Arc::new(build_schema_for_tables()),
            catalogs,
        }
    }
}

#[async_trait::async_trait]
impl Table for Tables {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> table::error::Result<SendableRecordBatchStream> {
        for catalog_name in self.catalogs.catalog_names() {
            let catalog = self.catalogs.catalog(&catalog_name).unwrap();
            for schema_name in catalog.schema_names() {
                let schema = catalog.schema(&schema_name).unwrap();
                for table_name in schema.table_names() {
                    todo!()
                }
            }
        }
        todo!()
    }
}

pub struct InformationSchema {
    pub tables: Arc<Tables>,
    pub system: Arc<SystemCatalogTable>,
}

impl SchemaProvider for InformationSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec!["tables".to_string(), SYSTEM_CATALOG_TABLE_NAME.to_string()]
    }

    fn table(&self, name: &str) -> Option<TableRef> {
        if name.eq_ignore_ascii_case("tables") {
            Some(self.tables.clone())
        } else if name.eq_ignore_ascii_case(SYSTEM_CATALOG_TABLE_NAME) {
            Some(self.system.clone())
        } else {
            None
        }
    }

    fn register_table(
        &self,
        _name: String,
        _table: TableRef,
    ) -> crate::error::Result<Option<TableRef>> {
        panic!("System catalog & schema does not support register table")
    }

    fn deregister_table(&self, _name: &str) -> crate::error::Result<Option<TableRef>> {
        panic!("System catalog & schema does not support deregister table")
    }

    fn table_exist(&self, name: &str) -> bool {
        if name.eq_ignore_ascii_case("tables")
            || name.eq_ignore_ascii_case(SYSTEM_CATALOG_TABLE_NAME)
        {
            true
        } else {
            false
        }
    }
}

pub struct SystemCatalog {
    pub information_schema: Arc<InformationSchema>,
}

impl SystemCatalog {
    pub fn new(system: SystemCatalogTable, catalogs: CatalogListRef) -> Self {
        let schema = InformationSchema {
            tables: Arc::new(Tables::new(catalogs)),
            system: Arc::new(system),
        };
        Self {
            information_schema: Arc::new(schema),
        }
    }
}

impl CatalogProvider for SystemCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![INFORMATION_SCHEMA_NAME.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name.eq_ignore_ascii_case(INFORMATION_SCHEMA_NAME) {
            Some(self.information_schema.clone())
        } else {
            None
        }
    }
}

fn build_schema_for_tables() -> Schema {
    let mut cols = Vec::with_capacity(6);
    cols.push(ColumnSchema::new(
        "catalog".to_string(),
        ConcreteDataType::string_datatype(),
        false,
    ));
    cols.push(ColumnSchema::new(
        "schema".to_string(),
        ConcreteDataType::string_datatype(),
        false,
    ));

    cols.push(ColumnSchema::new(
        "table_name".to_string(),
        ConcreteDataType::string_datatype(),
        false,
    ));

    cols.push(ColumnSchema::new(
        "table_id".to_string(),
        ConcreteDataType::uint64_datatype(),
        false,
    ));

    cols.push(ColumnSchema::new(
        "engine".to_string(),
        ConcreteDataType::string_datatype(),
        false,
    ));
    Schema::new(cols)
}
