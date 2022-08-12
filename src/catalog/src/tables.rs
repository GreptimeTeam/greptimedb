// The `tables` table in system catalog keeps a record of all tables created by user.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_stream::stream;
use common_query::logical_plan::Expr;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::prelude::{ConcreteDataType, VectorBuilder};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use futures::Stream;
use table::engine::TableEngineRef;
use table::{Table, TableRef};

use crate::consts::{INFORMATION_SCHEMA_NAME, SYSTEM_CATALOG_TABLE_NAME};
use crate::system::SystemCatalogTable;
use crate::{CatalogListRef, CatalogProvider, SchemaProvider, SchemaProviderRef};

/// Tables holds all tables created by user.
pub struct Tables {
    schema: SchemaRef,
    catalogs: CatalogListRef,
    engine: TableEngineRef,
}

impl Tables {
    pub fn new(catalogs: CatalogListRef, engine: TableEngineRef) -> Self {
        Self {
            schema: Arc::new(build_schema_for_tables()),
            catalogs,
            engine,
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
        let catalogs = self.catalogs.clone();
        let schema_ref = self.schema.clone();
        let engine_name = self.engine.name().to_string();

        let stream = stream!({
            for catalog_name in catalogs.catalog_names() {
                let catalog = catalogs.catalog(&catalog_name).unwrap();
                for schema_name in catalog.schema_names() {
                    let mut tables_in_schema = Vec::with_capacity(catalog.schema_names().len());
                    let schema = catalog.schema(&schema_name).unwrap();
                    for table_name in schema.table_names() {
                        tables_in_schema.push(table_name);
                    }

                    let vec = tables_to_record_batch(
                        &catalog_name,
                        &schema_name,
                        tables_in_schema,
                        &engine_name,
                    );
                    let record_batch_res = RecordBatch::new(schema_ref.clone(), vec);
                    yield record_batch_res;
                }
            }
        });

        Ok(Box::pin(TablesRecordBatchStream {
            schema: self.schema.clone(),
            stream: Box::pin(stream),
        }))
    }
}

/// Convert tables info to `RecordBatch`.
fn tables_to_record_batch(
    catalog_name: &str,
    schema_name: &str,
    table_names: Vec<String>,
    engine: &str,
) -> Vec<VectorRef> {
    let mut catalog_vec =
        VectorBuilder::with_capacity(ConcreteDataType::string_datatype(), table_names.len());
    let mut schema_vec =
        VectorBuilder::with_capacity(ConcreteDataType::string_datatype(), table_names.len());
    let mut table_name_vec =
        VectorBuilder::with_capacity(ConcreteDataType::string_datatype(), table_names.len());
    let mut engine_vec =
        VectorBuilder::with_capacity(ConcreteDataType::string_datatype(), table_names.len());

    for table_name in table_names {
        catalog_vec.push(&Value::String(catalog_name.into()));
        schema_vec.push(&Value::String(schema_name.into()));
        table_name_vec.push(&Value::String(table_name.into()));
        engine_vec.push(&Value::String(engine.into()));
    }

    vec![
        catalog_vec.finish(),
        schema_vec.finish(),
        table_name_vec.finish(),
        engine_vec.finish(),
    ]
}

pub struct TablesRecordBatchStream {
    schema: SchemaRef,
    stream: Pin<Box<dyn Stream<Item = RecordBatchResult<RecordBatch>> + Send>>,
}

impl Stream for TablesRecordBatchStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

impl RecordBatchStream for TablesRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        name.eq_ignore_ascii_case("tables") || name.eq_ignore_ascii_case(SYSTEM_CATALOG_TABLE_NAME)
    }
}

pub struct SystemCatalog {
    pub information_schema: Arc<InformationSchema>,
}

impl SystemCatalog {
    pub fn new(
        system: SystemCatalogTable,
        catalogs: CatalogListRef,
        engine: TableEngineRef,
    ) -> Self {
        let schema = InformationSchema {
            tables: Arc::new(Tables::new(catalogs, engine)),
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

    fn register_schema(
        &self,
        _name: String,
        _schema: SchemaProviderRef,
    ) -> Option<SchemaProviderRef> {
        panic!("System catalog does not support registering schema!")
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
    let cols = vec![
        ColumnSchema::new(
            "catalog".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "schema".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "table_name".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "engine".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
    ];
    Schema::new(cols)
}
