// The `tables` table in system catalog keeps a record of all tables created by user.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_stream::stream;
use common_catalog::consts::{INFORMATION_SCHEMA_NAME, SYSTEM_CATALOG_TABLE_NAME};
use common_error::ext::BoxedError;
use common_query::logical_plan::Expr;
use common_query::physical_plan::PhysicalPlanRef;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream};
use datatypes::prelude::{ConcreteDataType, VectorBuilder};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use futures::Stream;
use snafu::ResultExt;
use table::engine::TableEngineRef;
use table::error::TablesRecordBatchSnafu;
use table::metadata::{TableId, TableInfoRef};
use table::table::scan::SimpleTableScan;
use table::{Table, TableRef};

use crate::error::{Error, InsertTableRecordSnafu};
use crate::system::{build_table_insert_request, SystemCatalogTable};
use crate::{
    format_full_table_name, CatalogListRef, CatalogProvider, SchemaProvider, SchemaProviderRef,
};

/// Tables holds all tables created by user.
pub struct Tables {
    schema: SchemaRef,
    catalogs: CatalogListRef,
    engine_name: String,
}

impl Tables {
    pub fn new(catalogs: CatalogListRef, engine_name: String) -> Self {
        Self {
            schema: Arc::new(build_schema_for_tables()),
            catalogs,
            engine_name,
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

    fn table_info(&self) -> TableInfoRef {
        unreachable!("Tables does not support table_info method")
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> table::error::Result<PhysicalPlanRef> {
        let catalogs = self.catalogs.clone();
        let schema_ref = self.schema.clone();
        let engine_name = self.engine_name.clone();

        let stream = stream!({
            for catalog_name in catalogs
                .catalog_names()
                .map_err(BoxedError::new)
                .context(TablesRecordBatchSnafu)?
            {
                let catalog = catalogs
                    .catalog(&catalog_name)
                    .map_err(BoxedError::new)
                    .context(TablesRecordBatchSnafu)?
                    .unwrap();
                for schema_name in catalog
                    .schema_names()
                    .map_err(BoxedError::new)
                    .context(TablesRecordBatchSnafu)?
                {
                    let mut tables_in_schema = Vec::with_capacity(
                        catalog
                            .schema_names()
                            .map_err(BoxedError::new)
                            .context(TablesRecordBatchSnafu)?
                            .len(),
                    );
                    let schema = catalog
                        .schema(&schema_name)
                        .map_err(BoxedError::new)
                        .context(TablesRecordBatchSnafu)?
                        .unwrap();
                    for table_name in schema
                        .table_names()
                        .map_err(BoxedError::new)
                        .context(TablesRecordBatchSnafu)?
                    {
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

        let stream = Box::pin(TablesRecordBatchStream {
            schema: self.schema.clone(),
            stream: Box::pin(stream),
        });
        Ok(Arc::new(SimpleTableScan::new(stream)))
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

    fn table_names(&self) -> Result<Vec<String>, Error> {
        Ok(vec![
            "tables".to_string(),
            SYSTEM_CATALOG_TABLE_NAME.to_string(),
        ])
    }

    fn table(&self, name: &str) -> Result<Option<TableRef>, Error> {
        if name.eq_ignore_ascii_case("tables") {
            Ok(Some(self.tables.clone()))
        } else if name.eq_ignore_ascii_case(SYSTEM_CATALOG_TABLE_NAME) {
            Ok(Some(self.system.clone()))
        } else {
            Ok(None)
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

    fn table_exist(&self, name: &str) -> Result<bool, Error> {
        Ok(name.eq_ignore_ascii_case("tables")
            || name.eq_ignore_ascii_case(SYSTEM_CATALOG_TABLE_NAME))
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
            tables: Arc::new(Tables::new(catalogs, engine.name().to_string())),
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
    ) -> crate::error::Result<usize> {
        let full_table_name = format_full_table_name(&catalog, &schema, &table_name);
        let request = build_table_insert_request(full_table_name, table_id);
        self.information_schema
            .system
            .insert(request)
            .await
            .context(InsertTableRecordSnafu)
    }
}

impl CatalogProvider for SystemCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Result<Vec<String>, Error> {
        Ok(vec![INFORMATION_SCHEMA_NAME.to_string()])
    }

    fn register_schema(
        &self,
        _name: String,
        _schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>, Error> {
        panic!("System catalog does not support registering schema!")
    }

    fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>, Error> {
        if name.eq_ignore_ascii_case(INFORMATION_SCHEMA_NAME) {
            Ok(Some(self.information_schema.clone()))
        } else {
            Ok(None)
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

#[cfg(test)]
mod tests {
    use common_query::physical_plan::RuntimeEnv;
    use datatypes::arrow::array::Utf8Array;
    use datatypes::arrow::datatypes::DataType;
    use futures_util::StreamExt;
    use table::table::numbers::NumbersTable;

    use super::*;
    use crate::local::memory::{
        new_memory_catalog_list, MemoryCatalogProvider, MemorySchemaProvider,
    };
    use crate::CatalogList;

    #[tokio::test]
    async fn test_tables() {
        let catalog_list = new_memory_catalog_list().unwrap();
        let catalog_provider = Arc::new(MemoryCatalogProvider::default());
        let schema = Arc::new(MemorySchemaProvider::new());
        schema
            .register_table("test_table".to_string(), Arc::new(NumbersTable::default()))
            .unwrap();
        catalog_provider
            .register_schema("test_schema".to_string(), schema)
            .unwrap();
        catalog_list
            .register_catalog("test_catalog".to_string(), catalog_provider)
            .unwrap();
        let tables = Tables::new(catalog_list, "test_engine".to_string());

        let tables_stream = tables.scan(&None, &[], None).await.unwrap();
        let mut tables_stream = tables_stream
            .execute(0, Arc::new(RuntimeEnv::default()))
            .await
            .unwrap();
        if let Some(t) = tables_stream.next().await {
            let batch = t.unwrap().df_recordbatch;
            assert_eq!(1, batch.num_rows());
            assert_eq!(4, batch.num_columns());
            assert_eq!(&DataType::Utf8, batch.column(0).data_type());
            assert_eq!(&DataType::Utf8, batch.column(1).data_type());
            assert_eq!(&DataType::Utf8, batch.column(2).data_type());
            assert_eq!(&DataType::Utf8, batch.column(3).data_type());
            assert_eq!(
                "test_catalog",
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
                    .value(0)
            );

            assert_eq!(
                "test_schema",
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
                    .value(0)
            );

            assert_eq!(
                "test_table",
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
                    .value(0)
            );

            assert_eq!(
                "test_engine",
                batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
                    .value(0)
            );
        } else {
            panic!("Record batch should not be empty!")
        }
    }
}
