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
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::ValueRef;
use datatypes::vectors::VectorRef;
use futures::Stream;
use snafu::ResultExt;
use table::engine::TableEngineRef;
use table::error::TablesRecordBatchSnafu;
use table::metadata::{TableId, TableInfoRef};
use table::table::scan::SimpleTableScan;
use table::{Table, TableRef};

use crate::error::{self, Error, InsertCatalogRecordSnafu, Result as CatalogResult};
use crate::system::{
    build_schema_insert_request, build_table_deletion_request, build_table_insert_request,
    SystemCatalogTable,
};
use crate::{
    CatalogListRef, CatalogProvider, DeregisterTableRequest, SchemaProvider, SchemaProviderRef,
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
        _projection: Option<&Vec<usize>>,
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
        ConcreteDataType::string_datatype().create_mutable_vector(table_names.len());
    let mut schema_vec =
        ConcreteDataType::string_datatype().create_mutable_vector(table_names.len());
    let mut table_name_vec =
        ConcreteDataType::string_datatype().create_mutable_vector(table_names.len());
    let mut engine_vec =
        ConcreteDataType::string_datatype().create_mutable_vector(table_names.len());

    for table_name in table_names {
        // Safety: All these vectors are string type.
        catalog_vec
            .push_value_ref(ValueRef::String(catalog_name))
            .unwrap();
        schema_vec
            .push_value_ref(ValueRef::String(schema_name))
            .unwrap();
        table_name_vec
            .push_value_ref(ValueRef::String(&table_name))
            .unwrap();
        engine_vec.push_value_ref(ValueRef::String(engine)).unwrap();
    }

    vec![
        catalog_vec.to_vector(),
        schema_vec.to_vector(),
        table_name_vec.to_vector(),
        engine_vec.to_vector(),
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

    fn rename_table(&self, _name: &str, _new_name: String) -> crate::error::Result<TableRef> {
        unimplemented!("System catalog & schema does not support rename table")
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
        let request = build_table_insert_request(catalog, schema, table_name, table_id);
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
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_query::physical_plan::SessionContext;
    use futures_util::StreamExt;
    use table::table::numbers::NumbersTable;

    use super::*;
    use crate::local::memory::new_memory_catalog_list;
    use crate::CatalogList;

    #[tokio::test]
    async fn test_tables() {
        let catalog_list = new_memory_catalog_list().unwrap();
        let schema = catalog_list
            .catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .unwrap()
            .schema(DEFAULT_SCHEMA_NAME)
            .unwrap()
            .unwrap();
        schema
            .register_table("test_table".to_string(), Arc::new(NumbersTable::default()))
            .unwrap();

        let tables = Tables::new(catalog_list, "test_engine".to_string());
        let tables_stream = tables.scan(None, &[], None).await.unwrap();
        let session_ctx = SessionContext::new();
        let mut tables_stream = tables_stream.execute(0, session_ctx.task_ctx()).unwrap();

        if let Some(t) = tables_stream.next().await {
            let batch = t.unwrap();
            assert_eq!(1, batch.num_rows());
            assert_eq!(4, batch.num_columns());
            assert_eq!(
                ConcreteDataType::string_datatype(),
                batch.column(0).data_type()
            );
            assert_eq!(
                ConcreteDataType::string_datatype(),
                batch.column(1).data_type()
            );
            assert_eq!(
                ConcreteDataType::string_datatype(),
                batch.column(2).data_type()
            );
            assert_eq!(
                ConcreteDataType::string_datatype(),
                batch.column(3).data_type()
            );
            assert_eq!(
                "greptime",
                batch.column(0).get_ref(0).as_string().unwrap().unwrap()
            );

            assert_eq!(
                "public",
                batch.column(1).get_ref(0).as_string().unwrap().unwrap()
            );

            assert_eq!(
                "test_table",
                batch.column(2).get_ref(0).as_string().unwrap().unwrap()
            );

            assert_eq!(
                "test_engine",
                batch.column(3).get_ref(0).as_string().unwrap().unwrap()
            );
        } else {
            panic!("Record batch should not be empty!")
        }
    }
}
