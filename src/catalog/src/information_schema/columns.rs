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

use std::sync::{Arc, Weak};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::{
    INFORMATION_SCHEMA_COLUMNS_TABLE_ID, INFORMATION_SCHEMA_NAME, SEMANTIC_TYPE_FIELD,
    SEMANTIC_TYPE_PRIMARY_KEY, SEMANTIC_TYPE_TIME_INDEX,
};
use common_error::ext::BoxedError;
use common_query::physical_plan::TaskContext;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::{StringVectorBuilder, VectorRef};
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;

use super::tables::InformationSchemaTables;
use super::{InformationTable, COLUMNS, TABLES};
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::CatalogManager;

pub(super) struct InformationSchemaColumns {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

const TABLE_CATALOG: &str = "table_catalog";
const TABLE_SCHEMA: &str = "table_schema";
const TABLE_NAME: &str = "table_name";
const COLUMN_NAME: &str = "column_name";
const DATA_TYPE: &str = "data_type";
const SEMANTIC_TYPE: &str = "semantic_type";

impl InformationSchemaColumns {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(TABLE_CATALOG, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_SCHEMA, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(COLUMN_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(DATA_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(SEMANTIC_TYPE, ConcreteDataType::string_datatype(), false),
        ]))
    }

    fn builder(&self) -> InformationSchemaColumnsBuilder {
        InformationSchemaColumnsBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaColumns {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_COLUMNS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        COLUMNS
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_tables()
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

struct InformationSchemaColumnsBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    catalog_names: StringVectorBuilder,
    schema_names: StringVectorBuilder,
    table_names: StringVectorBuilder,
    column_names: StringVectorBuilder,
    data_types: StringVectorBuilder,
    semantic_types: StringVectorBuilder,
}

impl InformationSchemaColumnsBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            catalog_names: StringVectorBuilder::with_capacity(42),
            schema_names: StringVectorBuilder::with_capacity(42),
            table_names: StringVectorBuilder::with_capacity(42),
            column_names: StringVectorBuilder::with_capacity(42),
            data_types: StringVectorBuilder::with_capacity(42),
            semantic_types: StringVectorBuilder::with_capacity(42),
        }
    }

    /// Construct the `information_schema.tables` virtual table
    async fn make_tables(&mut self) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;

        for schema_name in catalog_manager.schema_names(&catalog_name).await? {
            if !catalog_manager
                .schema_exists(&catalog_name, &schema_name)
                .await?
            {
                continue;
            }
            for table_name in catalog_manager
                .table_names(&catalog_name, &schema_name)
                .await?
            {
                let (keys, schema) = if let Some(table) = catalog_manager
                    .table(&catalog_name, &schema_name, &table_name)
                    .await?
                {
                    let keys = &table.table_info().meta.primary_key_indices;
                    let schema = table.schema();
                    (keys.clone(), schema)
                } else {
                    // TODO: this specific branch is only a workaround for FrontendCatalogManager.
                    if schema_name == INFORMATION_SCHEMA_NAME {
                        if table_name == COLUMNS {
                            (vec![], InformationSchemaColumns::schema())
                        } else if table_name == TABLES {
                            (vec![], InformationSchemaTables::schema())
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                };

                for (idx, column) in schema.column_schemas().iter().enumerate() {
                    let semantic_type = if column.is_time_index() {
                        SEMANTIC_TYPE_TIME_INDEX
                    } else if keys.contains(&idx) {
                        SEMANTIC_TYPE_PRIMARY_KEY
                    } else {
                        SEMANTIC_TYPE_FIELD
                    };
                    self.add_column(
                        &catalog_name,
                        &schema_name,
                        &table_name,
                        &column.name,
                        column.data_type.name(),
                        semantic_type,
                    );
                }
            }
        }

        self.finish()
    }

    fn add_column(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
        data_type: &str,
        semantic_type: &str,
    ) {
        self.catalog_names.push(Some(catalog_name));
        self.schema_names.push(Some(schema_name));
        self.table_names.push(Some(table_name));
        self.column_names.push(Some(column_name));
        self.data_types.push(Some(data_type));
        self.semantic_types.push(Some(semantic_type));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.catalog_names.finish()),
            Arc::new(self.schema_names.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.column_names.finish()),
            Arc::new(self.data_types.finish()),
            Arc::new(self.semantic_types.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaColumns {
    fn schema(&self) -> &ArrowSchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_tables()
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
