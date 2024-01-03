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
    INFORMATION_SCHEMA_COLUMNS_TABLE_ID, SEMANTIC_TYPE_FIELD, SEMANTIC_TYPE_PRIMARY_KEY,
    SEMANTIC_TYPE_TIME_INDEX,
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
use datatypes::value::Value;
use datatypes::vectors::{StringVectorBuilder, VectorRef};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};

use super::{InformationTable, COLUMNS};
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::Predicates;
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
const COLUMN_DEFAULT: &str = "column_default";
const IS_NULLABLE: &str = "is_nullable";
const COLUMN_TYPE: &str = "column_type";
const COLUMN_COMMENT: &str = "column_comment";

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
            ColumnSchema::new(COLUMN_DEFAULT, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(IS_NULLABLE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(COLUMN_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(COLUMN_COMMENT, ConcreteDataType::string_datatype(), true),
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

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_columns(Some(request))
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

    column_defaults: StringVectorBuilder,
    is_nullables: StringVectorBuilder,
    column_types: StringVectorBuilder,
    column_comments: StringVectorBuilder,
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
            column_defaults: StringVectorBuilder::with_capacity(42),
            is_nullables: StringVectorBuilder::with_capacity(42),
            column_types: StringVectorBuilder::with_capacity(42),
            column_comments: StringVectorBuilder::with_capacity(42),
        }
    }

    /// Construct the `information_schema.columns` virtual table
    async fn make_columns(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let predicates = Predicates::from_scan_request(&request);

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
                if let Some(table) = catalog_manager
                    .table(&catalog_name, &schema_name, &table_name)
                    .await?
                {
                    let keys = &table.table_info().meta.primary_key_indices;
                    let schema = table.schema();

                    for (idx, column) in schema.column_schemas().iter().enumerate() {
                        let semantic_type = if column.is_time_index() {
                            SEMANTIC_TYPE_TIME_INDEX
                        } else if keys.contains(&idx) {
                            SEMANTIC_TYPE_PRIMARY_KEY
                        } else {
                            SEMANTIC_TYPE_FIELD
                        };

                        self.add_column(
                            &predicates,
                            &catalog_name,
                            &schema_name,
                            &table_name,
                            semantic_type,
                            column,
                        );
                    }
                } else {
                    unreachable!();
                }
            }
        }

        self.finish()
    }

    fn add_column(
        &mut self,
        predicates: &Predicates,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        semantic_type: &str,
        column_schema: &ColumnSchema,
    ) {
        let data_type = &column_schema.data_type.name();

        let row = [
            (TABLE_CATALOG, &Value::from(catalog_name)),
            (TABLE_SCHEMA, &Value::from(schema_name)),
            (TABLE_NAME, &Value::from(table_name)),
            (COLUMN_NAME, &Value::from(column_schema.name.as_str())),
            (DATA_TYPE, &Value::from(data_type.as_str())),
            (SEMANTIC_TYPE, &Value::from(semantic_type)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.catalog_names.push(Some(catalog_name));
        self.schema_names.push(Some(schema_name));
        self.table_names.push(Some(table_name));
        self.column_names.push(Some(&column_schema.name));
        self.data_types.push(Some(data_type));
        self.semantic_types.push(Some(semantic_type));
        self.column_defaults.push(
            column_schema
                .default_constraint()
                .map(|s| format!("{}", s))
                .as_deref(),
        );
        if column_schema.is_nullable() {
            self.is_nullables.push(Some("Yes"));
        } else {
            self.is_nullables.push(Some("No"));
        }
        self.column_types.push(Some(data_type));
        self.column_comments
            .push(column_schema.column_comment().map(|x| x.as_ref()));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.catalog_names.finish()),
            Arc::new(self.schema_names.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.column_names.finish()),
            Arc::new(self.data_types.finish()),
            Arc::new(self.semantic_types.finish()),
            Arc::new(self.column_defaults.finish()),
            Arc::new(self.is_nullables.finish()),
            Arc::new(self.column_types.finish()),
            Arc::new(self.column_comments.finish()),
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
                    .make_columns(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
