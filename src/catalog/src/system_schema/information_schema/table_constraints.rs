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
use common_catalog::consts::INFORMATION_SCHEMA_TABLE_CONSTRAINTS_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, MutableVector};
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{ConstantVector, StringVector, StringVectorBuilder, VectorRef};
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};

use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::key_column_usage::{
    PRI_CONSTRAINT_NAME, TIME_INDEX_CONSTRAINT_NAME,
};
use crate::information_schema::Predicates;
use crate::system_schema::information_schema::{InformationTable, TABLE_CONSTRAINTS};
use crate::CatalogManager;

/// The `TABLE_CONSTRAINTS` table describes which tables have constraints.
#[derive(Debug)]
pub(super) struct InformationSchemaTableConstraints {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

const CONSTRAINT_CATALOG: &str = "constraint_catalog";
const CONSTRAINT_SCHEMA: &str = "constraint_schema";
const CONSTRAINT_NAME: &str = "constraint_name";
const TABLE_SCHEMA: &str = "table_schema";
const TABLE_NAME: &str = "table_name";
const CONSTRAINT_TYPE: &str = "constraint_type";
const ENFORCED: &str = "enforced";

const INIT_CAPACITY: usize = 42;

const TIME_INDEX_CONSTRAINT_TYPE: &str = "TIME INDEX";
const PRI_KEY_CONSTRAINT_TYPE: &str = "PRIMARY KEY";

impl InformationSchemaTableConstraints {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                CONSTRAINT_CATALOG,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                CONSTRAINT_SCHEMA,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(CONSTRAINT_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_SCHEMA, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(CONSTRAINT_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(ENFORCED, ConcreteDataType::string_datatype(), false),
        ]))
    }

    fn builder(&self) -> InformationSchemaTableConstraintsBuilder {
        InformationSchemaTableConstraintsBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaTableConstraints {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_TABLE_CONSTRAINTS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        TABLE_CONSTRAINTS
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
                    .make_table_constraints(Some(request))
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

struct InformationSchemaTableConstraintsBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    constraint_schemas: StringVectorBuilder,
    constraint_names: StringVectorBuilder,
    table_schemas: StringVectorBuilder,
    table_names: StringVectorBuilder,
    constraint_types: StringVectorBuilder,
}

impl InformationSchemaTableConstraintsBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            constraint_schemas: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            constraint_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_schemas: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            constraint_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.table_constraints` virtual table
    async fn make_table_constraints(
        &mut self,
        request: Option<ScanRequest>,
    ) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let predicates = Predicates::from_scan_request(&request);

        for schema_name in catalog_manager.schema_names(&catalog_name, None).await? {
            let mut stream = catalog_manager.tables(&catalog_name, &schema_name, None);

            while let Some(table) = stream.try_next().await? {
                let keys = &table.table_info().meta.primary_key_indices;
                let schema = table.schema();

                if schema.timestamp_index().is_some() {
                    self.add_table_constraint(
                        &predicates,
                        &schema_name,
                        TIME_INDEX_CONSTRAINT_NAME,
                        &schema_name,
                        &table.table_info().name,
                        TIME_INDEX_CONSTRAINT_TYPE,
                    );
                }

                if !keys.is_empty() {
                    self.add_table_constraint(
                        &predicates,
                        &schema_name,
                        PRI_CONSTRAINT_NAME,
                        &schema_name,
                        &table.table_info().name,
                        PRI_KEY_CONSTRAINT_TYPE,
                    );
                }
            }
        }

        self.finish()
    }

    fn add_table_constraint(
        &mut self,
        predicates: &Predicates,
        constraint_schema: &str,
        constraint_name: &str,
        table_schema: &str,
        table_name: &str,
        constraint_type: &str,
    ) {
        let row = [
            (CONSTRAINT_SCHEMA, &Value::from(constraint_schema)),
            (CONSTRAINT_NAME, &Value::from(constraint_name)),
            (TABLE_SCHEMA, &Value::from(table_schema)),
            (TABLE_NAME, &Value::from(table_name)),
            (CONSTRAINT_TYPE, &Value::from(constraint_type)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.constraint_schemas.push(Some(constraint_schema));
        self.constraint_names.push(Some(constraint_name));
        self.table_schemas.push(Some(table_schema));
        self.table_names.push(Some(table_name));
        self.constraint_types.push(Some(constraint_type));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let rows_num = self.constraint_names.len();

        let constraint_catalogs = Arc::new(ConstantVector::new(
            Arc::new(StringVector::from(vec!["def"])),
            rows_num,
        ));
        let enforceds = Arc::new(ConstantVector::new(
            Arc::new(StringVector::from(vec!["YES"])),
            rows_num,
        ));

        let columns: Vec<VectorRef> = vec![
            constraint_catalogs,
            Arc::new(self.constraint_schemas.finish()),
            Arc::new(self.constraint_names.finish()),
            Arc::new(self.table_schemas.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.constraint_types.finish()),
            enforceds,
        ];

        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaTableConstraints {
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
                    .make_table_constraints(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
