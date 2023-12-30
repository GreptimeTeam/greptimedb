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
use common_catalog::consts::INFORMATION_SCHEMA_KEY_COLUMN_USAGE_TABLE_ID;
use common_error::ext::BoxedError;
use common_query::physical_plan::TaskContext;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::{StringVectorBuilder, UInt32VectorBuilder};
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;

use super::KEY_COLUMN_USAGE;
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::InformationTable;
use crate::CatalogManager;

pub(super) struct InformationSchemaKeyColumnUsage {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaKeyColumnUsage {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "constraint_catalog",
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                "constraint_schema",
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                "constraint_name",
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new("table_catalog", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("table_schema", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("table_name", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("column_name", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ordinal_position",
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            ColumnSchema::new(
                "position_in_unique_constraint",
                ConcreteDataType::uint32_datatype(),
                true,
            ),
            ColumnSchema::new(
                "referenced_table_schema",
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(
                "referenced_table_name",
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(
                "referenced_column_name",
                ConcreteDataType::string_datatype(),
                true,
            ),
        ]))
    }

    fn builder(&self) -> InformationSchemaKeyColumnUsageBuilder {
        InformationSchemaKeyColumnUsageBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaKeyColumnUsage {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_KEY_COLUMN_USAGE_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        KEY_COLUMN_USAGE
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
                    .make_key_column_usage()
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

/// Builds the `information_schema.KEY_COLUMN_USAGE` table row by row
///
/// Columns are based on <https://dev.mysql.com/doc/refman/8.2/en/information-schema-key-column-usage-table.html>
struct InformationSchemaKeyColumnUsageBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    constraint_catalog: StringVectorBuilder,
    constraint_schema: StringVectorBuilder,
    constraint_name: StringVectorBuilder,
    table_catalog: StringVectorBuilder,
    table_schema: StringVectorBuilder,
    table_name: StringVectorBuilder,
    column_name: StringVectorBuilder,
    ordinal_position: UInt32VectorBuilder,
    position_in_unique_constraint: UInt32VectorBuilder,
    referenced_table_schema: StringVectorBuilder,
    referenced_table_name: StringVectorBuilder,
    referenced_column_name: StringVectorBuilder,
}

impl InformationSchemaKeyColumnUsageBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            constraint_catalog: StringVectorBuilder::with_capacity(42),
            constraint_schema: StringVectorBuilder::with_capacity(42),
            constraint_name: StringVectorBuilder::with_capacity(42),
            table_catalog: StringVectorBuilder::with_capacity(42),
            table_schema: StringVectorBuilder::with_capacity(42),
            table_name: StringVectorBuilder::with_capacity(42),
            column_name: StringVectorBuilder::with_capacity(42),
            ordinal_position: UInt32VectorBuilder::with_capacity(42),
            position_in_unique_constraint: UInt32VectorBuilder::with_capacity(42),
            referenced_table_schema: StringVectorBuilder::with_capacity(42),
            referenced_table_name: StringVectorBuilder::with_capacity(42),
            referenced_column_name: StringVectorBuilder::with_capacity(42),
        }
    }

    /// Construct the `information_schema.KEY_COLUMN_USAGE` virtual table
    async fn make_key_column_usage(&mut self) -> Result<RecordBatch> {
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
                if let Some(table) = catalog_manager
                    .table(&catalog_name, &schema_name, &table_name)
                    .await?
                {
                    let keys = &table.table_info().meta.primary_key_indices;
                    let schema = table.schema();

                    for (idx, column) in schema.column_schemas().iter().enumerate() {
                        let constraint_name = if column.is_time_index() {
                            "TIME INDEX"
                        } else if keys.contains(&idx) {
                            "PRIMARY"
                        } else {
                            // TODO: foreign constraint
                            continue;
                        };

                        self.add_key_column_usage(
                            "def",
                            &schema_name,
                            constraint_name,
                            "def",
                            &schema_name,
                            &table_name,
                            &column.name,
                            idx as u32 + 1,
                            None,
                            None,
                            None,
                            None,
                        );
                    }
                } else {
                    unreachable!();
                }
            }
        }

        self.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn add_key_column_usage(
        &mut self,
        constraint_catalog: &str,
        constraint_schema: &str,
        constraint_name: &str,
        table_catalog: &str,
        table_schema: &str,
        table_name: &str,
        column_name: &str,
        ordinal_position: u32,
        position_in_unique_constraint: Option<u32>,
        referenced_table_schema: Option<&str>,
        referenced_table_name: Option<&str>,
        referenced_column_name: Option<&str>,
    ) {
        self.constraint_catalog.push(Some(constraint_catalog));
        self.constraint_schema.push(Some(constraint_schema));
        self.constraint_name.push(Some(constraint_name));
        self.table_catalog.push(Some(table_catalog));
        self.table_schema.push(Some(table_schema));
        self.table_name.push(Some(table_name));
        self.column_name.push(Some(column_name));
        self.ordinal_position.push(Some(ordinal_position));
        self.position_in_unique_constraint
            .push(position_in_unique_constraint);
        self.referenced_table_schema.push(referenced_table_schema);
        self.referenced_table_name.push(referenced_table_name);
        self.referenced_column_name.push(referenced_column_name);
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.constraint_catalog.finish()),
            Arc::new(self.constraint_schema.finish()),
            Arc::new(self.constraint_name.finish()),
            Arc::new(self.table_catalog.finish()),
            Arc::new(self.table_schema.finish()),
            Arc::new(self.table_name.finish()),
            Arc::new(self.column_name.finish()),
            Arc::new(self.ordinal_position.finish()),
            Arc::new(self.position_in_unique_constraint.finish()),
            Arc::new(self.referenced_table_schema.finish()),
            Arc::new(self.referenced_table_name.finish()),
            Arc::new(self.referenced_column_name.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaKeyColumnUsage {
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
                    .make_key_column_usage()
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
