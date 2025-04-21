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
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, MutableVector, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, FulltextBackend, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{ConstantVector, StringVector, StringVectorBuilder, UInt32VectorBuilder};
use futures_util::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};

use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::system_schema::information_schema::{InformationTable, Predicates, KEY_COLUMN_USAGE};
use crate::CatalogManager;

pub const CONSTRAINT_SCHEMA: &str = "constraint_schema";
pub const CONSTRAINT_NAME: &str = "constraint_name";
// It's always `def` in MySQL
pub const TABLE_CATALOG: &str = "table_catalog";
// The real catalog name for this key column.
pub const REAL_TABLE_CATALOG: &str = "real_table_catalog";
pub const TABLE_SCHEMA: &str = "table_schema";
pub const TABLE_NAME: &str = "table_name";
pub const COLUMN_NAME: &str = "column_name";
pub const ORDINAL_POSITION: &str = "ordinal_position";
/// The type of the index.
pub const GREPTIME_INDEX_TYPE: &str = "greptime_index_type";
const INIT_CAPACITY: usize = 42;

/// Time index constraint name
pub(crate) const CONSTRAINT_NAME_TIME_INDEX: &str = "TIME INDEX";

/// Primary key constraint name
pub(crate) const CONSTRAINT_NAME_PRI: &str = "PRIMARY";
/// Primary key index type
pub(crate) const INDEX_TYPE_PRI: &str = "greptime-primary-key-v1";

/// Inverted index constraint name
pub(crate) const CONSTRAINT_NAME_INVERTED_INDEX: &str = "INVERTED INDEX";
/// Inverted index type
pub(crate) const INDEX_TYPE_INVERTED_INDEX: &str = "greptime-inverted-index-v1";

/// Fulltext index constraint name
pub(crate) const CONSTRAINT_NAME_FULLTEXT_INDEX: &str = "FULLTEXT INDEX";
/// Fulltext index v1 type
pub(crate) const INDEX_TYPE_FULLTEXT_TANTIVY: &str = "greptime-fulltext-index-v1";
/// Fulltext index bloom type
pub(crate) const INDEX_TYPE_FULLTEXT_BLOOM: &str = "greptime-fulltext-index-bloom";

/// Skipping index constraint name
pub(crate) const CONSTRAINT_NAME_SKIPPING_INDEX: &str = "SKIPPING INDEX";
/// Skipping index type
pub(crate) const INDEX_TYPE_SKIPPING_INDEX: &str = "greptime-bloom-filter-v1";

/// The virtual table implementation for `information_schema.KEY_COLUMN_USAGE`.
///
/// Provides an extra column `greptime_index_type` for the index type of the key column.
#[derive(Debug)]
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
                CONSTRAINT_SCHEMA,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(CONSTRAINT_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_CATALOG, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                REAL_TABLE_CATALOG,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(TABLE_SCHEMA, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(COLUMN_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(ORDINAL_POSITION, ConcreteDataType::uint32_datatype(), false),
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
            ColumnSchema::new(
                GREPTIME_INDEX_TYPE,
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

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_key_column_usage(Some(request))
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
    real_table_catalog: StringVectorBuilder,
    table_schema: StringVectorBuilder,
    table_name: StringVectorBuilder,
    column_name: StringVectorBuilder,
    ordinal_position: UInt32VectorBuilder,
    position_in_unique_constraint: UInt32VectorBuilder,
    greptime_index_type: StringVectorBuilder,
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
            constraint_catalog: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            constraint_schema: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            constraint_name: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_catalog: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            real_table_catalog: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_schema: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_name: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            column_name: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            ordinal_position: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            position_in_unique_constraint: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            greptime_index_type: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.KEY_COLUMN_USAGE` virtual table
    async fn make_key_column_usage(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let predicates = Predicates::from_scan_request(&request);

        for schema_name in catalog_manager.schema_names(&catalog_name, None).await? {
            let mut stream = catalog_manager.tables(&catalog_name, &schema_name, None);

            while let Some(table) = stream.try_next().await? {
                let table_info = table.table_info();
                let table_name = &table_info.name;
                let keys = &table_info.meta.primary_key_indices;
                let schema = table.schema();

                for (idx, column) in schema.column_schemas().iter().enumerate() {
                    let mut constraints = vec![];
                    let mut greptime_index_type = vec![];
                    if column.is_time_index() {
                        self.add_key_column_usage(
                            &predicates,
                            &schema_name,
                            CONSTRAINT_NAME_TIME_INDEX,
                            &catalog_name,
                            &schema_name,
                            table_name,
                            &column.name,
                            1, //always 1 for time index
                            "",
                        );
                    }
                    // TODO(dimbtp): foreign key constraint not supported yet
                    if keys.contains(&idx) {
                        constraints.push(CONSTRAINT_NAME_PRI);
                        greptime_index_type.push(INDEX_TYPE_PRI);
                    }
                    if column.is_inverted_indexed() {
                        constraints.push(CONSTRAINT_NAME_INVERTED_INDEX);
                        greptime_index_type.push(INDEX_TYPE_INVERTED_INDEX);
                    }
                    if let Ok(Some(options)) = column.fulltext_options() {
                        if options.enable {
                            constraints.push(CONSTRAINT_NAME_FULLTEXT_INDEX);
                            let index_type = match options.backend {
                                FulltextBackend::Bloom => INDEX_TYPE_FULLTEXT_BLOOM,
                                FulltextBackend::Tantivy => INDEX_TYPE_FULLTEXT_TANTIVY,
                            };
                            greptime_index_type.push(index_type);
                        }
                    }
                    if column.is_skipping_indexed() {
                        constraints.push(CONSTRAINT_NAME_SKIPPING_INDEX);
                        greptime_index_type.push(INDEX_TYPE_SKIPPING_INDEX);
                    }

                    if !constraints.is_empty() {
                        let aggregated_constraints = constraints.join(", ");
                        let aggregated_index_types = greptime_index_type.join(", ");
                        self.add_key_column_usage(
                            &predicates,
                            &schema_name,
                            &aggregated_constraints,
                            &catalog_name,
                            &schema_name,
                            table_name,
                            &column.name,
                            idx as u32 + 1,
                            &aggregated_index_types,
                        );
                    }
                }
            }
        }

        self.finish()
    }

    // TODO(dimbtp): Foreign key constraint has not `None` value for last 4
    // fields, but it is not supported yet.
    #[allow(clippy::too_many_arguments)]
    fn add_key_column_usage(
        &mut self,
        predicates: &Predicates,
        constraint_schema: &str,
        constraint_name: &str,
        table_catalog: &str,
        table_schema: &str,
        table_name: &str,
        column_name: &str,
        ordinal_position: u32,
        index_types: &str,
    ) {
        let row = [
            (CONSTRAINT_SCHEMA, &Value::from(constraint_schema)),
            (CONSTRAINT_NAME, &Value::from(constraint_name)),
            (REAL_TABLE_CATALOG, &Value::from(table_catalog)),
            (TABLE_SCHEMA, &Value::from(table_schema)),
            (TABLE_NAME, &Value::from(table_name)),
            (COLUMN_NAME, &Value::from(column_name)),
            (ORDINAL_POSITION, &Value::from(ordinal_position)),
            (GREPTIME_INDEX_TYPE, &Value::from(index_types)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.constraint_catalog.push(Some("def"));
        self.constraint_schema.push(Some(constraint_schema));
        self.constraint_name.push(Some(constraint_name));
        self.table_catalog.push(Some("def"));
        self.real_table_catalog.push(Some(table_catalog));
        self.table_schema.push(Some(table_schema));
        self.table_name.push(Some(table_name));
        self.column_name.push(Some(column_name));
        self.ordinal_position.push(Some(ordinal_position));
        self.position_in_unique_constraint.push(None);
        self.greptime_index_type.push(Some(index_types));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let rows_num = self.table_catalog.len();

        let null_string_vector = Arc::new(ConstantVector::new(
            Arc::new(StringVector::from(vec![None as Option<&str>])),
            rows_num,
        ));
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.constraint_catalog.finish()),
            Arc::new(self.constraint_schema.finish()),
            Arc::new(self.constraint_name.finish()),
            Arc::new(self.table_catalog.finish()),
            Arc::new(self.real_table_catalog.finish()),
            Arc::new(self.table_schema.finish()),
            Arc::new(self.table_name.finish()),
            Arc::new(self.column_name.finish()),
            Arc::new(self.ordinal_position.finish()),
            Arc::new(self.position_in_unique_constraint.finish()),
            null_string_vector.clone(),
            null_string_vector.clone(),
            null_string_vector,
            Arc::new(self.greptime_index_type.finish()),
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
                    .make_key_column_usage(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
