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
use common_catalog::consts::INFORMATION_SCHEMA_STATISTICS_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, FulltextBackend, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{Int64VectorBuilder, StringVectorBuilder};
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};

use crate::CatalogManager;
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::system_schema::information_schema::key_column_usage::{
    CONSTRAINT_NAME_FULLTEXT_INDEX, CONSTRAINT_NAME_INVERTED_INDEX, CONSTRAINT_NAME_PRI,
    CONSTRAINT_NAME_SKIPPING_INDEX, CONSTRAINT_NAME_TIME_INDEX, INDEX_TYPE_FULLTEXT_BLOOM,
    INDEX_TYPE_FULLTEXT_TANTIVY, INDEX_TYPE_INVERTED_INDEX, INDEX_TYPE_PRI,
    INDEX_TYPE_SKIPPING_INDEX,
};
use crate::system_schema::information_schema::{InformationTable, Predicates, STATISTICS};

pub const TABLE_CATALOG: &str = "table_catalog";
pub const TABLE_SCHEMA: &str = "table_schema";
pub const TABLE_NAME: &str = "table_name";
pub const NON_UNIQUE: &str = "non_unique";
pub const INDEX_SCHEMA: &str = "index_schema";
pub const INDEX_NAME: &str = "index_name";
pub const SEQ_IN_INDEX: &str = "seq_in_index";
pub const COLUMN_NAME: &str = "column_name";
pub const COLLATION: &str = "collation";
pub const CARDINALITY: &str = "cardinality";
pub const SUB_PART: &str = "sub_part";
pub const PACKED: &str = "packed";
pub const NULLABLE: &str = "nullable";
pub const INDEX_TYPE: &str = "index_type";
pub const COMMENT: &str = "comment";
pub const INDEX_COMMENT: &str = "index_comment";
pub const IS_VISIBLE: &str = "is_visible";
pub const EXPRESSION: &str = "expression";
pub const GREPTIME_INDEX_TYPE: &str = "greptime_index_type";

const INIT_CAPACITY: usize = 42;
const MYSQL_DEFAULT_CATALOG: &str = "def";
const ASCENDING_COLLATION: &str = "A";
const YES: &str = "YES";
const NO: &str = "NO";
const EMPTY: &str = "";
const BTREE: &str = "BTREE";
const FULLTEXT: &str = "FULLTEXT";
const INVERTED: &str = "INVERTED";
const BLOOM: &str = "BLOOM";

/// The `information_schema.statistics` table provides index metadata in a
/// MySQL-compatible shape.
#[derive(Debug)]
pub(super) struct InformationSchemaStatistics {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaStatistics {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(TABLE_CATALOG, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_SCHEMA, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(NON_UNIQUE, ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new(INDEX_SCHEMA, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(INDEX_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(SEQ_IN_INDEX, ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new(COLUMN_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(COLLATION, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(CARDINALITY, ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new(SUB_PART, ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new(PACKED, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(NULLABLE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(INDEX_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(COMMENT, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(INDEX_COMMENT, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(IS_VISIBLE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(EXPRESSION, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                GREPTIME_INDEX_TYPE,
                ConcreteDataType::string_datatype(),
                true,
            ),
        ]))
    }

    fn builder(&self) -> InformationSchemaStatisticsBuilder {
        InformationSchemaStatisticsBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaStatistics {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_STATISTICS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        STATISTICS
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
                    .make_statistics(Some(request))
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

struct InformationSchemaStatisticsBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    table_catalogs: StringVectorBuilder,
    table_schemas: StringVectorBuilder,
    table_names: StringVectorBuilder,
    non_unique: Int64VectorBuilder,
    index_schemas: StringVectorBuilder,
    index_names: StringVectorBuilder,
    seq_in_index: Int64VectorBuilder,
    column_names: StringVectorBuilder,
    collations: StringVectorBuilder,
    cardinalities: Int64VectorBuilder,
    sub_parts: Int64VectorBuilder,
    packed: StringVectorBuilder,
    nullable: StringVectorBuilder,
    index_types: StringVectorBuilder,
    comments: StringVectorBuilder,
    index_comments: StringVectorBuilder,
    is_visible: StringVectorBuilder,
    expressions: StringVectorBuilder,
    greptime_index_types: StringVectorBuilder,
}

impl InformationSchemaStatisticsBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            table_catalogs: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_schemas: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            non_unique: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            index_schemas: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            index_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            seq_in_index: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            column_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            collations: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            cardinalities: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            sub_parts: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            packed: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            nullable: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            index_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            comments: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            index_comments: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            is_visible: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            expressions: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            greptime_index_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    async fn make_statistics(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
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

                for (primary_key_seq, idx) in keys.iter().enumerate() {
                    if let Some(column) = schema.column_schemas().get(*idx) {
                        self.add_statistics(
                            &predicates,
                            &schema_name,
                            table_name,
                            0,
                            CONSTRAINT_NAME_PRI,
                            primary_key_seq as i64 + 1,
                            column,
                            BTREE,
                            INDEX_TYPE_PRI,
                        );
                    }
                }

                let mut inverted_seq = 0;
                let mut fulltext_seq = 0;
                let mut skipping_seq = 0;

                for column in schema.column_schemas() {
                    if column.is_time_index() {
                        self.add_statistics(
                            &predicates,
                            &schema_name,
                            table_name,
                            1,
                            CONSTRAINT_NAME_TIME_INDEX,
                            1,
                            column,
                            BTREE,
                            EMPTY,
                        );
                    }
                    if column.is_inverted_indexed() {
                        inverted_seq += 1;
                        self.add_statistics(
                            &predicates,
                            &schema_name,
                            table_name,
                            1,
                            CONSTRAINT_NAME_INVERTED_INDEX,
                            inverted_seq,
                            column,
                            INVERTED,
                            INDEX_TYPE_INVERTED_INDEX,
                        );
                    }
                    if let Ok(Some(options)) = column.fulltext_options()
                        && options.enable
                    {
                        let greptime_index_type = match options.backend {
                            FulltextBackend::Bloom => INDEX_TYPE_FULLTEXT_BLOOM,
                            FulltextBackend::Tantivy => INDEX_TYPE_FULLTEXT_TANTIVY,
                        };
                        fulltext_seq += 1;
                        self.add_statistics(
                            &predicates,
                            &schema_name,
                            table_name,
                            1,
                            CONSTRAINT_NAME_FULLTEXT_INDEX,
                            fulltext_seq,
                            column,
                            FULLTEXT,
                            greptime_index_type,
                        );
                    }
                    if column.is_skipping_indexed() {
                        skipping_seq += 1;
                        self.add_statistics(
                            &predicates,
                            &schema_name,
                            table_name,
                            1,
                            CONSTRAINT_NAME_SKIPPING_INDEX,
                            skipping_seq,
                            column,
                            BLOOM,
                            INDEX_TYPE_SKIPPING_INDEX,
                        );
                    }
                }
            }
        }

        self.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn add_statistics(
        &mut self,
        predicates: &Predicates,
        table_schema: &str,
        table_name: &str,
        non_unique: i64,
        index_name: &str,
        seq_in_index: i64,
        column: &ColumnSchema,
        index_type: &str,
        greptime_index_type: &str,
    ) {
        let nullable = if column.is_nullable() { YES } else { NO };
        let row = [
            (TABLE_CATALOG, &Value::from(MYSQL_DEFAULT_CATALOG)),
            (TABLE_SCHEMA, &Value::from(table_schema)),
            (TABLE_NAME, &Value::from(table_name)),
            (NON_UNIQUE, &Value::from(non_unique)),
            (INDEX_SCHEMA, &Value::from(table_schema)),
            (INDEX_NAME, &Value::from(index_name)),
            (SEQ_IN_INDEX, &Value::from(seq_in_index)),
            (COLUMN_NAME, &Value::from(column.name.as_str())),
            (NULLABLE, &Value::from(nullable)),
            (INDEX_TYPE, &Value::from(index_type)),
            (GREPTIME_INDEX_TYPE, &Value::from(greptime_index_type)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.table_catalogs.push(Some(MYSQL_DEFAULT_CATALOG));
        self.table_schemas.push(Some(table_schema));
        self.table_names.push(Some(table_name));
        self.non_unique.push(Some(non_unique));
        self.index_schemas.push(Some(table_schema));
        self.index_names.push(Some(index_name));
        self.seq_in_index.push(Some(seq_in_index));
        self.column_names.push(Some(&column.name));
        self.collations.push(Some(ASCENDING_COLLATION));
        self.cardinalities.push(None);
        self.sub_parts.push(None);
        self.packed.push(None);
        self.nullable.push(Some(nullable));
        self.index_types.push(Some(index_type));
        self.comments.push(Some(EMPTY));
        self.index_comments.push(Some(EMPTY));
        self.is_visible.push(Some(YES));
        self.expressions.push(None);
        self.greptime_index_types.push(Some(greptime_index_type));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.table_catalogs.finish()),
            Arc::new(self.table_schemas.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.non_unique.finish()),
            Arc::new(self.index_schemas.finish()),
            Arc::new(self.index_names.finish()),
            Arc::new(self.seq_in_index.finish()),
            Arc::new(self.column_names.finish()),
            Arc::new(self.collations.finish()),
            Arc::new(self.cardinalities.finish()),
            Arc::new(self.sub_parts.finish()),
            Arc::new(self.packed.finish()),
            Arc::new(self.nullable.finish()),
            Arc::new(self.index_types.finish()),
            Arc::new(self.comments.finish()),
            Arc::new(self.index_comments.finish()),
            Arc::new(self.is_visible.finish()),
            Arc::new(self.expressions.finish()),
            Arc::new(self.greptime_index_types.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaStatistics {
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
                    .make_statistics(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
