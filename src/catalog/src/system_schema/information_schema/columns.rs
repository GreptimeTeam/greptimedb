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
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::prelude::{ConcreteDataType, DataType, MutableVector};
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{
    ConstantVector, Int64Vector, Int64VectorBuilder, StringVector, StringVectorBuilder, VectorRef,
};
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use sql::statements;
use store_api::storage::{ScanRequest, TableId};

use crate::CatalogManager;
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::Predicates;
use crate::system_schema::information_schema::{COLUMNS, InformationTable};

#[derive(Debug)]
pub(super) struct InformationSchemaColumns {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

pub const TABLE_CATALOG: &str = "table_catalog";
pub const TABLE_SCHEMA: &str = "table_schema";
pub const TABLE_NAME: &str = "table_name";
pub const COLUMN_NAME: &str = "column_name";
pub const REGION_ID: &str = "region_id";
pub const PEER_ID: &str = "peer_id";
const ORDINAL_POSITION: &str = "ordinal_position";
const CHARACTER_MAXIMUM_LENGTH: &str = "character_maximum_length";
const CHARACTER_OCTET_LENGTH: &str = "character_octet_length";
const NUMERIC_PRECISION: &str = "numeric_precision";
const NUMERIC_SCALE: &str = "numeric_scale";
const DATETIME_PRECISION: &str = "datetime_precision";
const CHARACTER_SET_NAME: &str = "character_set_name";
pub const COLLATION_NAME: &str = "collation_name";
pub const COLUMN_KEY: &str = "column_key";
pub const EXTRA: &str = "extra";
pub const PRIVILEGES: &str = "privileges";
const GENERATION_EXPRESSION: &str = "generation_expression";
// Extension field to keep greptime data type name
pub const GREPTIME_DATA_TYPE: &str = "greptime_data_type";
pub const DATA_TYPE: &str = "data_type";
pub const SEMANTIC_TYPE: &str = "semantic_type";
pub const COLUMN_DEFAULT: &str = "column_default";
pub const IS_NULLABLE: &str = "is_nullable";
const COLUMN_TYPE: &str = "column_type";
pub const COLUMN_COMMENT: &str = "column_comment";
const SRS_ID: &str = "srs_id";
const INIT_CAPACITY: usize = 42;

// The maximum length of string type
const MAX_STRING_LENGTH: i64 = 2147483647;
const UTF8_CHARSET_NAME: &str = "utf8";
const UTF8_COLLATE_NAME: &str = "utf8_bin";
const PRI_COLUMN_KEY: &str = "PRI";
const TIME_INDEX_COLUMN_KEY: &str = "TIME INDEX";
const DEFAULT_PRIVILEGES: &str = "select,insert";
const EMPTY_STR: &str = "";

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
            ColumnSchema::new(ORDINAL_POSITION, ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new(
                CHARACTER_MAXIMUM_LENGTH,
                ConcreteDataType::int64_datatype(),
                true,
            ),
            ColumnSchema::new(
                CHARACTER_OCTET_LENGTH,
                ConcreteDataType::int64_datatype(),
                true,
            ),
            ColumnSchema::new(NUMERIC_PRECISION, ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new(NUMERIC_SCALE, ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new(DATETIME_PRECISION, ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new(
                CHARACTER_SET_NAME,
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(COLLATION_NAME, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(COLUMN_KEY, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(EXTRA, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(PRIVILEGES, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                GENERATION_EXPRESSION,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                GREPTIME_DATA_TYPE,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(DATA_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(SEMANTIC_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(COLUMN_DEFAULT, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(IS_NULLABLE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(COLUMN_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(COLUMN_COMMENT, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(SRS_ID, ConcreteDataType::int64_datatype(), true),
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
    ordinal_positions: Int64VectorBuilder,
    character_maximum_lengths: Int64VectorBuilder,
    character_octet_lengths: Int64VectorBuilder,
    numeric_precisions: Int64VectorBuilder,
    numeric_scales: Int64VectorBuilder,
    datetime_precisions: Int64VectorBuilder,
    character_set_names: StringVectorBuilder,
    collation_names: StringVectorBuilder,
    column_keys: StringVectorBuilder,
    greptime_data_types: StringVectorBuilder,
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
            catalog_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            schema_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            column_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            ordinal_positions: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            character_maximum_lengths: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            character_octet_lengths: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            numeric_precisions: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            numeric_scales: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            datetime_precisions: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            character_set_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            collation_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            column_keys: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            greptime_data_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            data_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            semantic_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            column_defaults: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            is_nullables: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            column_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            column_comments: StringVectorBuilder::with_capacity(INIT_CAPACITY),
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

        for schema_name in catalog_manager.schema_names(&catalog_name, None).await? {
            let mut stream = catalog_manager.tables(&catalog_name, &schema_name, None);

            while let Some(table) = stream.try_next().await? {
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
                        idx,
                        &catalog_name,
                        &schema_name,
                        &table.table_info().name,
                        semantic_type,
                        column,
                    );
                }
            }
        }

        self.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn add_column(
        &mut self,
        predicates: &Predicates,
        index: usize,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        semantic_type: &str,
        column_schema: &ColumnSchema,
    ) {
        // Use sql data type name
        let data_type = statements::concrete_data_type_to_sql_data_type(&column_schema.data_type)
            .map(|dt| dt.to_string().to_lowercase())
            .unwrap_or_else(|_| column_schema.data_type.name());

        let column_key = match semantic_type {
            SEMANTIC_TYPE_PRIMARY_KEY => PRI_COLUMN_KEY,
            SEMANTIC_TYPE_TIME_INDEX => TIME_INDEX_COLUMN_KEY,
            _ => EMPTY_STR,
        };

        let row = [
            (TABLE_CATALOG, &Value::from(catalog_name)),
            (TABLE_SCHEMA, &Value::from(schema_name)),
            (TABLE_NAME, &Value::from(table_name)),
            (COLUMN_NAME, &Value::from(column_schema.name.as_str())),
            (DATA_TYPE, &Value::from(data_type.as_str())),
            (SEMANTIC_TYPE, &Value::from(semantic_type)),
            (ORDINAL_POSITION, &Value::from((index + 1) as i64)),
            (COLUMN_KEY, &Value::from(column_key)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.catalog_names.push(Some(catalog_name));
        self.schema_names.push(Some(schema_name));
        self.table_names.push(Some(table_name));
        self.column_names.push(Some(&column_schema.name));
        // Starts from 1
        self.ordinal_positions.push(Some((index + 1) as i64));

        if column_schema.data_type.is_string() {
            self.character_maximum_lengths.push(Some(MAX_STRING_LENGTH));
            self.character_octet_lengths.push(Some(MAX_STRING_LENGTH));
            self.numeric_precisions.push(None);
            self.numeric_scales.push(None);
            self.datetime_precisions.push(None);
            self.character_set_names.push(Some(UTF8_CHARSET_NAME));
            self.collation_names.push(Some(UTF8_COLLATE_NAME));
        } else if column_schema.data_type.is_numeric() || column_schema.data_type.is_decimal() {
            self.character_maximum_lengths.push(None);
            self.character_octet_lengths.push(None);

            self.numeric_precisions.push(
                column_schema
                    .data_type
                    .numeric_precision()
                    .map(|x| x as i64),
            );
            self.numeric_scales
                .push(column_schema.data_type.numeric_scale().map(|x| x as i64));

            self.datetime_precisions.push(None);
            self.character_set_names.push(None);
            self.collation_names.push(None);
        } else {
            self.character_maximum_lengths.push(None);
            self.character_octet_lengths.push(None);
            self.numeric_precisions.push(None);
            self.numeric_scales.push(None);

            match &column_schema.data_type {
                ConcreteDataType::Timestamp(ts_type) => {
                    self.datetime_precisions
                        .push(Some(ts_type.precision() as i64));
                }
                ConcreteDataType::Time(time_type) => {
                    self.datetime_precisions
                        .push(Some(time_type.precision() as i64));
                }
                _ => self.datetime_precisions.push(None),
            }

            self.character_set_names.push(None);
            self.collation_names.push(None);
        }

        self.column_keys.push(Some(column_key));
        self.greptime_data_types
            .push(Some(&column_schema.data_type.name()));
        self.data_types.push(Some(&data_type));
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
        self.column_types.push(Some(&data_type));
        let column_comment = column_schema.column_comment().map(|x| x.as_ref());
        self.column_comments.push(column_comment);
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let rows_num = self.collation_names.len();

        let privileges = Arc::new(ConstantVector::new(
            Arc::new(StringVector::from(vec![DEFAULT_PRIVILEGES])),
            rows_num,
        ));
        let empty_string = Arc::new(ConstantVector::new(
            Arc::new(StringVector::from(vec![EMPTY_STR])),
            rows_num,
        ));
        let srs_ids = Arc::new(ConstantVector::new(
            Arc::new(Int64Vector::from(vec![None])),
            rows_num,
        ));

        let columns: Vec<VectorRef> = vec![
            Arc::new(self.catalog_names.finish()),
            Arc::new(self.schema_names.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.column_names.finish()),
            Arc::new(self.ordinal_positions.finish()),
            Arc::new(self.character_maximum_lengths.finish()),
            Arc::new(self.character_octet_lengths.finish()),
            Arc::new(self.numeric_precisions.finish()),
            Arc::new(self.numeric_scales.finish()),
            Arc::new(self.datetime_precisions.finish()),
            Arc::new(self.character_set_names.finish()),
            Arc::new(self.collation_names.finish()),
            Arc::new(self.column_keys.finish()),
            empty_string.clone(),
            privileges,
            empty_string,
            Arc::new(self.greptime_data_types.finish()),
            Arc::new(self.data_types.finish()),
            Arc::new(self.semantic_types.finish()),
            Arc::new(self.column_defaults.finish()),
            Arc::new(self.is_nullables.finish()),
            Arc::new(self.column_types.finish()),
            Arc::new(self.column_comments.finish()),
            srs_ids,
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
