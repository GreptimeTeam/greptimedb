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
use common_catalog::consts::INFORMATION_SCHEMA_TABLES_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{
    DateTimeVectorBuilder, StringVectorBuilder, UInt32VectorBuilder, UInt64VectorBuilder,
};
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};
use table::metadata::{TableInfo, TableType};

use super::TABLES;
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::system_schema::information_schema::{InformationTable, Predicates};
use crate::CatalogManager;

pub const TABLE_CATALOG: &str = "table_catalog";
pub const TABLE_SCHEMA: &str = "table_schema";
pub const TABLE_NAME: &str = "table_name";
pub const TABLE_TYPE: &str = "table_type";
pub const VERSION: &str = "version";
pub const ROW_FORMAT: &str = "row_format";
pub const TABLE_ROWS: &str = "table_rows";
pub const DATA_LENGTH: &str = "data_length";
pub const INDEX_LENGTH: &str = "index_length";
pub const MAX_DATA_LENGTH: &str = "max_data_length";
pub const AVG_ROW_LENGTH: &str = "avg_row_length";
pub const DATA_FREE: &str = "data_free";
pub const AUTO_INCREMENT: &str = "auto_increment";
pub const CREATE_TIME: &str = "create_time";
pub const UPDATE_TIME: &str = "update_time";
pub const CHECK_TIME: &str = "check_time";
pub const TABLE_COLLATION: &str = "table_collation";
pub const CHECKSUM: &str = "checksum";
pub const CREATE_OPTIONS: &str = "create_options";
pub const TABLE_COMMENT: &str = "table_comment";
pub const MAX_INDEX_LENGTH: &str = "max_index_length";
pub const TEMPORARY: &str = "temporary";
const TABLE_ID: &str = "table_id";
pub const ENGINE: &str = "engine";
const INIT_CAPACITY: usize = 42;

pub(super) struct InformationSchemaTables {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaTables {
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
            ColumnSchema::new(TABLE_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_ID, ConcreteDataType::uint32_datatype(), true),
            ColumnSchema::new(DATA_LENGTH, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(MAX_DATA_LENGTH, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(INDEX_LENGTH, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(MAX_INDEX_LENGTH, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(AVG_ROW_LENGTH, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(ENGINE, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(VERSION, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(ROW_FORMAT, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(TABLE_ROWS, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(DATA_FREE, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(AUTO_INCREMENT, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(CREATE_TIME, ConcreteDataType::datetime_datatype(), true),
            ColumnSchema::new(UPDATE_TIME, ConcreteDataType::datetime_datatype(), true),
            ColumnSchema::new(CHECK_TIME, ConcreteDataType::datetime_datatype(), true),
            ColumnSchema::new(TABLE_COLLATION, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(CHECKSUM, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(CREATE_OPTIONS, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(TABLE_COMMENT, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(TEMPORARY, ConcreteDataType::string_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaTablesBuilder {
        InformationSchemaTablesBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaTables {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_TABLES_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        TABLES
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
                    .make_tables(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

/// Builds the `information_schema.TABLE` table row by row
///
/// Columns are based on <https://www.postgresql.org/docs/current/infoschema-columns.html>
struct InformationSchemaTablesBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    catalog_names: StringVectorBuilder,
    schema_names: StringVectorBuilder,
    table_names: StringVectorBuilder,
    table_types: StringVectorBuilder,
    table_ids: UInt32VectorBuilder,
    version: UInt64VectorBuilder,
    row_format: StringVectorBuilder,
    table_rows: UInt64VectorBuilder,
    data_length: UInt64VectorBuilder,
    max_data_length: UInt64VectorBuilder,
    index_length: UInt64VectorBuilder,
    avg_row_length: UInt64VectorBuilder,
    max_index_length: UInt64VectorBuilder,
    data_free: UInt64VectorBuilder,
    auto_increment: UInt64VectorBuilder,
    create_time: DateTimeVectorBuilder,
    update_time: DateTimeVectorBuilder,
    check_time: DateTimeVectorBuilder,
    table_collation: StringVectorBuilder,
    checksum: UInt64VectorBuilder,
    create_options: StringVectorBuilder,
    table_comment: StringVectorBuilder,
    engines: StringVectorBuilder,
    temporary: StringVectorBuilder,
}

impl InformationSchemaTablesBuilder {
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
            table_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_ids: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            data_length: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            max_data_length: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            index_length: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            avg_row_length: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            engines: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            version: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            row_format: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_rows: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            max_index_length: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            data_free: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            auto_increment: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            create_time: DateTimeVectorBuilder::with_capacity(INIT_CAPACITY),
            update_time: DateTimeVectorBuilder::with_capacity(INIT_CAPACITY),
            check_time: DateTimeVectorBuilder::with_capacity(INIT_CAPACITY),
            table_collation: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            checksum: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            create_options: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_comment: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            temporary: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.tables` virtual table
    async fn make_tables(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let predicates = Predicates::from_scan_request(&request);

        for schema_name in catalog_manager.schema_names(&catalog_name).await? {
            let mut stream = catalog_manager.tables(&catalog_name, &schema_name);

            while let Some(table) = stream.try_next().await? {
                let table_info = table.table_info();
                self.add_table(
                    &predicates,
                    &catalog_name,
                    &schema_name,
                    table_info,
                    table.table_type(),
                );
            }
        }

        self.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn add_table(
        &mut self,
        predicates: &Predicates,
        catalog_name: &str,
        schema_name: &str,
        table_info: Arc<TableInfo>,
        table_type: TableType,
    ) {
        let table_name = table_info.name.as_ref();
        let table_id = table_info.table_id();
        let engine = table_info.meta.engine.as_ref();

        let table_type_text = match table_type {
            TableType::Base => "BASE TABLE",
            TableType::View => "VIEW",
            TableType::Temporary => "LOCAL TEMPORARY",
        };

        let row = [
            (TABLE_CATALOG, &Value::from(catalog_name)),
            (TABLE_SCHEMA, &Value::from(schema_name)),
            (TABLE_NAME, &Value::from(table_name)),
            (TABLE_TYPE, &Value::from(table_type_text)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.catalog_names.push(Some(catalog_name));
        self.schema_names.push(Some(schema_name));
        self.table_names.push(Some(table_name));
        self.table_types.push(Some(table_type_text));
        self.table_ids.push(Some(table_id));
        // TODO(sunng87): use real data for these fields
        self.data_length.push(Some(0));
        self.max_data_length.push(Some(0));
        self.index_length.push(Some(0));
        self.avg_row_length.push(Some(0));
        self.max_index_length.push(Some(0));
        self.checksum.push(Some(0));
        self.table_rows.push(Some(0));
        self.data_free.push(Some(0));
        self.auto_increment.push(Some(0));
        self.row_format.push(Some("Fixed"));
        self.table_collation.push(Some("utf8_bin"));
        self.update_time.push(None);
        self.check_time.push(None);

        // use mariadb default table version number here
        self.version.push(Some(11));
        self.table_comment.push(table_info.desc.as_deref());
        self.create_options
            .push(Some(table_info.meta.options.to_string().as_ref()));
        self.create_time
            .push(Some(table_info.meta.created_on.timestamp_millis().into()));

        self.temporary
            .push(if matches!(table_type, TableType::Temporary) {
                Some("Y")
            } else {
                Some("N")
            });
        self.engines.push(Some(engine));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.catalog_names.finish()),
            Arc::new(self.schema_names.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.table_types.finish()),
            Arc::new(self.table_ids.finish()),
            Arc::new(self.data_length.finish()),
            Arc::new(self.max_data_length.finish()),
            Arc::new(self.index_length.finish()),
            Arc::new(self.max_index_length.finish()),
            Arc::new(self.avg_row_length.finish()),
            Arc::new(self.engines.finish()),
            Arc::new(self.version.finish()),
            Arc::new(self.row_format.finish()),
            Arc::new(self.table_rows.finish()),
            Arc::new(self.data_free.finish()),
            Arc::new(self.auto_increment.finish()),
            Arc::new(self.create_time.finish()),
            Arc::new(self.update_time.finish()),
            Arc::new(self.check_time.finish()),
            Arc::new(self.table_collation.finish()),
            Arc::new(self.checksum.finish()),
            Arc::new(self.create_options.finish()),
            Arc::new(self.table_comment.finish()),
            Arc::new(self.temporary.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaTables {
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
                    .make_tables(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
