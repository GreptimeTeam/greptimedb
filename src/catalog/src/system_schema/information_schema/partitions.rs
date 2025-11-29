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

use core::pin::pin;
use std::sync::{Arc, Weak};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::INFORMATION_SCHEMA_PARTITIONS_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::timestamp::TimestampSecond;
use datatypes::value::Value;
use datatypes::vectors::{
    ConstantVector, Int64Vector, Int64VectorBuilder, MutableVector, StringVector,
    StringVectorBuilder, TimestampSecondVector, TimestampSecondVectorBuilder, UInt64VectorBuilder,
};
use futures::{StreamExt, TryStreamExt};
use partition::manager::PartitionInfo;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};
use table::metadata::{TableInfo, TableType};

use crate::CatalogManager;
use crate::error::{
    CreateRecordBatchSnafu, FindPartitionsSnafu, InternalSnafu, PartitionManagerNotFoundSnafu,
    Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::kvbackend::KvBackendCatalogManager;
use crate::system_schema::information_schema::{InformationTable, PARTITIONS, Predicates};

const TABLE_CATALOG: &str = "table_catalog";
const TABLE_SCHEMA: &str = "table_schema";
const TABLE_NAME: &str = "table_name";
const PARTITION_NAME: &str = "partition_name";
const PARTITION_EXPRESSION: &str = "partition_expression";
/// The region id
const GREPTIME_PARTITION_ID: &str = "greptime_partition_id";
const INIT_CAPACITY: usize = 42;

/// The `PARTITIONS` table provides information about partitioned tables.
/// See https://dev.mysql.com/doc/refman/8.0/en/information-schema-partitions-table.html
/// We provide an extral column `greptime_partition_id` for GreptimeDB region id.
#[derive(Debug)]
pub(super) struct InformationSchemaPartitions {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaPartitions {
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
            ColumnSchema::new(PARTITION_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "subpartition_name",
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(
                "partition_ordinal_position",
                ConcreteDataType::int64_datatype(),
                true,
            ),
            ColumnSchema::new(
                "subpartition_ordinal_position",
                ConcreteDataType::int64_datatype(),
                true,
            ),
            ColumnSchema::new(
                "partition_method",
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(
                "subpartition_method",
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(
                PARTITION_EXPRESSION,
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(
                "subpartition_expression",
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(
                "partition_description",
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new("table_rows", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new("avg_row_length", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new("data_length", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new("max_data_length", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new("index_length", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new("data_free", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new(
                "create_time",
                ConcreteDataType::timestamp_second_datatype(),
                true,
            ),
            ColumnSchema::new(
                "update_time",
                ConcreteDataType::timestamp_second_datatype(),
                true,
            ),
            ColumnSchema::new(
                "check_time",
                ConcreteDataType::timestamp_second_datatype(),
                true,
            ),
            ColumnSchema::new("checksum", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new(
                "partition_comment",
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new("nodegroup", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("tablespace_name", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                GREPTIME_PARTITION_ID,
                ConcreteDataType::uint64_datatype(),
                true,
            ),
        ]))
    }

    fn builder(&self) -> InformationSchemaPartitionsBuilder {
        InformationSchemaPartitionsBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaPartitions {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_PARTITIONS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        PARTITIONS
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
                    .make_partitions(Some(request))
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

struct InformationSchemaPartitionsBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    catalog_names: StringVectorBuilder,
    schema_names: StringVectorBuilder,
    table_names: StringVectorBuilder,
    partition_names: StringVectorBuilder,
    partition_ordinal_positions: Int64VectorBuilder,
    partition_expressions: StringVectorBuilder,
    partition_descriptions: StringVectorBuilder,
    create_times: TimestampSecondVectorBuilder,
    partition_ids: UInt64VectorBuilder,
}

impl InformationSchemaPartitionsBuilder {
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
            partition_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            partition_ordinal_positions: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            partition_expressions: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            partition_descriptions: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            create_times: TimestampSecondVectorBuilder::with_capacity(INIT_CAPACITY),
            partition_ids: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.partitions` virtual table
    async fn make_partitions(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;

        let partition_manager = catalog_manager
            .as_any()
            .downcast_ref::<KvBackendCatalogManager>()
            .map(|catalog_manager| catalog_manager.partition_manager())
            .context(PartitionManagerNotFoundSnafu)?;

        let predicates = Predicates::from_scan_request(&request);

        for schema_name in catalog_manager.schema_names(&catalog_name, None).await? {
            let table_info_stream = catalog_manager
                .tables(&catalog_name, &schema_name, None)
                .try_filter_map(|t| async move {
                    let table_info = t.table_info();
                    if table_info.table_type == TableType::Temporary {
                        Ok(None)
                    } else {
                        Ok(Some(table_info))
                    }
                });

            const BATCH_SIZE: usize = 128;

            // Split table infos into chunks
            let mut table_info_chunks = pin!(table_info_stream.ready_chunks(BATCH_SIZE));

            while let Some(table_infos) = table_info_chunks.next().await {
                let table_infos = table_infos.into_iter().collect::<Result<Vec<_>>>()?;
                let table_ids: Vec<TableId> =
                    table_infos.iter().map(|info| info.ident.table_id).collect();

                let mut table_partitions = partition_manager
                    .batch_find_table_partitions(&table_ids)
                    .await
                    .context(FindPartitionsSnafu)?;

                for table_info in table_infos {
                    let partitions = table_partitions
                        .remove(&table_info.ident.table_id)
                        .unwrap_or(vec![]);

                    self.add_partitions(
                        &predicates,
                        &table_info,
                        &catalog_name,
                        &schema_name,
                        &table_info.name,
                        &partitions,
                    );
                }
            }
        }

        self.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn add_partitions(
        &mut self,
        predicates: &Predicates,
        table_info: &TableInfo,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        partitions: &[PartitionInfo],
    ) {
        let row = [
            (TABLE_CATALOG, &Value::from(catalog_name)),
            (TABLE_SCHEMA, &Value::from(schema_name)),
            (TABLE_NAME, &Value::from(table_name)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        for (index, partition) in partitions.iter().enumerate() {
            let partition_name = format!("p{index}");

            self.catalog_names.push(Some(catalog_name));
            self.schema_names.push(Some(schema_name));
            self.table_names.push(Some(table_name));
            self.partition_names.push(Some(&partition_name));
            self.partition_ordinal_positions
                .push(Some((index + 1) as i64));
            let expression = partition.partition_expr.as_ref().map(|e| e.to_string());
            self.partition_expressions.push(expression.as_deref());
            // Use partition boundary/expression as partition_description for MySQL/StarRocks compatibility
            self.partition_descriptions.push(expression.as_deref());
            self.create_times.push(Some(TimestampSecond::from(
                table_info.meta.created_on.timestamp(),
            )));
            self.partition_ids.push(Some(partition.id.as_u64()));
        }
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let rows_num = self.catalog_names.len();

        let null_string_vector = Arc::new(ConstantVector::new(
            Arc::new(StringVector::from(vec![None as Option<&str>])),
            rows_num,
        ));
        let null_i64_vector = Arc::new(ConstantVector::new(
            Arc::new(Int64Vector::from(vec![None])),
            rows_num,
        ));
        let null_timestamp_second_vector = Arc::new(ConstantVector::new(
            Arc::new(TimestampSecondVector::from(vec![None])),
            rows_num,
        ));
        let partition_methods = Arc::new(ConstantVector::new(
            Arc::new(StringVector::from(vec![Some("RANGE")])),
            rows_num,
        ));

        let columns: Vec<VectorRef> = vec![
            Arc::new(self.catalog_names.finish()),
            Arc::new(self.schema_names.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.partition_names.finish()),
            null_string_vector.clone(),
            Arc::new(self.partition_ordinal_positions.finish()),
            null_i64_vector.clone(),
            partition_methods,
            null_string_vector.clone(),
            Arc::new(self.partition_expressions.finish()),
            null_string_vector.clone(),
            Arc::new(self.partition_descriptions.finish()),
            // TODO(dennis): rows and index statistics info
            null_i64_vector.clone(),
            null_i64_vector.clone(),
            null_i64_vector.clone(),
            null_i64_vector.clone(),
            null_i64_vector.clone(),
            null_i64_vector.clone(),
            Arc::new(self.create_times.finish()),
            // TODO(dennis): supports update_time
            null_timestamp_second_vector.clone(),
            null_timestamp_second_vector,
            null_i64_vector,
            null_string_vector.clone(),
            null_string_vector.clone(),
            null_string_vector,
            Arc::new(self.partition_ids.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaPartitions {
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
                    .make_partitions(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
