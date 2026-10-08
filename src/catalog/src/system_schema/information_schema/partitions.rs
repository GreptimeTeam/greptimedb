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
    Int64Vector, Int64VectorBuilder, MutableVector, StringVector, StringVectorBuilder,
    TimestampSecondVector, TimestampSecondVectorBuilder, UInt64VectorBuilder,
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

        // Fast path: when the predicates pin both the schema and the table name
        // with equality, resolve the table directly instead of enumerating all
        // tables in all schemas and fetching partition metadata for each of them.
        if let (Some(schema_name), Some(table_name)) = (
            predicates
                .find_eq_value(TABLE_SCHEMA)
                .and_then(Value::as_string),
            predicates
                .find_eq_value(TABLE_NAME)
                .and_then(Value::as_string),
        ) {
            if let Some(table) = catalog_manager
                .table(&catalog_name, &schema_name, &table_name, None)
                .await?
            {
                let table_info = table.table_info();

                // The enumeration path below skips temporary tables, keep the
                // behavior identical.
                if table_info.table_type != TableType::Temporary {
                    let mut table_partitions = partition_manager
                        .batch_find_table_partitions(&[table_info.table_id()])
                        .await
                        .context(FindPartitionsSnafu)?;

                    let partitions = table_partitions
                        .remove(&table_info.table_id())
                        .unwrap_or_default();

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

            // The pinned table fully determines the result: the table above was
            // found (or not) unambiguously, so no other table can contribute rows.
            return self.finish();
        }

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

        // Get partition column names (shared by all partitions)
        // In MySQL, PARTITION_EXPRESSION is the partitioning function expression (e.g., column name)
        let partition_columns: String = table_info
            .meta
            .partition_column_names()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");

        let partition_expr_str = if partition_columns.is_empty() {
            None
        } else {
            Some(partition_columns)
        };

        for (index, partition) in partitions.iter().enumerate() {
            let partition_name = format!("p{index}");

            self.catalog_names.push(Some(catalog_name));
            self.schema_names.push(Some(schema_name));
            self.table_names.push(Some(table_name));
            self.partition_names.push(Some(&partition_name));
            self.partition_ordinal_positions
                .push(Some((index + 1) as i64));
            // PARTITION_EXPRESSION: partition column names (same for all partitions)
            self.partition_expressions
                .push(partition_expr_str.as_deref());
            // PARTITION_DESCRIPTION: partition boundary expression (different for each partition)
            let description = partition.partition_expr.as_ref().map(|e| e.to_string());
            self.partition_descriptions.push(description.as_deref());
            self.create_times.push(Some(TimestampSecond::from(
                table_info.meta.created_on.timestamp(),
            )));
            self.partition_ids.push(Some(partition.id.as_u64()));
        }
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let rows_num = self.catalog_names.len();

        let null_string_vector = Arc::new(StringVector::from(vec![None as Option<&str>; rows_num]));
        let null_i64_vector = Arc::new(Int64Vector::from(vec![None; rows_num]));
        let null_timestamp_second_vector =
            Arc::new(TimestampSecondVector::from(vec![None; rows_num]));
        let partition_methods = Arc::new(StringVector::from(vec![Some("RANGE"); rows_num]));

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

#[cfg(test)]
mod tests {
    use api::v1::meta::Peer;
    use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, INFORMATION_SCHEMA_NAME};
    use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
    use common_meta::key::schema_name::SchemaNameKey;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::wal_provider::RegionWalOptions;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator, col, lit};
    use futures::TryStreamExt;
    use store_api::storage::RegionId;

    use super::*;
    use crate::information_schema::NoopInformationExtension;
    use crate::kvbackend::KvBackendCatalogManagerBuilder;

    const SCHEMA_1: &str = "partitions_schema_1";
    const SCHEMA_2: &str = "partitions_schema_2";
    const PARTITIONED_TABLE: &str = "partitioned_table";
    const MISSING_TABLE: &str = "missing_table";
    const TABLE_ID_1: TableId = 1;
    const TABLE_ID_2: TableId = 2;
    const REGION_NUMBER_1: u32 = 1;
    const REGION_NUMBER_2: u32 = 2;
    const REGION_NUMBER_7: u32 = 7;

    fn kv_catalog_manager() -> Arc<KvBackendCatalogManager> {
        let backend = Arc::new(MemoryKvBackend::default());
        let layered_cache_builder = LayeredCacheRegistryBuilder::default()
            .add_cache_registry(CacheRegistryBuilder::default().build());
        let fundamental_cache_registry = build_fundamental_cache_registry(backend.clone());
        let layered_cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .unwrap()
            .build(),
        );

        KvBackendCatalogManagerBuilder::new(
            Arc::new(NoopInformationExtension),
            backend,
            layered_cache_registry,
        )
        .build()
    }

    fn region_route(table_id: TableId, region_number: u32) -> RegionRoute {
        RegionRoute {
            region: Region {
                id: RegionId::new(table_id, region_number),
                ..Default::default()
            },
            leader_peer: Some(Peer {
                id: 1,
                addr: "127.0.0.1:3001".to_string(),
            }),
            ..Default::default()
        }
    }

    fn table_info(schema: &str, table: &str, table_id: TableId) -> TableInfo {
        // The catalog defaults to the default catalog name.
        let mut table_info =
            common_meta::key::test_utils::new_test_table_info_with_name(table_id, table);
        table_info.schema_name = schema.to_string();
        table_info
    }

    /// Creates a catalog with two partitioned tables named `PARTITIONED_TABLE`
    /// in two different schemas:
    ///
    /// - `SCHEMA_1.PARTITIONED_TABLE` has two regions.
    /// - `SCHEMA_2.PARTITIONED_TABLE` has one region.
    async fn setup_catalog() -> Arc<KvBackendCatalogManager> {
        let manager = kv_catalog_manager();
        let metadata = manager.table_metadata_manager_ref().clone();

        for schema in [SCHEMA_1, SCHEMA_2] {
            metadata
                .schema_manager()
                .create(SchemaNameKey::new(DEFAULT_CATALOG_NAME, schema), None, true)
                .await
                .unwrap();
        }

        for (table_id, schema, region_numbers) in [
            (TABLE_ID_1, SCHEMA_1, vec![REGION_NUMBER_1, REGION_NUMBER_2]),
            (TABLE_ID_2, SCHEMA_2, vec![REGION_NUMBER_7]),
        ] {
            let routes = region_numbers
                .into_iter()
                .map(|region_number| region_route(table_id, region_number))
                .collect();
            metadata
                .create_table_metadata(
                    table_info(schema, PARTITIONED_TABLE, table_id),
                    TableRouteValue::physical(routes),
                    RegionWalOptions::default(),
                )
                .await
                .unwrap();
        }

        manager
    }

    /// Scans `information_schema.partitions` and returns the total row count
    /// and the pretty printed batches.
    async fn scan_partitions(
        manager: &Arc<dyn CatalogManager>,
        filters: Vec<Expr>,
    ) -> (usize, String) {
        let table = manager
            .table(
                DEFAULT_CATALOG_NAME,
                INFORMATION_SCHEMA_NAME,
                PARTITIONS,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        let stream = table
            .scan_to_stream(ScanRequest {
                filters,
                ..Default::default()
            })
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let rows = batches.iter().map(|batch| batch.num_rows()).sum();
        let output = batches
            .iter()
            .map(|batch| batch.pretty_print())
            .collect::<Vec<_>>()
            .join("\n");
        (rows, output)
    }

    fn eq_filter(column: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col(column)),
            Operator::Eq,
            Box::new(lit(value)),
        ))
    }

    fn not_eq_filter(column: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col(column)),
            Operator::NotEq,
            Box::new(lit(value)),
        ))
    }

    #[tokio::test]
    async fn test_scan_pinned_table_skips_catalog_enumeration() {
        let manager: Arc<dyn CatalogManager> = setup_catalog().await;

        // No predicates: every partitioned table is listed.
        let (rows, output) = scan_partitions(&manager, vec![]).await;
        assert_eq!(3, rows, "{output}");

        // Pins both the schema and the table name, so the scan resolves the
        // table directly instead of enumerating all catalog entries.
        let (rows, output) = scan_partitions(
            &manager,
            vec![
                eq_filter(TABLE_SCHEMA, SCHEMA_1),
                eq_filter(TABLE_NAME, PARTITIONED_TABLE),
            ],
        )
        .await;
        assert_eq!(2, rows, "{output}");
        assert!(output.contains(SCHEMA_1), "{output}");
        assert!(!output.contains(SCHEMA_2), "{output}");
        assert!(
            output.contains(&RegionId::new(TABLE_ID_1, REGION_NUMBER_1).as_u64().to_string()),
            "{output}"
        );
        assert!(
            output.contains(&RegionId::new(TABLE_ID_1, REGION_NUMBER_2).as_u64().to_string()),
            "{output}"
        );

        // The fallback enumeration path must return the identical result. The
        // inequality on `table_name` doesn't pin a single name, so no direct
        // lookup happens here.
        let (fallback_rows, fallback_output) = scan_partitions(
            &manager,
            vec![
                eq_filter(TABLE_SCHEMA, SCHEMA_1),
                not_eq_filter(TABLE_NAME, MISSING_TABLE),
            ],
        )
        .await;
        assert_eq!(rows, fallback_rows, "{fallback_output}");
        assert_eq!(output, fallback_output);

        // Only the table name is pinned: the schema can't be resolved and both
        // tables of that name are kept.
        let (rows, output) =
            scan_partitions(&manager, vec![eq_filter(TABLE_NAME, PARTITIONED_TABLE)]).await;
        assert_eq!(3, rows, "{output}");
        assert!(output.contains(SCHEMA_1), "{output}");
        assert!(output.contains(SCHEMA_2), "{output}");

        // An unknown table in a pinned schema yields no rows.
        let (rows, output) = scan_partitions(
            &manager,
            vec![
                eq_filter(TABLE_SCHEMA, SCHEMA_1),
                eq_filter(TABLE_NAME, MISSING_TABLE),
            ],
        )
        .await;
        assert_eq!(0, rows, "{output}");
    }

    #[tokio::test]
    async fn test_scan_equality_under_disjunction_falls_back_to_enumeration() {
        let manager: Arc<dyn CatalogManager> = setup_catalog().await;

        // `table_schema = SCHEMA_1 OR table_name = PARTITIONED_TABLE` matches
        // the tables in both schemas: equality nested under a disjunction must
        // not pin a single table.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter(TABLE_SCHEMA, SCHEMA_1)),
            Operator::Or,
            Box::new(eq_filter(TABLE_NAME, PARTITIONED_TABLE)),
        ));
        let (rows, output) = scan_partitions(&manager, vec![filter]).await;
        assert_eq!(3, rows, "{output}");
        assert!(output.contains(SCHEMA_1), "{output}");
        assert!(output.contains(SCHEMA_2), "{output}");
    }
}
