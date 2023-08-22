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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use client::Database;
use common_error::ext::BoxedError;
use common_meta::table_name::TableName;
use common_query::error::Result as QueryResult;
use common_query::logical_plan::Expr;
use common_query::physical_plan::{PhysicalPlan, PhysicalPlanRef};
use common_query::Output;
use common_recordbatch::adapter::AsyncRecordBatchStreamAdapter;
use common_recordbatch::error::{
    ExternalSnafu as RecordBatchExternalSnafu, Result as RecordBatchResult,
};
use common_recordbatch::{RecordBatchStreamAdaptor, SendableRecordBatchStream};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::Partitioning;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use futures_util::StreamExt;
use snafu::prelude::*;
use store_api::storage::ScanRequest;
use table::error::TableOperationSnafu;
use table::metadata::{FilterPushDownType, TableInfoRef, TableType};
use table::requests::{DeleteRequest, InsertRequest};
use table::Table;

use crate::catalog::FrontendCatalogManager;
use crate::instance::distributed::deleter::DistDeleter;
use crate::instance::distributed::inserter::DistInserter;
use crate::table::scan::{DatanodeInstance, TableScanPlan};

pub mod delete;
pub mod insert;
pub(crate) mod scan;

#[derive(Clone)]
pub struct DistTable {
    table_name: TableName,
    table_info: TableInfoRef,
    catalog_manager: Arc<FrontendCatalogManager>,
}

#[async_trait]
impl Table for DistTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_info.meta.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        self.table_info.clone()
    }

    fn table_type(&self) -> TableType {
        self.table_info.table_type
    }

    async fn insert(&self, request: InsertRequest) -> table::Result<usize> {
        let inserter = DistInserter::new(
            request.catalog_name.clone(),
            request.schema_name.clone(),
            self.catalog_manager.clone(),
        );
        let affected_rows = inserter
            .insert(vec![request])
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;
        Ok(affected_rows as usize)
    }

    // TODO(ruihang): DistTable should not call this method directly
    async fn scan_to_stream(
        &self,
        request: ScanRequest,
    ) -> table::Result<SendableRecordBatchStream> {
        let partition_manager = self.catalog_manager.partition_manager();
        let datanode_clients = self.catalog_manager.datanode_clients();

        let table_id = self.table_info.table_id();
        let partition_rule = partition_manager
            .find_table_partition_rule(table_id)
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;

        let regions = partition_manager
            .find_regions_by_filters(partition_rule, &request.filters)
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;
        let datanodes = partition_manager
            .find_region_datanodes(table_id, regions)
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;

        let table_name = &self.table_name;
        let mut partition_execs = Vec::with_capacity(datanodes.len());
        for (datanode, _regions) in datanodes.iter() {
            let client = datanode_clients.get_client(datanode).await;
            let db = Database::new(&table_name.catalog_name, &table_name.schema_name, client);
            let datanode_instance = DatanodeInstance::new(Arc::new(self.clone()) as _, db);

            partition_execs.push(Arc::new(PartitionExec {
                table_name: table_name.clone(),
                datanode_instance,
                projection: request.projection.clone(),
                filters: request.filters.clone(),
                limit: request.limit,
            }));
        }

        let stream = Box::pin(async_stream::stream!({
            for partition_exec in partition_execs {
                let mut stream = partition_exec.scan_to_stream().await?;
                while let Some(record_batch) = stream.next().await {
                    yield record_batch;
                }
            }
        }));

        let schema = project_schema(self.schema(), request.projection.as_ref());
        let stream = RecordBatchStreamAdaptor {
            schema,
            stream,
            output_ordering: None,
        };

        Ok(Box::pin(stream))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> table::Result<Vec<FilterPushDownType>> {
        Ok(vec![FilterPushDownType::Inexact; filters.len()])
    }

    async fn delete(&self, request: DeleteRequest) -> table::Result<usize> {
        let deleter = DistDeleter::new(
            request.catalog_name.clone(),
            request.schema_name.clone(),
            self.catalog_manager.clone(),
        );
        let affected_rows = deleter
            .delete(vec![request])
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;
        Ok(affected_rows)
    }
}

impl DistTable {
    pub fn new(
        table_name: TableName,
        table_info: TableInfoRef,
        catalog_manager: Arc<FrontendCatalogManager>,
    ) -> Self {
        Self {
            table_name,
            table_info,
            catalog_manager,
        }
    }
}

fn project_schema(table_schema: SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    if let Some(projection) = projection {
        let columns = table_schema.column_schemas();
        let projected = projection
            .iter()
            .map(|x| columns[*x].clone())
            .collect::<Vec<ColumnSchema>>();
        Arc::new(Schema::new(projected))
    } else {
        table_schema
    }
}

#[derive(Debug)]
struct DistTableScan {
    schema: SchemaRef,
    partition_execs: Vec<Arc<PartitionExec>>,
}

impl PhysicalPlan for DistTableScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partition_execs.len())
    }

    fn children(&self) -> Vec<PhysicalPlanRef> {
        vec![]
    }

    fn with_new_children(&self, _children: Vec<PhysicalPlanRef>) -> QueryResult<PhysicalPlanRef> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> QueryResult<SendableRecordBatchStream> {
        let exec = self.partition_execs[partition].clone();
        let stream = Box::pin(async move { exec.scan_to_stream().await });
        let stream = AsyncRecordBatchStreamAdapter::new(self.schema(), stream);
        Ok(Box::pin(stream))
    }
}

#[derive(Debug)]
struct PartitionExec {
    table_name: TableName,
    datanode_instance: DatanodeInstance,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl PartitionExec {
    async fn scan_to_stream(&self) -> RecordBatchResult<SendableRecordBatchStream> {
        let plan: TableScanPlan = TableScanPlan {
            table_name: self.table_name.clone(),
            projection: self.projection.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
        };

        let output = self
            .datanode_instance
            .grpc_table_scan(plan)
            .await
            .map_err(BoxedError::new)
            .context(RecordBatchExternalSnafu)?;

        let Output::Stream(stream) = output else {
            unreachable!()
        };

        Ok(stream)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute, Table, TableRoute};
    use datafusion_expr::expr_fn::{and, binary_expr, col, or};
    use datafusion_expr::{lit, Operator};
    use meta_client::client::MetaClient;
    use meter_core::collect::Collect;
    use meter_core::data::{ReadRecord, WriteRecord};
    use meter_core::global::global_registry;
    use meter_core::write_calc::WriteCalculator;
    use partition::columns::RangeColumnsPartitionRule;
    use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
    use partition::partition::{PartitionBound, PartitionDef};
    use partition::range::RangePartitionRule;
    use partition::route::TableRoutes;
    use partition::PartitionRuleRef;
    use store_api::storage::RegionNumber;
    use table::meter_insert_request;

    use super::*;

    /// Create a partition rule manager with two tables, one is partitioned by single column, and
    /// the other one is two. The tables are under default catalog and schema.
    ///
    /// Table named "one_column_partitioning_table" is partitioned by column "a" like this:
    /// PARTITION BY RANGE (a) (
    ///   PARTITION r1 VALUES LESS THAN (10),
    ///   PARTITION r2 VALUES LESS THAN (50),
    ///   PARTITION r3 VALUES LESS THAN (MAXVALUE),
    /// )
    ///
    /// Table named "two_column_partitioning_table" is partitioned by columns "a" and "b" like this:
    /// PARTITION BY RANGE (a, b) (
    ///   PARTITION r1 VALUES LESS THAN (10, 'hz'),
    ///   PARTITION r2 VALUES LESS THAN (50, 'sh'),
    ///   PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
    /// )
    pub(crate) async fn create_partition_rule_manager() -> PartitionRuleManagerRef {
        let table_routes = Arc::new(TableRoutes::new(Arc::new(MetaClient::default())));
        let partition_manager = Arc::new(PartitionRuleManager::new(table_routes.clone()));

        let table_name = TableName::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            "one_column_partitioning_table",
        );
        let table_route = TableRoute::new(
            Table {
                id: 1,
                table_name: table_name.clone(),
                table_schema: vec![],
            },
            vec![
                RegionRoute {
                    region: Region {
                        id: 3.into(),
                        name: "r1".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string()],
                                vec![PartitionBound::Value(10_i32.into())],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: Some(Peer::new(3, "")),
                    follower_peers: vec![],
                },
                RegionRoute {
                    region: Region {
                        id: 2.into(),
                        name: "r2".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string()],
                                vec![PartitionBound::Value(50_i32.into())],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: Some(Peer::new(2, "")),
                    follower_peers: vec![],
                },
                RegionRoute {
                    region: Region {
                        id: 1.into(),
                        name: "r3".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string()],
                                vec![PartitionBound::MaxValue],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: Some(Peer::new(1, "")),
                    follower_peers: vec![],
                },
            ],
        );
        table_routes
            .insert_table_route(1, Arc::new(table_route))
            .await;

        let table_name = TableName::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            "two_column_partitioning_table",
        );
        let table_route = TableRoute::new(
            Table {
                id: 2,
                table_name: table_name.clone(),
                table_schema: vec![],
            },
            vec![
                RegionRoute {
                    region: Region {
                        id: 1.into(),
                        name: "r1".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string(), "b".to_string()],
                                vec![
                                    PartitionBound::Value(10_i32.into()),
                                    PartitionBound::Value("hz".into()),
                                ],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: None,
                    follower_peers: vec![],
                },
                RegionRoute {
                    region: Region {
                        id: 2.into(),
                        name: "r2".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string(), "b".to_string()],
                                vec![
                                    PartitionBound::Value(50_i32.into()),
                                    PartitionBound::Value("sh".into()),
                                ],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: None,
                    follower_peers: vec![],
                },
                RegionRoute {
                    region: Region {
                        id: 3.into(),
                        name: "r3".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string(), "b".to_string()],
                                vec![PartitionBound::MaxValue, PartitionBound::MaxValue],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: None,
                    follower_peers: vec![],
                },
            ],
        );
        table_routes
            .insert_table_route(2, Arc::new(table_route))
            .await;

        partition_manager
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_find_partition_rule() {
        let partition_manager = create_partition_rule_manager().await;

        // "one_column_partitioning_table" has id 1
        let partition_rule = partition_manager
            .find_table_partition_rule(1)
            .await
            .unwrap();
        let range_rule = partition_rule
            .as_any()
            .downcast_ref::<RangePartitionRule>()
            .unwrap();
        assert_eq!(range_rule.column_name(), "a");
        assert_eq!(range_rule.all_regions(), &vec![3, 2, 1]);
        assert_eq!(range_rule.bounds(), &vec![10_i32.into(), 50_i32.into()]);

        // "two_column_partitioning_table" has table 2
        let partition_rule = partition_manager
            .find_table_partition_rule(2)
            .await
            .unwrap();
        let range_columns_rule = partition_rule
            .as_any()
            .downcast_ref::<RangeColumnsPartitionRule>()
            .unwrap();
        assert_eq!(range_columns_rule.column_list(), &vec!["a", "b"]);
        assert_eq!(
            range_columns_rule.value_lists(),
            &vec![
                vec![
                    PartitionBound::Value(10_i32.into()),
                    PartitionBound::Value("hz".into()),
                ],
                vec![
                    PartitionBound::Value(50_i32.into()),
                    PartitionBound::Value("sh".into()),
                ],
                vec![PartitionBound::MaxValue, PartitionBound::MaxValue]
            ]
        );
        assert_eq!(range_columns_rule.regions(), &vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_find_regions() {
        let partition_manager = Arc::new(PartitionRuleManager::new(Arc::new(TableRoutes::new(
            Arc::new(MetaClient::default()),
        ))));

        // PARTITION BY RANGE (a) (
        //   PARTITION r1 VALUES LESS THAN (10),
        //   PARTITION r2 VALUES LESS THAN (20),
        //   PARTITION r3 VALUES LESS THAN (50),
        //   PARTITION r4 VALUES LESS THAN (MAXVALUE),
        // )
        let partition_rule: PartitionRuleRef = Arc::new(RangePartitionRule::new(
            "a",
            vec![10_i32.into(), 20_i32.into(), 50_i32.into()],
            vec![0_u32, 1, 2, 3],
        )) as _;

        let partition_rule_clone = partition_rule.clone();
        let test = |filters: Vec<Expr>, expect_regions: Vec<RegionNumber>| {
            let mut regions = partition_manager
                .find_regions_by_filters(partition_rule_clone.clone(), filters.as_slice())
                .unwrap();
            regions.sort();
            assert_eq!(regions, expect_regions);
        };

        // test simple filter
        test(
            vec![binary_expr(col("a"), Operator::Lt, lit(10)).into()], // a < 10
            vec![0],
        );
        test(
            vec![binary_expr(col("a"), Operator::LtEq, lit(10)).into()], // a <= 10
            vec![0, 1],
        );
        test(
            vec![binary_expr(lit(20), Operator::Gt, col("a")).into()], // 20 > a
            vec![0, 1],
        );
        test(
            vec![binary_expr(lit(20), Operator::GtEq, col("a")).into()], // 20 >= a
            vec![0, 1, 2],
        );
        test(
            vec![binary_expr(lit(45), Operator::Eq, col("a")).into()], // 45 == a
            vec![2],
        );
        test(
            vec![binary_expr(col("a"), Operator::NotEq, lit(45)).into()], // a != 45
            vec![0, 1, 2, 3],
        );
        test(
            vec![binary_expr(col("a"), Operator::Gt, lit(50)).into()], // a > 50
            vec![3],
        );

        // test multiple filters
        test(
            vec![
                binary_expr(col("a"), Operator::Gt, lit(10)).into(),
                binary_expr(col("a"), Operator::Gt, lit(50)).into(),
            ], // [a > 10, a > 50]
            vec![3],
        );

        // test finding all regions when provided with not supported filters or not partition column
        test(
            vec![binary_expr(col("row_id"), Operator::LtEq, lit(123)).into()], // row_id <= 123
            vec![0, 1, 2, 3],
        );
        test(
            vec![binary_expr(col("c"), Operator::Gt, lit(123)).into()], // c > 789
            vec![0, 1, 2, 3],
        );

        // test complex "AND" or "OR" filters
        test(
            vec![and(
                binary_expr(col("row_id"), Operator::Lt, lit(1)),
                or(
                    binary_expr(col("row_id"), Operator::Lt, lit(1)),
                    binary_expr(col("a"), Operator::Lt, lit(1)),
                ),
            )
            .into()], // row_id < 1 OR (row_id < 1 AND a > 1)
            vec![0, 1, 2, 3],
        );
        test(
            vec![or(
                binary_expr(col("a"), Operator::Lt, lit(20)),
                binary_expr(col("a"), Operator::GtEq, lit(20)),
            )
            .into()], // a < 20 OR a >= 20
            vec![0, 1, 2, 3],
        );
        test(
            vec![and(
                binary_expr(col("a"), Operator::Lt, lit(20)),
                binary_expr(col("a"), Operator::Lt, lit(50)),
            )
            .into()], // a < 20 AND a < 50
            vec![0, 1],
        );

        // test failed to find regions by contradictory filters
        let regions = partition_manager.find_regions_by_filters(
            partition_rule,
            vec![and(
                binary_expr(col("a"), Operator::Lt, lit(20)),
                binary_expr(col("a"), Operator::GtEq, lit(20)),
            )
            .into()]
            .as_slice(),
        ); // a < 20 AND a >= 20
        assert!(matches!(
            regions.unwrap_err(),
            partition::error::Error::FindRegions { .. }
        ));
    }

    #[derive(Default)]
    struct MockCollector {
        pub write_sum: AtomicU32,
    }

    impl Collect for MockCollector {
        fn on_write(&self, record: WriteRecord) {
            let _ = self
                .write_sum
                .fetch_add(record.byte_count, Ordering::Relaxed);
        }

        fn on_read(&self, _record: ReadRecord) {
            todo!()
        }
    }

    struct MockCalculator;

    impl WriteCalculator<InsertRequest> for MockCalculator {
        fn calc_byte(&self, _value: &InsertRequest) -> u32 {
            1024 * 10
        }
    }

    #[test]
    #[ignore]
    fn test_meter_insert_request() {
        let collector = Arc::new(MockCollector::default());
        global_registry().set_collector(collector.clone());
        global_registry().register_calculator(Arc::new(MockCalculator));

        let req = InsertRequest {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: "numbers".to_string(),
            columns_values: Default::default(),
            region_number: 0,
        };
        meter_insert_request!(req);

        let re = collector.write_sum.load(Ordering::Relaxed);
        assert_eq!(re, 1024 * 10);
    }
}
