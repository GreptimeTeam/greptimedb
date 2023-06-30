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
use std::iter;
use std::pin::Pin;
use std::sync::Arc;

use api::v1::AlterExpr;
use async_trait::async_trait;
use catalog::helper::{TableGlobalKey, TableGlobalValue};
use client::Database;
use common_error::prelude::BoxedError;
use common_meta::key::TableRouteKey;
use common_meta::table_name::TableName;
use common_query::error::Result as QueryResult;
use common_query::logical_plan::Expr;
use common_query::physical_plan::{PhysicalPlan, PhysicalPlanRef};
use common_query::Output;
use common_recordbatch::adapter::AsyncRecordBatchStreamAdapter;
use common_recordbatch::error::{
    InitRecordbatchStreamSnafu, PollStreamSnafu, Result as RecordBatchResult,
};
use common_recordbatch::{
    RecordBatch, RecordBatchStreamAdaptor, RecordBatches, SendableRecordBatchStream,
};
use common_telemetry::debug;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    Partitioning, SendableRecordBatchStream as DfSendableRecordBatchStream,
};
use datafusion_common::DataFusionError;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use futures_util::{Stream, StreamExt};
use partition::manager::TableRouteCacheInvalidator;
use partition::splitter::WriteSplitter;
use snafu::prelude::*;
use store_api::storage::{RegionNumber, ScanRequest};
use table::error::TableOperationSnafu;
use table::metadata::{FilterPushDownType, TableInfo, TableInfoRef};
use table::requests::{AlterKind, AlterTableRequest, DeleteRequest, InsertRequest};
use table::table::AlterContext;
use table::Table;
use tokio::sync::RwLock;

use crate::catalog::FrontendCatalogManager;
use crate::error::{self, FindDatanodeSnafu, FindTableRouteSnafu, Result};
use crate::instance::distributed::inserter::DistInserter;
use crate::table::delete::to_grpc_delete_request;
use crate::table::scan::{DatanodeInstance, TableScanPlan};

mod delete;
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

        let partition_rule = partition_manager
            .find_table_partition_rule(&self.table_name)
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;

        let regions = partition_manager
            .find_regions_by_filters(partition_rule, &request.filters)
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;
        let datanodes = partition_manager
            .find_region_datanodes(&self.table_name, regions)
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
                batches: Arc::new(RwLock::new(None)),
            }));
        }

        let schema = project_schema(self.schema(), request.projection.as_ref());
        let schema_to_move = schema.clone();
        let stream: Pin<Box<dyn Stream<Item = RecordBatchResult<RecordBatch>> + Send>> = Box::pin(
            async_stream::try_stream! {
                for partition_exec in partition_execs {
                    partition_exec
                        .maybe_init()
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))
                        .context(InitRecordbatchStreamSnafu)?;
                    let mut stream = partition_exec.as_stream().await.context(InitRecordbatchStreamSnafu)?;

                    while let Some(batch) = stream.next().await{
                        yield RecordBatch::try_from_df_record_batch(schema_to_move.clone(),batch.context(PollStreamSnafu)?)?
                    }
                }
            },
        );
        let record_batch_stream = RecordBatchStreamAdaptor {
            schema,
            stream,
            output_ordering: None,
        };

        Ok(Box::pin(record_batch_stream))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> table::Result<Vec<FilterPushDownType>> {
        Ok(vec![FilterPushDownType::Inexact; filters.len()])
    }

    async fn alter(&self, context: AlterContext, request: &AlterTableRequest) -> table::Result<()> {
        self.handle_alter(context, request)
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)
    }

    async fn delete(&self, request: DeleteRequest) -> table::Result<usize> {
        let partition_manager = self.catalog_manager.partition_manager();

        let partition_rule = partition_manager
            .find_table_partition_rule(&self.table_name)
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;

        let schema = self.schema();
        let time_index = &schema
            .timestamp_column()
            .with_context(|| table::error::MissingTimeIndexColumnSnafu {
                table_name: self.table_name.to_string(),
            })?
            .name;

        let table_info = self.table_info();
        let key_column_names = table_info
            .meta
            .row_key_column_names()
            .chain(iter::once(time_index))
            .collect::<Vec<_>>();

        let requests = WriteSplitter::with_partition_rule(partition_rule)
            .split_delete(request, key_column_names)
            .map_err(BoxedError::new)
            .and_then(|requests| {
                requests
                    .into_iter()
                    .map(|(region_number, request)| {
                        to_grpc_delete_request(
                            &table_info.meta,
                            &self.table_name,
                            region_number,
                            request,
                        )
                    })
                    .collect::<Result<Vec<_>>>()
                    .map_err(BoxedError::new)
            })
            .context(TableOperationSnafu)?;

        let output = self
            .dist_delete(requests)
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;
        let Output::AffectedRows(rows) = output else { unreachable!() };
        Ok(rows)
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

    pub async fn table_global_value(
        &self,
        key: &TableGlobalKey,
    ) -> Result<Option<TableGlobalValue>> {
        let raw = self
            .catalog_manager
            .backend()
            .get(key.to_string().as_bytes())
            .await
            .context(error::CatalogSnafu)?;
        Ok(if let Some(raw) = raw {
            Some(TableGlobalValue::from_bytes(raw.1).context(error::CatalogEntrySerdeSnafu)?)
        } else {
            None
        })
    }

    async fn set_table_global_value(
        &self,
        key: TableGlobalKey,
        value: TableGlobalValue,
    ) -> Result<()> {
        let value = value.as_bytes().context(error::CatalogEntrySerdeSnafu)?;
        self.catalog_manager
            .backend()
            .set(key.to_string().as_bytes(), &value)
            .await
            .context(error::CatalogSnafu)
    }

    async fn delete_table_global_value(&self, key: TableGlobalKey) -> Result<()> {
        self.catalog_manager
            .backend()
            .delete(key.to_string().as_bytes())
            .await
            .context(error::CatalogSnafu)
    }

    async fn move_table_route_value(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_id: u32,
        old_table_name: &str,
        new_table_name: &str,
    ) -> Result<()> {
        let old_key = TableRouteKey {
            table_id: table_id.into(),
            catalog_name,
            schema_name,
            table_name: old_table_name,
        }
        .key();

        let new_key = TableRouteKey {
            table_id: table_id.into(),
            catalog_name,
            schema_name,
            table_name: new_table_name,
        }
        .key();

        self.catalog_manager
            .backend()
            .move_value(old_key.as_bytes(), new_key.as_bytes())
            .await
            .context(error::CatalogSnafu)?;

        self.catalog_manager
            .partition_manager()
            .invalidate_table_route(&TableName {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name: old_table_name.to_string(),
            })
            .await;

        Ok(())
    }

    async fn handle_alter(&self, context: AlterContext, request: &AlterTableRequest) -> Result<()> {
        let AlterTableRequest {
            catalog_name,
            schema_name,
            table_name,
            alter_kind,
            table_id: _table_id,
        } = request;

        let alter_expr = context
            .get::<AlterExpr>()
            .context(error::ContextValueNotFoundSnafu { key: "AlterExpr" })?;

        self.alter_by_expr(alter_expr).await?;

        let table_info = self.table_info();
        let new_meta = table_info
            .meta
            .builder_with_alter_kind(table_name, &request.alter_kind)
            .context(error::TableSnafu)?
            .build()
            .context(error::BuildTableMetaSnafu {
                table_name: table_name.clone(),
            })?;

        let mut new_info = TableInfo::clone(&*table_info);
        new_info.ident.version = table_info.ident.version + 1;
        new_info.meta = new_meta;

        let key = TableGlobalKey {
            catalog_name: catalog_name.clone(),
            schema_name: schema_name.clone(),
            table_name: table_name.clone(),
        };
        let mut value = self
            .table_global_value(&key)
            .await?
            .context(error::TableNotFoundSnafu { table_name })?;

        value.table_info = new_info.into();

        if let AlterKind::RenameTable { new_table_name } = alter_kind {
            let new_key = TableGlobalKey {
                catalog_name: catalog_name.clone(),
                schema_name: schema_name.clone(),
                table_name: new_table_name.clone(),
            };
            self.set_table_global_value(new_key, value).await?;
            self.delete_table_global_value(key).await?;
            self.move_table_route_value(
                catalog_name,
                schema_name,
                table_info.ident.table_id,
                table_name,
                new_table_name,
            )
            .await?;
            Ok(())
        } else {
            self.set_table_global_value(key, value).await
        }
    }

    /// Define a `alter_by_expr` instead of impl [`Table::alter`] to avoid redundant conversion between
    /// [`table::requests::AlterTableRequest`] and [`AlterExpr`].
    async fn alter_by_expr(&self, expr: &AlterExpr) -> Result<()> {
        let table_routes = self
            .catalog_manager
            .partition_manager()
            .find_table_route(&self.table_name)
            .await
            .with_context(|_| error::FindTableRouteSnafu {
                table_name: self.table_name.to_string(),
            })?;
        let leaders = table_routes.find_leaders();
        ensure!(
            !leaders.is_empty(),
            error::LeaderNotFoundSnafu {
                table: format!(
                    "{:?}.{:?}.{}",
                    expr.catalog_name, expr.schema_name, expr.table_name
                )
            }
        );

        let datanode_clients = self.catalog_manager.datanode_clients();
        for datanode in leaders {
            let client = datanode_clients.get_client(&datanode).await;
            let db = Database::new(&expr.catalog_name, &expr.schema_name, client);
            debug!("Sending {:?} to {:?}", expr, db);
            let result = db
                .alter(expr.clone())
                .await
                .context(error::RequestDatanodeSnafu)?;
            debug!("Alter table result: {:?}", result);
            // TODO(hl): We should further check and track alter result in some global DDL task tracker
        }
        Ok(())
    }

    async fn find_datanode_instances(
        &self,
        regions: &[RegionNumber],
    ) -> Result<Vec<DatanodeInstance>> {
        let table_name = &self.table_name;
        let route = self
            .catalog_manager
            .partition_manager()
            .find_table_route(table_name)
            .await
            .with_context(|_| FindTableRouteSnafu {
                table_name: table_name.to_string(),
            })?;

        let datanodes = regions
            .iter()
            .map(|&n| {
                route
                    .find_region_leader(n)
                    .context(FindDatanodeSnafu { region: n })
            })
            .collect::<Result<Vec<_>>>()?;

        let datanode_clients = self.catalog_manager.datanode_clients();
        let mut instances = Vec::with_capacity(datanodes.len());
        for datanode in datanodes {
            let client = datanode_clients.get_client(datanode).await;
            let db = Database::new(&table_name.catalog_name, &table_name.schema_name, client);
            instances.push(DatanodeInstance::new(Arc::new(self.clone()) as _, db));
        }
        Ok(instances)
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
        let stream = Box::pin(async move {
            exec.maybe_init()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            exec.as_stream().await
        });
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
    batches: Arc<RwLock<Option<RecordBatches>>>,
}

impl PartitionExec {
    async fn maybe_init(&self) -> Result<()> {
        if self.batches.read().await.is_some() {
            return Ok(());
        }

        let mut batches = self.batches.write().await;
        if batches.is_some() {
            return Ok(());
        }

        let plan: TableScanPlan = TableScanPlan {
            table_name: self.table_name.clone(),
            projection: self.projection.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
        };
        let result = self.datanode_instance.grpc_table_scan(plan).await?;
        let _ = batches.insert(result);
        Ok(())
    }

    /// Notice: the record batch will be consumed.
    async fn as_stream(&self) -> std::result::Result<DfSendableRecordBatchStream, DataFusionError> {
        let mut batches = self.batches.write().await;
        Ok(batches
            .take()
            .expect("should have been initialized in \"maybe_init\"")
            .into_df_stream())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::collections::HashMap;
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
                        id: 3,
                        name: "r1".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string()],
                                vec![PartitionBound::Value(10_i32.into())],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: HashMap::new(),
                    },
                    leader_peer: Some(Peer::new(3, "")),
                    follower_peers: vec![],
                },
                RegionRoute {
                    region: Region {
                        id: 2,
                        name: "r2".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string()],
                                vec![PartitionBound::Value(50_i32.into())],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: HashMap::new(),
                    },
                    leader_peer: Some(Peer::new(2, "")),
                    follower_peers: vec![],
                },
                RegionRoute {
                    region: Region {
                        id: 1,
                        name: "r3".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string()],
                                vec![PartitionBound::MaxValue],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: HashMap::new(),
                    },
                    leader_peer: Some(Peer::new(1, "")),
                    follower_peers: vec![],
                },
            ],
        );
        table_routes
            .insert_table_route(table_name.clone(), Arc::new(table_route))
            .await;

        let table_name = TableName::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            "two_column_partitioning_table",
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
                        id: 1,
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
                        attrs: HashMap::new(),
                    },
                    leader_peer: None,
                    follower_peers: vec![],
                },
                RegionRoute {
                    region: Region {
                        id: 2,
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
                        attrs: HashMap::new(),
                    },
                    leader_peer: None,
                    follower_peers: vec![],
                },
                RegionRoute {
                    region: Region {
                        id: 3,
                        name: "r3".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string(), "b".to_string()],
                                vec![PartitionBound::MaxValue, PartitionBound::MaxValue],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: HashMap::new(),
                    },
                    leader_peer: None,
                    follower_peers: vec![],
                },
            ],
        );
        table_routes
            .insert_table_route(table_name.clone(), Arc::new(table_route))
            .await;

        partition_manager
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_find_partition_rule() {
        let partition_manager = create_partition_rule_manager().await;

        let partition_rule = partition_manager
            .find_table_partition_rule(&TableName::new(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                "one_column_partitioning_table",
            ))
            .await
            .unwrap();
        let range_rule = partition_rule
            .as_any()
            .downcast_ref::<RangePartitionRule>()
            .unwrap();
        assert_eq!(range_rule.column_name(), "a");
        assert_eq!(range_rule.all_regions(), &vec![3, 2, 1]);
        assert_eq!(range_rule.bounds(), &vec![10_i32.into(), 50_i32.into()]);

        let partition_rule = partition_manager
            .find_table_partition_rule(&TableName::new(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                "two_column_partitioning_table",
            ))
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
