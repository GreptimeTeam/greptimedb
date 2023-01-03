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

pub(crate) mod route;

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::v1::AlterExpr;
use async_trait::async_trait;
use client::{Database, RpcOutput};
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_error::prelude::BoxedError;
use common_query::error::Result as QueryResult;
use common_query::logical_plan::Expr;
use common_query::physical_plan::{PhysicalPlan, PhysicalPlanRef};
use common_recordbatch::adapter::AsyncRecordBatchStreamAdapter;
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use common_telemetry::debug;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    Partitioning, SendableRecordBatchStream as DfSendableRecordBatchStream,
};
use datafusion_common::DataFusionError;
use datafusion_expr::expr::Expr as DfExpr;
use datafusion_expr::BinaryExpr;
use datatypes::prelude::Value;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use meta_client::rpc::{Peer, TableName};
use snafu::prelude::*;
use store_api::storage::RegionNumber;
use table::error::TableOperationSnafu;
use table::metadata::{FilterPushDownType, TableInfoRef};
use table::requests::InsertRequest;
use table::Table;
use tokio::sync::RwLock;

use crate::datanode::DatanodeClients;
use crate::error::{self, Error, LeaderNotFoundSnafu, RequestDatanodeSnafu, Result};
use crate::partitioning::columns::RangeColumnsPartitionRule;
use crate::partitioning::range::RangePartitionRule;
use crate::partitioning::{
    Operator, PartitionBound, PartitionDef, PartitionExpr, PartitionRuleRef,
};
use crate::spliter::WriteSpliter;
use crate::table::route::TableRoutes;
use crate::table::scan::{DatanodeInstance, TableScanPlan};

pub mod insert;
pub(crate) mod scan;

#[derive(Clone)]
pub struct DistTable {
    table_name: TableName,
    table_info: TableInfoRef,
    table_routes: Arc<TableRoutes>,
    datanode_clients: Arc<DatanodeClients>,
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
        let partition_rule = self
            .find_partition_rule()
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;

        let spliter = WriteSpliter::with_partition_rule(partition_rule);
        let inserts = spliter
            .split(request)
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;

        let output = self
            .dist_insert(inserts)
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;
        let RpcOutput::AffectedRows(rows) = output else { unreachable!() };
        Ok(rows)
    }

    async fn scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> table::Result<PhysicalPlanRef> {
        let partition_rule = self
            .find_partition_rule()
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;

        let regions = self
            .find_regions(partition_rule, filters)
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;
        let datanodes = self
            .find_datanodes(regions)
            .await
            .map_err(BoxedError::new)
            .context(TableOperationSnafu)?;

        let mut partition_execs = Vec::with_capacity(datanodes.len());
        for (datanode, _regions) in datanodes.iter() {
            let client = self.datanode_clients.get_client(datanode).await;
            let db = Database::new(&self.table_name.schema_name, client);
            let datanode_instance = DatanodeInstance::new(Arc::new(self.clone()) as _, db);

            // TODO(LFC): Pass in "regions" when Datanode supports multi regions for a table.
            partition_execs.push(Arc::new(PartitionExec {
                table_name: self.table_name.clone(),
                datanode_instance,
                projection: projection.cloned(),
                filters: filters.to_vec(),
                limit,
                batches: Arc::new(RwLock::new(None)),
            }));
        }

        let dist_scan = DistTableScan {
            schema: project_schema(self.schema(), projection),
            partition_execs,
        };
        Ok(Arc::new(dist_scan))
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> table::Result<FilterPushDownType> {
        Ok(FilterPushDownType::Inexact)
    }
}

impl DistTable {
    pub(crate) fn new(
        table_name: TableName,
        table_info: TableInfoRef,
        table_routes: Arc<TableRoutes>,
        datanode_clients: Arc<DatanodeClients>,
    ) -> Self {
        Self {
            table_name,
            table_info,
            table_routes,
            datanode_clients,
        }
    }

    // TODO(LFC): Finding regions now seems less efficient, should be further looked into.
    fn find_regions(
        &self,
        partition_rule: PartitionRuleRef<Error>,
        filters: &[Expr],
    ) -> Result<Vec<RegionNumber>> {
        let regions = if let Some((first, rest)) = filters.split_first() {
            let mut target = Self::find_regions0(partition_rule.clone(), first)?;
            for filter in rest {
                let regions = Self::find_regions0(partition_rule.clone(), filter)?;

                // When all filters are provided as a collection, it often implicitly states that
                // "all filters must be satisfied". So we join all the results here.
                target.retain(|x| regions.contains(x));

                // Failed fast, empty collection join any is empty.
                if target.is_empty() {
                    break;
                }
            }
            target.into_iter().collect::<Vec<_>>()
        } else {
            partition_rule.find_regions(&[])?
        };
        ensure!(
            !regions.is_empty(),
            error::FindRegionsSnafu {
                filters: filters.to_vec()
            }
        );
        Ok(regions)
    }

    // TODO(LFC): Support other types of filter expr:
    //   - BETWEEN and IN (maybe more)
    //   - expr with arithmetic like "a + 1 < 10" (should have been optimized in logic plan?)
    //   - not comparison or neither "AND" nor "OR" operations, for example, "a LIKE x"
    fn find_regions0(
        partition_rule: PartitionRuleRef<Error>,
        filter: &Expr,
    ) -> Result<HashSet<RegionNumber>> {
        let expr = filter.df_expr();
        match expr {
            DfExpr::BinaryExpr(BinaryExpr { left, op, right }) if is_compare_op(op) => {
                let column_op_value = match (left.as_ref(), right.as_ref()) {
                    (DfExpr::Column(c), DfExpr::Literal(v)) => Some((&c.name, *op, v)),
                    (DfExpr::Literal(v), DfExpr::Column(c)) => {
                        Some((&c.name, reverse_operator(op), v))
                    }
                    _ => None,
                };
                if let Some((column, op, sv)) = column_op_value {
                    let value = sv
                        .clone()
                        .try_into()
                        .with_context(|_| error::ConvertScalarValueSnafu { value: sv.clone() })?;
                    return Ok(partition_rule
                        .find_regions(&[PartitionExpr::new(column, op, value)])?
                        .into_iter()
                        .collect::<HashSet<RegionNumber>>());
                }
            }
            DfExpr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(op, Operator::And | Operator::Or) =>
            {
                let left_regions =
                    Self::find_regions0(partition_rule.clone(), &(*left.clone()).into())?;
                let right_regions =
                    Self::find_regions0(partition_rule.clone(), &(*right.clone()).into())?;
                let regions = match op {
                    Operator::And => left_regions
                        .intersection(&right_regions)
                        .cloned()
                        .collect::<HashSet<RegionNumber>>(),
                    Operator::Or => left_regions
                        .union(&right_regions)
                        .cloned()
                        .collect::<HashSet<RegionNumber>>(),
                    _ => unreachable!(),
                };
                return Ok(regions);
            }
            _ => (),
        }

        // Returns all regions for not supported partition expr as a safety hatch.
        Ok(partition_rule
            .find_regions(&[])?
            .into_iter()
            .collect::<HashSet<RegionNumber>>())
    }

    async fn find_datanodes(
        &self,
        regions: Vec<RegionNumber>,
    ) -> Result<HashMap<Peer, Vec<RegionNumber>>> {
        let route = self.table_routes.get_route(&self.table_name).await?;

        let mut datanodes = HashMap::new();
        for region in regions.iter() {
            let datanode = route
                .region_routes
                .iter()
                .find_map(|x| {
                    if x.region.id == *region as u64 {
                        x.leader_peer.clone()
                    } else {
                        None
                    }
                })
                .context(error::FindDatanodeSnafu { region: *region })?;
            datanodes
                .entry(datanode)
                .or_insert_with(Vec::new)
                .push(*region);
        }
        Ok(datanodes)
    }

    async fn find_partition_rule(&self) -> Result<PartitionRuleRef<Error>> {
        let route = self.table_routes.get_route(&self.table_name).await?;
        ensure!(
            !route.region_routes.is_empty(),
            error::FindRegionRoutesSnafu {
                table_name: self.table_name.to_string()
            }
        );

        let mut partitions = Vec::with_capacity(route.region_routes.len());
        for r in route.region_routes.iter() {
            let partition =
                r.region
                    .partition
                    .clone()
                    .context(error::FindRegionPartitionSnafu {
                        region: r.region.id,
                        table_name: self.table_name.to_string(),
                    })?;
            let partition_def: PartitionDef = partition.try_into()?;
            partitions.push((r.region.id, partition_def));
        }
        partitions.sort_by(|a, b| a.1.partition_bounds().cmp(b.1.partition_bounds()));

        ensure!(
            partitions
                .windows(2)
                .all(|w| w[0].1.partition_columns() == w[1].1.partition_columns()),
            error::IllegalTableRoutesDataSnafu {
                table_name: self.table_name.to_string(),
                err_msg: "partition columns of all regions are not the same"
            }
        );
        let partition_columns = partitions[0].1.partition_columns();
        ensure!(
            !partition_columns.is_empty(),
            error::IllegalTableRoutesDataSnafu {
                table_name: self.table_name.to_string(),
                err_msg: "no partition columns found"
            }
        );

        let regions = partitions
            .iter()
            .map(|x| x.0 as u32)
            .collect::<Vec<RegionNumber>>();

        // TODO(LFC): Serializing and deserializing partition rule is ugly, must find a much more elegant way.
        let partition_rule: PartitionRuleRef<Error> = match partition_columns.len() {
            1 => {
                // Omit the last "MAXVALUE".
                let bounds = partitions
                    .iter()
                    .filter_map(|(_, p)| match &p.partition_bounds()[0] {
                        PartitionBound::Value(v) => Some(v.clone()),
                        PartitionBound::MaxValue => None,
                    })
                    .collect::<Vec<Value>>();
                Arc::new(RangePartitionRule::new(
                    partition_columns[0].clone(),
                    bounds,
                    regions,
                )) as _
            }
            _ => {
                let bounds = partitions
                    .iter()
                    .map(|x| x.1.partition_bounds().clone())
                    .collect::<Vec<Vec<PartitionBound>>>();
                Arc::new(RangeColumnsPartitionRule::new(
                    partition_columns.clone(),
                    bounds,
                    regions,
                )) as _
            }
        };
        Ok(partition_rule)
    }

    /// Define a `alter_by_expr` instead of impl [`Table::alter`] to avoid redundant conversion between
    /// [`table::requests::AlterTableRequest`] and [`AlterExpr`].
    pub(crate) async fn alter_by_expr(&self, expr: AlterExpr) -> Result<()> {
        let table_routes = self.table_routes.get_route(&self.table_name).await?;
        let leaders = table_routes.find_leaders();
        ensure!(
            !leaders.is_empty(),
            LeaderNotFoundSnafu {
                table: format!(
                    "{:?}.{:?}.{}",
                    expr.catalog_name, expr.schema_name, expr.table_name
                )
            }
        );
        for datanode in leaders {
            let db = Database::new(
                DEFAULT_CATALOG_NAME,
                self.datanode_clients.get_client(&datanode).await,
            );
            debug!("Sending {:?} to {:?}", expr, db);
            let result = db.alter(expr.clone()).await.context(RequestDatanodeSnafu)?;
            debug!("Alter table result: {:?}", result);
            // TODO(hl): We should further check and track alter result in some global DDL task tracker
        }
        Ok(())
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

fn is_compare_op(op: &Operator) -> bool {
    matches!(
        *op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

fn reverse_operator(op: &Operator) -> Operator {
    match *op {
        Operator::Lt => Operator::Gt,
        Operator::Gt => Operator::Lt,
        Operator::LtEq => Operator::GtEq,
        Operator::GtEq => Operator::LtEq,
        _ => *op,
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

        let plan = TableScanPlan {
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
mod test {
    use api::v1::column::SemanticType;
    use api::v1::{column, Column, ColumnDataType, InsertRequest};
    use common_query::physical_plan::DfPhysicalPlanAdapter;
    use common_recordbatch::adapter::RecordBatchStreamAdapter;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::{col as physical_col, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::sqlparser;
    use datafusion_expr::expr_fn::{and, binary_expr, col, or};
    use datafusion_expr::lit;
    use datanode::instance::Instance;
    use datatypes::arrow::compute::SortOptions;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use itertools::Itertools;
    use meta_client::client::MetaClient;
    use meta_client::rpc::router::RegionRoute;
    use meta_client::rpc::{Region, Table, TableRoute};
    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::TableRef;

    use super::*;
    use crate::expr_factory::{CreateExprFactory, DefaultCreateExprFactory};
    use crate::partitioning::range::RangePartitionRule;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_find_partition_rule() {
        let table_name = TableName::new("greptime", "public", "foo");

        let column_schemas = vec![
            ColumnSchema::new("ts", ConcreteDataType::uint64_datatype(), false),
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("b", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas.clone()));
        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![])
            .next_column_id(1)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(&table_name.table_name)
            .meta(meta)
            .build()
            .unwrap();

        let table_routes = Arc::new(TableRoutes::new(Arc::new(MetaClient::default())));
        let table = DistTable {
            table_name: table_name.clone(),
            table_info: Arc::new(table_info),
            table_routes: table_routes.clone(),
            datanode_clients: Arc::new(DatanodeClients::new()),
        };

        let table_route = TableRoute {
            table: Table {
                id: 1,
                table_name: table_name.clone(),
                table_schema: vec![],
            },
            region_routes: vec![
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
                    leader_peer: None,
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
                    leader_peer: None,
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
                    leader_peer: None,
                    follower_peers: vec![],
                },
            ],
        };
        table_routes
            .insert_table_route(table_name.clone(), Arc::new(table_route))
            .await;

        let partition_rule = table.find_partition_rule().await.unwrap();
        let range_rule = partition_rule
            .as_any()
            .downcast_ref::<RangePartitionRule>()
            .unwrap();
        assert_eq!(range_rule.column_name(), "a");
        assert_eq!(range_rule.all_regions(), &vec![3, 2, 1]);
        assert_eq!(range_rule.bounds(), &vec![10_i32.into(), 50_i32.into()]);

        let table_route = TableRoute {
            table: Table {
                id: 1,
                table_name: table_name.clone(),
                table_schema: vec![],
            },
            region_routes: vec![
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
        };
        table_routes
            .insert_table_route(table_name.clone(), Arc::new(table_route))
            .await;

        let partition_rule = table.find_partition_rule().await.unwrap();
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
    async fn test_dist_table_scan() {
        common_telemetry::init_default_ut_logging();
        let table = Arc::new(new_dist_table().await);
        // should scan all regions
        // select a, row_id from numbers
        let projection = Some(vec![1, 2]);
        let filters = vec![];
        let expected_output = vec![
            "+-----+--------+",
            "| a   | row_id |",
            "+-----+--------+",
            "| 0   | 1      |",
            "| 1   | 2      |",
            "| 2   | 3      |",
            "| 3   | 4      |",
            "| 4   | 5      |",
            "| 10  | 1      |",
            "| 11  | 2      |",
            "| 12  | 3      |",
            "| 13  | 4      |",
            "| 14  | 5      |",
            "| 30  | 1      |",
            "| 31  | 2      |",
            "| 32  | 3      |",
            "| 33  | 4      |",
            "| 34  | 5      |",
            "| 100 | 1      |",
            "| 101 | 2      |",
            "| 102 | 3      |",
            "| 103 | 4      |",
            "| 104 | 5      |",
            "+-----+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 4, expected_output).await;

        // should scan only region 1
        // select a, row_id from numbers where a < 10
        let projection = Some(vec![1, 2]);
        let filters = vec![binary_expr(col("a"), Operator::Lt, lit(10)).into()];
        let expected_output = vec![
            "+---+--------+",
            "| a | row_id |",
            "+---+--------+",
            "| 0 | 1      |",
            "| 1 | 2      |",
            "| 2 | 3      |",
            "| 3 | 4      |",
            "| 4 | 5      |",
            "+---+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 1, expected_output).await;

        // should scan region 1 and 2
        // select a, row_id from numbers where a < 15
        let projection = Some(vec![1, 2]);
        let filters = vec![binary_expr(col("a"), Operator::Lt, lit(15)).into()];
        let expected_output = vec![
            "+----+--------+",
            "| a  | row_id |",
            "+----+--------+",
            "| 0  | 1      |",
            "| 1  | 2      |",
            "| 2  | 3      |",
            "| 3  | 4      |",
            "| 4  | 5      |",
            "| 10 | 1      |",
            "| 11 | 2      |",
            "| 12 | 3      |",
            "| 13 | 4      |",
            "| 14 | 5      |",
            "+----+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 2, expected_output).await;

        // should scan region 2 and 3
        // select a, row_id from numbers where a < 40 and a >= 10
        let projection = Some(vec![1, 2]);
        let filters = vec![and(
            binary_expr(col("a"), Operator::Lt, lit(40)),
            binary_expr(col("a"), Operator::GtEq, lit(10)),
        )
        .into()];
        let expected_output = vec![
            "+----+--------+",
            "| a  | row_id |",
            "+----+--------+",
            "| 10 | 1      |",
            "| 11 | 2      |",
            "| 12 | 3      |",
            "| 13 | 4      |",
            "| 14 | 5      |",
            "| 30 | 1      |",
            "| 31 | 2      |",
            "| 32 | 3      |",
            "| 33 | 4      |",
            "| 34 | 5      |",
            "+----+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 2, expected_output).await;

        // should scan all regions
        // select a, row_id from numbers where a < 1000 and row_id == 1
        let projection = Some(vec![1, 2]);
        let filters = vec![and(
            binary_expr(col("a"), Operator::Lt, lit(1000)),
            binary_expr(col("row_id"), Operator::Eq, lit(1)),
        )
        .into()];
        let expected_output = vec![
            "+-----+--------+",
            "| a   | row_id |",
            "+-----+--------+",
            "| 0   | 1      |",
            "| 10  | 1      |",
            "| 30  | 1      |",
            "| 100 | 1      |",
            "+-----+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 4, expected_output).await;
    }

    async fn exec_table_scan(
        table: TableRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        expected_partitions: usize,
        expected_output: Vec<&str>,
    ) {
        let expected_output = expected_output.into_iter().join("\n");
        let table_scan = table
            .scan(projection.as_ref(), filters.as_slice(), None)
            .await
            .unwrap();
        assert_eq!(
            table_scan.output_partitioning().partition_count(),
            expected_partitions
        );

        let merge =
            CoalescePartitionsExec::new(Arc::new(DfPhysicalPlanAdapter(table_scan.clone())));

        let sort = SortExec::try_new(
            vec![PhysicalSortExpr {
                expr: physical_col("a", table_scan.schema().arrow_schema()).unwrap(),
                options: SortOptions::default(),
            }],
            Arc::new(merge),
            None,
        )
        .unwrap();
        assert_eq!(sort.output_partitioning().partition_count(), 1);

        let session_ctx = SessionContext::new();
        let stream = sort.execute(0, session_ctx.task_ctx()).unwrap();
        let stream = Box::pin(RecordBatchStreamAdapter::try_new(stream).unwrap());

        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(recordbatches.pretty_print().unwrap(), expected_output);
    }

    async fn new_dist_table() -> DistTable {
        let column_schemas = vec![
            ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("row_id", ConcreteDataType::int32_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas.clone()));

        let (dist_instance, datanode_instances) = crate::tests::create_dist_instance().await;
        let catalog_manager = dist_instance.catalog_manager();
        let table_routes = catalog_manager.table_routes();
        let datanode_clients = catalog_manager.datanode_clients();

        let table_name = TableName::new("greptime", "public", "dist_numbers");

        let sql = "
            CREATE TABLE greptime.public.dist_numbers (
                ts BIGINT,
                a INT,
                row_id INT,
                TIME INDEX (ts),
            )
            PARTITION BY RANGE COLUMNS (a) (
                PARTITION r0 VALUES LESS THAN (10),
                PARTITION r1 VALUES LESS THAN (20),
                PARTITION r2 VALUES LESS THAN (50),
                PARTITION r3 VALUES LESS THAN (MAXVALUE),
            )
            ENGINE=mito";

        let create_table =
            match ParserContext::create_with_dialect(sql, &sqlparser::dialect::GenericDialect {})
                .unwrap()
                .pop()
                .unwrap()
            {
                Statement::CreateTable(c) => c,
                _ => unreachable!(),
            };

        let mut expr = DefaultCreateExprFactory
            .create_expr_by_stmt(&create_table)
            .await
            .unwrap();
        let _result = dist_instance
            .create_table(&mut expr, create_table.partitions)
            .await
            .unwrap();

        let table_route = table_routes.get_route(&table_name).await.unwrap();

        let mut region_to_datanode_mapping = HashMap::new();
        for region_route in table_route.region_routes.iter() {
            let region_id = region_route.region.id as u32;
            let datanode_id = region_route.leader_peer.as_ref().unwrap().id;
            region_to_datanode_mapping.insert(region_id, datanode_id);
        }

        let mut global_start_ts = 1;
        let regional_numbers = vec![
            (0, (0..5).collect::<Vec<i32>>()),
            (1, (10..15).collect::<Vec<i32>>()),
            (2, (30..35).collect::<Vec<i32>>()),
            (3, (100..105).collect::<Vec<i32>>()),
        ];
        for (region_id, numbers) in regional_numbers {
            let datanode_id = *region_to_datanode_mapping.get(&region_id).unwrap();
            let instance = datanode_instances.get(&datanode_id).unwrap().clone();

            let start_ts = global_start_ts;
            global_start_ts += numbers.len() as i64;

            insert_testing_data(&table_name, instance.clone(), numbers, start_ts).await;
        }

        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![])
            .next_column_id(1)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(&table_name.table_name)
            .meta(meta)
            .build()
            .unwrap();
        DistTable {
            table_name,
            table_info: Arc::new(table_info),
            table_routes,
            datanode_clients,
        }
    }

    async fn insert_testing_data(
        table_name: &TableName,
        dn_instance: Arc<Instance>,
        data: Vec<i32>,
        start_ts: i64,
    ) {
        let row_count = data.len() as u32;
        let columns = vec![
            Column {
                column_name: "ts".to_string(),
                values: Some(column::Values {
                    i64_values: (start_ts..start_ts + row_count as i64).collect::<Vec<i64>>(),
                    ..Default::default()
                }),
                datatype: ColumnDataType::Int64 as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
            Column {
                column_name: "a".to_string(),
                values: Some(column::Values {
                    i32_values: data,
                    ..Default::default()
                }),
                datatype: ColumnDataType::Int32 as i32,
                ..Default::default()
            },
            Column {
                column_name: "row_id".to_string(),
                values: Some(column::Values {
                    i32_values: (1..=row_count as i32).collect::<Vec<i32>>(),
                    ..Default::default()
                }),
                datatype: ColumnDataType::Int32 as i32,
                ..Default::default()
            },
        ];
        let request = InsertRequest {
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
            columns,
            row_count,
            region_number: 0,
        };
        dn_instance.handle_insert(request).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_find_regions() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            true,
        )]));
        let table_name = TableName::new("greptime", "public", "foo");
        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![])
            .next_column_id(1)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(&table_name.table_name)
            .meta(meta)
            .build()
            .unwrap();
        let table = DistTable {
            table_name,
            table_info: Arc::new(table_info),
            table_routes: Arc::new(TableRoutes::new(Arc::new(MetaClient::default()))),
            datanode_clients: Arc::new(DatanodeClients::new()),
        };

        // PARTITION BY RANGE (a) (
        //   PARTITION r1 VALUES LESS THAN (10),
        //   PARTITION r2 VALUES LESS THAN (20),
        //   PARTITION r3 VALUES LESS THAN (50),
        //   PARTITION r4 VALUES LESS THAN (MAXVALUE),
        // )
        let partition_rule: PartitionRuleRef<Error> = Arc::new(RangePartitionRule::new(
            "a",
            vec![10_i32.into(), 20_i32.into(), 50_i32.into()],
            vec![0_u32, 1, 2, 3],
        )) as _;

        let test = |filters: Vec<Expr>, expect_regions: Vec<RegionNumber>| {
            let mut regions = table
                .find_regions(partition_rule.clone(), filters.as_slice())
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
            vec![binary_expr(col("b"), Operator::Like, lit("foo%")).into()], // b LIKE 'foo%'
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
        let regions = table.find_regions(
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
            error::Error::FindRegions { .. }
        ));
    }
}
