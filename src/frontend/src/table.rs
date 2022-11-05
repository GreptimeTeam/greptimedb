mod insert;

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use common_query::error::Result as QueryResult;
use common_query::logical_plan::Expr;
use common_query::physical_plan::{PhysicalPlan, PhysicalPlanRef};
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_plan::Expr as DfExpr;
use datafusion::physical_plan::Partitioning;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use snafu::prelude::*;
use store_api::storage::RegionId;
use table::error::Error as TableError;
use table::metadata::{FilterPushDownType, TableInfoRef};
use table::Table;
use tokio::sync::RwLock;

use crate::error::{self, Error, Result};
use crate::mock::{DatanodeId, DatanodeInstance, TableScanPlan};
use crate::partitioning::{Operator, PartitionExpr, PartitionRuleRef};

struct DistTable {
    table_name: String,
    schema: SchemaRef,
    partition_rule: PartitionRuleRef<Error>,
    region_dist_map: HashMap<RegionId, DatanodeId>,
    datanode_instances: HashMap<DatanodeId, DatanodeInstance>,
}

#[async_trait]
impl Table for DistTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        unimplemented!()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> table::Result<PhysicalPlanRef> {
        let regions = self.find_regions(filters).map_err(TableError::new)?;
        let datanodes = self.find_datanodes(regions).map_err(TableError::new)?;

        let partition_execs = datanodes
            .iter()
            .map(|(datanode, _regions)| {
                let datanode_instance = self
                    .datanode_instances
                    .get(datanode)
                    .context(error::DatanodeInstanceSnafu {
                        datanode: *datanode,
                    })?
                    .clone();
                // TODO(LFC): Pass in "regions" when Datanode supports multi regions for a table.
                Ok(PartitionExec {
                    table_name: self.table_name.clone(),
                    datanode_instance,
                    projection: projection.clone(),
                    filters: filters.to_vec(),
                    limit,
                    batches: Arc::new(RwLock::new(None)),
                })
            })
            .collect::<Result<Vec<PartitionExec>>>()
            .map_err(TableError::new)?;

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
    // TODO(LFC): Finding regions now seems less efficient, should be further looked into.
    fn find_regions(&self, filters: &[Expr]) -> Result<Vec<RegionId>> {
        let regions = if let Some((first, rest)) = filters.split_first() {
            let mut target = self.find_regions0(first)?;
            for filter in rest {
                let regions = self.find_regions0(filter)?;

                // When all filters are provided as a collection, it often implicitly states that
                // "all filters must be satisfied". So we join all the results here.
                target.retain(|x| regions.contains(x));

                // Failed fast, empty collection join any is empty.
                if target.is_empty() {
                    break;
                }
            }
            target.into_iter().collect::<Vec<RegionId>>()
        } else {
            self.partition_rule.find_regions(&[])?
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
    fn find_regions0(&self, filter: &Expr) -> Result<HashSet<RegionId>> {
        let expr = filter.df_expr();
        match expr {
            DfExpr::BinaryExpr { left, op, right } if is_compare_op(op) => {
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
                    return Ok(self
                        .partition_rule
                        .find_regions(&[PartitionExpr::new(column, op, value)])?
                        .into_iter()
                        .collect::<HashSet<RegionId>>());
                }
            }
            DfExpr::BinaryExpr { left, op, right }
                if matches!(op, Operator::And | Operator::Or) =>
            {
                let left_regions = self.find_regions0(&(*left.clone()).into())?;
                let right_regions = self.find_regions0(&(*right.clone()).into())?;
                let regions = match op {
                    Operator::And => left_regions
                        .intersection(&right_regions)
                        .cloned()
                        .collect::<HashSet<RegionId>>(),
                    Operator::Or => left_regions
                        .union(&right_regions)
                        .cloned()
                        .collect::<HashSet<RegionId>>(),
                    _ => unreachable!(),
                };
                return Ok(regions);
            }
            _ => (),
        }

        // Returns all regions for not supported partition expr as a safety hatch.
        Ok(self
            .partition_rule
            .find_regions(&[])?
            .into_iter()
            .collect::<HashSet<RegionId>>())
    }

    fn find_datanodes(&self, regions: Vec<RegionId>) -> Result<HashMap<DatanodeId, Vec<RegionId>>> {
        let mut datanodes = HashMap::new();
        for region in regions.iter() {
            let datanode = *self
                .region_dist_map
                .get(region)
                .context(error::FindDatanodeSnafu { region: *region })?;
            datanodes
                .entry(datanode)
                .or_insert_with(Vec::new)
                .push(*region);
        }
        Ok(datanodes)
    }
}

fn project_schema(table_schema: SchemaRef, projection: &Option<Vec<usize>>) -> SchemaRef {
    if let Some(projection) = &projection {
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
    partition_execs: Vec<PartitionExec>,
}

#[async_trait]
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

    async fn execute(
        &self,
        partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> QueryResult<SendableRecordBatchStream> {
        let exec = &self.partition_execs[partition];
        exec.maybe_init().await;
        Ok(exec.as_stream().await)
    }
}

#[derive(Debug)]
struct PartitionExec {
    table_name: String,
    datanode_instance: DatanodeInstance,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    batches: Arc<RwLock<Option<RecordBatches>>>,
}

impl PartitionExec {
    async fn maybe_init(&self) {
        if self.batches.read().await.is_some() {
            return;
        }

        let mut batches = self.batches.write().await;
        if batches.is_some() {
            return;
        }

        let plan = TableScanPlan {
            table_name: self.table_name.clone(),
            projection: self.projection.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
        };
        let result = self.datanode_instance.grpc_table_scan(plan).await;
        let _ = batches.insert(result);
    }

    async fn as_stream(&self) -> SendableRecordBatchStream {
        let batches = self.batches.read().await;
        batches
            .as_ref()
            .expect("should have been initialized in \"maybe_init\"")
            .as_stream()
    }
}

// FIXME(LFC): no allow, for clippy temporarily
#[allow(clippy::print_stdout)]
#[cfg(test)]
mod test {
    use catalog::RegisterTableRequest;
    use client::Database;
    use common_recordbatch::{util, RecordBatch};
    use datafusion::arrow_print;
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
    use datafusion_expr::expr_fn::col;
    use datafusion_expr::expr_fn::{and, binary_expr, or};
    use datafusion_expr::lit;
    use datanode::datanode::{DatanodeOptions, ObjectStoreConfig};
    use datanode::instance::Instance;
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Int32Vector, UInt32Vector};
    use table::test_util::MemTable;
    use table::TableRef;
    use tempdir::TempDir;

    use super::*;
    use crate::partitioning::range::RangePartitionRule;

    #[tokio::test]
    async fn test_dist_table_scan() {
        let table = Arc::new(new_dist_table().await);

        // should scan all regions
        // select * from numbers
        let projection = None;
        let filters = vec![];
        exec_table_scan(table.clone(), projection, filters, None).await;
        println!();

        // should scan only region 1
        // select a, row_id from numbers where a < 10
        let projection = Some(vec![0, 1]);
        let filters = vec![binary_expr(col("a"), Operator::Lt, lit(10)).into()];
        exec_table_scan(table.clone(), projection, filters, None).await;
        println!();

        // should scan region 1 and 2
        // select a, row_id from numbers where a < 15
        let projection = Some(vec![0, 1]);
        let filters = vec![binary_expr(col("a"), Operator::Lt, lit(15)).into()];
        exec_table_scan(table.clone(), projection, filters, None).await;
        println!();

        // should scan region 2 and 3
        // select a, row_id from numbers where a < 40 and a >= 10
        let projection = Some(vec![0, 1]);
        let filters = vec![and(
            binary_expr(col("a"), Operator::Lt, lit(40)),
            binary_expr(col("a"), Operator::GtEq, lit(10)),
        )
        .into()];
        exec_table_scan(table.clone(), projection, filters, None).await;
        println!();

        // should scan all regions
        // select a, row_id from numbers where a < 1000 and row_id == 1
        let projection = Some(vec![0, 1]);
        let filters = vec![and(
            binary_expr(col("a"), Operator::Lt, lit(1000)),
            binary_expr(col("row_id"), Operator::Eq, lit(1)),
        )
        .into()];
        exec_table_scan(table.clone(), projection, filters, None).await;
    }

    async fn exec_table_scan(
        table: TableRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) {
        let table_scan = table
            .scan(&projection, filters.as_slice(), limit)
            .await
            .unwrap();

        for partition in 0..table_scan.output_partitioning().partition_count() {
            let result = table_scan
                .execute(partition, Arc::new(RuntimeEnv::default()))
                .await
                .unwrap();
            let recordbatches = util::collect(result).await.unwrap();

            let df_recordbatch = recordbatches
                .into_iter()
                .map(|r| r.df_recordbatch)
                .collect::<Vec<DfRecordBatch>>();

            println!("DataFusion partition {}:", partition);
            let pretty_print = arrow_print::write(&df_recordbatch);
            let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
            pretty_print.iter().for_each(|x| println!("{}", x));
        }
    }

    async fn new_dist_table() -> DistTable {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("row_id", ConcreteDataType::uint32_datatype(), true),
        ]));

        // PARTITION BY RANGE (a) (
        //   PARTITION r1 VALUES LESS THAN (10),
        //   PARTITION r2 VALUES LESS THAN (20),
        //   PARTITION r3 VALUES LESS THAN (50),
        //   PARTITION r4 VALUES LESS THAN (MAXVALUE),
        // )
        let partition_rule = RangePartitionRule::new(
            "a",
            vec![10_i32.into(), 20_i32.into(), 50_i32.into()],
            vec![1_u64, 2, 3, 4],
        );

        let table1 = new_memtable(schema.clone(), (0..5).collect::<Vec<i32>>());
        let table2 = new_memtable(schema.clone(), (10..15).collect::<Vec<i32>>());
        let table3 = new_memtable(schema.clone(), (30..35).collect::<Vec<i32>>());
        let table4 = new_memtable(schema.clone(), (100..105).collect::<Vec<i32>>());

        let instance1 = create_datanode_instance(1, table1).await;
        let instance2 = create_datanode_instance(2, table2).await;
        let instance3 = create_datanode_instance(3, table3).await;
        let instance4 = create_datanode_instance(4, table4).await;

        let datanode_instances = HashMap::from([
            (instance1.datanode_id, instance1),
            (instance2.datanode_id, instance2),
            (instance3.datanode_id, instance3),
            (instance4.datanode_id, instance4),
        ]);

        DistTable {
            table_name: "dist_numbers".to_string(),
            schema,
            partition_rule: Arc::new(partition_rule),
            region_dist_map: HashMap::from([(1_u64, 1), (2_u64, 2), (3_u64, 3), (4_u64, 4)]),
            datanode_instances,
        }
    }

    fn new_memtable(schema: SchemaRef, data: Vec<i32>) -> MemTable {
        let rows = data.len() as u32;
        let columns: Vec<VectorRef> = vec![
            // column "a"
            Arc::new(Int32Vector::from_slice(data)),
            // column "row_id"
            Arc::new(UInt32Vector::from_slice((1..=rows).collect::<Vec<u32>>())),
        ];
        let recordbatch = RecordBatch::new(schema, columns).unwrap();
        MemTable::new("dist_numbers", recordbatch)
    }

    async fn create_datanode_instance(
        datanode_id: DatanodeId,
        table: MemTable,
    ) -> DatanodeInstance {
        let wal_tmp_dir = TempDir::new_in("/tmp", "gt_wal_dist_table_test").unwrap();
        let data_tmp_dir = TempDir::new_in("/tmp", "gt_data_dist_table_test").unwrap();
        let opts = DatanodeOptions {
            wal_dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
            storage: ObjectStoreConfig::File {
                data_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
            },
            ..Default::default()
        };

        let instance = Arc::new(Instance::new(&opts).await.unwrap());
        instance.start().await.unwrap();

        let catalog_manager = instance.catalog_manager().clone();
        catalog_manager
            .register_table(RegisterTableRequest {
                catalog: "greptime".to_string(),
                schema: "public".to_string(),
                table_name: table.table_name().to_string(),
                table_id: 1234,
                table: Arc::new(table),
            })
            .await
            .unwrap();

        let client = crate::tests::create_datanode_client(instance).await;
        DatanodeInstance::new(
            datanode_id,
            catalog_manager,
            Database::new("greptime", client),
        )
    }

    #[tokio::test]
    async fn test_find_regions() {
        let table = new_dist_table().await;

        let test = |filters: Vec<Expr>, expect_regions: Vec<u64>| {
            let mut regions = table.find_regions(filters.as_slice()).unwrap();
            regions.sort();

            assert_eq!(regions, expect_regions);
        };

        // test simple filter
        test(
            vec![binary_expr(col("a"), Operator::Lt, lit(10)).into()], // a < 10
            vec![1],
        );
        test(
            vec![binary_expr(col("a"), Operator::LtEq, lit(10)).into()], // a <= 10
            vec![1, 2],
        );
        test(
            vec![binary_expr(lit(20), Operator::Gt, col("a")).into()], // 20 > a
            vec![1, 2],
        );
        test(
            vec![binary_expr(lit(20), Operator::GtEq, col("a")).into()], // 20 >= a
            vec![1, 2, 3],
        );
        test(
            vec![binary_expr(lit(45), Operator::Eq, col("a")).into()], // 45 == a
            vec![3],
        );
        test(
            vec![binary_expr(col("a"), Operator::NotEq, lit(45)).into()], // a != 45
            vec![1, 2, 3, 4],
        );
        test(
            vec![binary_expr(col("a"), Operator::Gt, lit(50)).into()], // a > 50
            vec![4],
        );

        // test multiple filters
        test(
            vec![
                binary_expr(col("a"), Operator::Gt, lit(10)).into(),
                binary_expr(col("a"), Operator::Gt, lit(50)).into(),
            ], // [a > 10, a > 50]
            vec![4],
        );

        // test finding all regions when provided with not supported filters or not partition column
        test(
            vec![binary_expr(col("row_id"), Operator::LtEq, lit(123)).into()], // row_id <= 123
            vec![1, 2, 3, 4],
        );
        test(
            vec![binary_expr(col("b"), Operator::Like, lit("foo%")).into()], // b LIKE 'foo%'
            vec![1, 2, 3, 4],
        );
        test(
            vec![binary_expr(col("c"), Operator::Gt, lit(123)).into()], // c > 789
            vec![1, 2, 3, 4],
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
            vec![1, 2, 3, 4],
        );
        test(
            vec![or(
                binary_expr(col("a"), Operator::Lt, lit(20)),
                binary_expr(col("a"), Operator::GtEq, lit(20)),
            )
            .into()], // a < 20 OR a >= 20
            vec![1, 2, 3, 4],
        );
        test(
            vec![and(
                binary_expr(col("a"), Operator::Lt, lit(20)),
                binary_expr(col("a"), Operator::Lt, lit(50)),
            )
            .into()], // a < 20 AND a < 50
            vec![1, 2],
        );

        // test failed to find regions by contradictory filters
        let regions = table.find_regions(
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
