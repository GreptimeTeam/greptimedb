// FIXME(LFC): no mock

use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use catalog::{CatalogListRef, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::prelude::Expr;
use common_query::Output;
use common_recordbatch::util;
use common_recordbatch::RecordBatches;
use datafusion::logical_plan::LogicalPlanBuilder;
use datafusion_expr::Operator;
use datatypes::prelude::Value;
use query::plan::LogicalPlan;
use query::QueryEngineRef;
use table::table::adapter::DfTableProviderAdapter;
use tokio::sync::Mutex;

struct PartitionColumn {
    column_name: String,
    // Does not store the last "MAXVALUE" bound because it can make our binary search in finding regions easier
    // (besides, it's hard to represent "MAXVALUE" in our `Value`).
    // So the length of `value_lists` is one less than `regions`.
    partition_points: Vec<Value>,
    regions: Vec<Region>,
}

impl PartitionColumn {
    #[allow(dead_code)]
    fn new(
        column_name: impl Into<String>,
        partition_points: Vec<Value>,
        regions: Vec<Region>,
    ) -> Self {
        Self {
            column_name: column_name.into(),
            partition_points,
            regions,
        }
    }

    fn column_name(&self) -> &String {
        &self.column_name
    }

    fn all_regions(&self) -> Vec<Region> {
        self.regions.clone()
    }

    fn find_regions(&self, op: Operator, value: &Value) -> Vec<Region> {
        match self.partition_points.binary_search(value) {
            Ok(i) => match op {
                Operator::Lt => self.regions[..=i].to_vec(),
                Operator::LtEq => self.regions[..=(i + 1)].to_vec(),
                Operator::Eq => vec![self.regions[i + 1].clone()],
                Operator::Gt | Operator::GtEq => self.regions[(i + 1)..].to_vec(),
                Operator::NotEq => self.regions.clone(),
                _ => unreachable!(),
            },
            Err(i) => match op {
                Operator::Lt | Operator::LtEq => self.regions[..=i].to_vec(),
                Operator::Eq => vec![self.regions[i].clone()],
                Operator::Gt | Operator::GtEq => self.regions[i..].to_vec(),
                Operator::NotEq => self.regions.clone(),
                _ => unreachable!(),
            },
        }
    }
}

pub(crate) struct RangePartitionRule {
    partition_column: PartitionColumn,
}

impl RangePartitionRule {
    #[allow(dead_code)]
    pub(crate) fn new(
        column_name: impl Into<String>,
        partition_points: Vec<Value>,
        regions: Vec<Region>,
    ) -> Self {
        let partition_column = PartitionColumn::new(column_name, partition_points, regions);
        Self { partition_column }
    }

    pub(crate) fn find_regions(&self, partition_expr: &PartitionExpr) -> Vec<Region> {
        let PartitionExpr { column, op, value } = partition_expr;
        if column == self.partition_column.column_name() {
            self.partition_column.find_regions(*op, value)
        } else {
            self.partition_column.all_regions()
        }
    }

    pub(crate) fn all_regions(&self) -> Vec<Region> {
        self.partition_column.all_regions()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Region {
    id: u64,
}

#[allow(dead_code)]
impl Region {
    pub(crate) fn new(id: u64) -> Self {
        Self { id }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct PartitionExpr {
    column: String,
    op: Operator,
    value: Value,
}

impl PartitionExpr {
    pub(crate) fn new(column: impl Into<String>, op: Operator, value: Value) -> Self {
        Self {
            column: column.into(),
            op,
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Datanode {
    id: u64,
}

#[allow(dead_code)]
impl Datanode {
    pub(crate) fn new(id: u64) -> Self {
        Self { id }
    }
}

#[derive(Clone)]
pub(crate) struct DatanodeInstance {
    catalog_list: CatalogListRef,
    query_engines: Arc<Mutex<HashMap<Region, QueryEngineRef>>>,
}

impl std::fmt::Debug for DatanodeInstance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatanodeInstance")
    }
}

impl DatanodeInstance {
    #[allow(dead_code)]
    pub(crate) fn new(
        catalog_list: CatalogListRef,
        query_engines: HashMap<Region, QueryEngineRef>,
    ) -> Self {
        Self {
            catalog_list,
            query_engines: Arc::new(Mutex::new(query_engines)),
        }
    }

    pub(crate) async fn grpc_table_scan(&self, plan: TableScanPlan) -> RecordBatches {
        let query_engines = self.query_engines.lock().await;

        let mut recordbatches = Vec::new();
        for region in plan.regions.iter() {
            let logical_plan = self.build_logical_plan(&plan, region);
            let engine = query_engines.get(region).unwrap();
            let output = engine.execute(&logical_plan).await.unwrap();

            let regional_recordbatches = &mut match output {
                Output::Stream(stream) => util::collect(stream).await.unwrap(),
                _ => unreachable!(),
            };
            recordbatches.append(regional_recordbatches);
        }

        let schema = recordbatches.first().unwrap().schema.clone();
        RecordBatches::try_new(schema, recordbatches).unwrap()
    }

    fn build_logical_plan(&self, table_scan: &TableScanPlan, region: &Region) -> LogicalPlan {
        let table_name = format!("{}-region-{}", table_scan.table_name, region.id);

        let catalog = self.catalog_list.catalog(DEFAULT_CATALOG_NAME).unwrap();
        let schema = catalog.schema(DEFAULT_SCHEMA_NAME).unwrap();
        let table = schema.table(&table_name).unwrap();
        let table_provider = Arc::new(DfTableProviderAdapter::new(table.clone()));

        let mut builder = LogicalPlanBuilder::scan(
            table_scan.table_name.clone(),
            table_provider,
            table_scan.projection.clone(),
        )
        .unwrap();
        if let Some(limit) = table_scan.limit {
            builder = builder.limit(limit).unwrap();
        }
        for filter in table_scan.filters.iter() {
            builder = builder.filter(filter.df_expr().clone()).unwrap();
        }

        LogicalPlan::DfPlan(builder.build().unwrap())
    }
}

#[derive(Debug)]
pub(crate) struct TableScanPlan {
    pub table_name: String,
    pub regions: Vec<Region>,
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_find_regions() {
        // PARTITION BY RANGE (a) (
        //   PARTITION r1 VALUES LESS THAN ('hz'),
        //   PARTITION r2 VALUES LESS THAN ('sh'),
        //   PARTITION r3 VALUES LESS THAN ('sz'),
        //   PARTITION r4 VALUES LESS THAN (MAXVALUE),
        // )
        let rule = RangePartitionRule::new(
            "a",
            vec!["hz".into(), "sh".into(), "sz".into()],
            vec![
                Region::new(1),
                Region::new(2),
                Region::new(3),
                Region::new(4),
            ],
        );

        let test = |column: &str, op: Operator, value: &str, expect_regions: Vec<u64>| {
            let partition_expr = PartitionExpr::new(column, op, value.into());
            let regions = rule.find_regions(&partition_expr);
            assert_eq!(
                regions,
                expect_regions
                    .into_iter()
                    .map(Region::new)
                    .collect::<Vec<Region>>()
            );
        };

        test("a", Operator::NotEq, "hz", vec![1, 2, 3, 4]);
        test("a", Operator::NotEq, "what", vec![1, 2, 3, 4]);

        test("a", Operator::GtEq, "ab", vec![1, 2, 3, 4]);
        test("a", Operator::GtEq, "hz", vec![2, 3, 4]);
        test("a", Operator::GtEq, "ijk", vec![2, 3, 4]);
        test("a", Operator::GtEq, "sh", vec![3, 4]);
        test("a", Operator::GtEq, "ssh", vec![3, 4]);
        test("a", Operator::GtEq, "sz", vec![4]);
        test("a", Operator::GtEq, "zz", vec![4]);

        test("a", Operator::Gt, "ab", vec![1, 2, 3, 4]);
        test("a", Operator::Gt, "hz", vec![2, 3, 4]);
        test("a", Operator::Gt, "ijk", vec![2, 3, 4]);
        test("a", Operator::Gt, "sh", vec![3, 4]);
        test("a", Operator::Gt, "ssh", vec![3, 4]);
        test("a", Operator::Gt, "sz", vec![4]);
        test("a", Operator::Gt, "zz", vec![4]);

        test("a", Operator::Eq, "ab", vec![1]);
        test("a", Operator::Eq, "hz", vec![2]);
        test("a", Operator::Eq, "ijk", vec![2]);
        test("a", Operator::Eq, "sh", vec![3]);
        test("a", Operator::Eq, "ssh", vec![3]);
        test("a", Operator::Eq, "sz", vec![4]);
        test("a", Operator::Eq, "zz", vec![4]);

        test("a", Operator::Lt, "ab", vec![1]);
        test("a", Operator::Lt, "hz", vec![1]);
        test("a", Operator::Lt, "ijk", vec![1, 2]);
        test("a", Operator::Lt, "sh", vec![1, 2]);
        test("a", Operator::Lt, "ssh", vec![1, 2, 3]);
        test("a", Operator::Lt, "sz", vec![1, 2, 3]);
        test("a", Operator::Lt, "zz", vec![1, 2, 3, 4]);

        test("a", Operator::LtEq, "ab", vec![1]);
        test("a", Operator::LtEq, "hz", vec![1, 2]);
        test("a", Operator::LtEq, "ijk", vec![1, 2]);
        test("a", Operator::LtEq, "sh", vec![1, 2, 3]);
        test("a", Operator::LtEq, "ssh", vec![1, 2, 3]);
        test("a", Operator::LtEq, "sz", vec![1, 2, 3, 4]);
        test("a", Operator::LtEq, "zz", vec![1, 2, 3, 4]);

        test("b", Operator::Lt, "1", vec![1, 2, 3, 4]);
    }
}
