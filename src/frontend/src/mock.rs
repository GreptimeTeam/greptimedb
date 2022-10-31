// FIXME(LFC): no mock

use std::fmt::Formatter;
use std::sync::Arc;

use api::v1::InsertExpr;
use catalog::CatalogListRef;
use client::ObjectResult;
use client::{Database, Select};
use common_query::prelude::Expr;
use common_query::Output;
use common_recordbatch::util;
use common_recordbatch::RecordBatches;
use datafusion::logical_plan::LogicalPlan as DfLogicPlan;
use datafusion::logical_plan::LogicalPlanBuilder;
use datafusion_expr::Expr as DfExpr;
use datafusion_expr::Operator;
use datatypes::prelude::Value;
use datatypes::schema::SchemaRef;
use query::plan::LogicalPlan;
use table::table::adapter::DfTableProviderAdapter;

use crate::error::Error;
use crate::partition::PartitionRule;

#[derive(Clone)]
pub struct PartitionColumn {
    column_name: String,
    // Does not store the last "MAXVALUE" bound because it can make our binary search in finding regions easier
    // (besides, it's hard to represent "MAXVALUE" in our `Value`).
    // So the length of `value_lists` is one less than `regions`.
    partition_points: Vec<Value>,
    regions: Vec<Region>,
}

impl PartitionColumn {
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

    pub fn column_name(&self) -> &String {
        &self.column_name
    }

    pub fn partition_points(&self) -> &Vec<Value> {
        &self.partition_points
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

    fn find_region(&self, value: &Value) -> Region {
        match self.partition_points.binary_search(value) {
            Ok(i) => self.regions[i + 1].clone(),
            Err(i) => self.regions[i].clone(),
        }
    }
}

#[derive(Clone)]
pub struct RangePartitionRule {
    pub partition_column: PartitionColumn,
}

impl PartitionRule for RangePartitionRule {
    type Error = Error;

    fn partition_columns(&self) -> Vec<crate::spliter::ColumnName> {
        vec![self.partition_column.column_name.clone()]
    }

    fn find_region(
        &self,
        values: &crate::spliter::ValueList,
    ) -> std::result::Result<crate::spliter::RegionId, Self::Error> {
        debug_assert!(values.len() == 1, "Length of `region_keys` must be one");
        Ok(self.partition_column.find_region(&values[0]).id())
    }
}

impl RangePartitionRule {
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
pub struct Datanode {
    id: u64,
}

#[allow(dead_code)]
impl Datanode {
    pub(crate) fn new(id: u64) -> Self {
        Self { id }
    }
}

#[derive(Clone)]
pub struct DatanodeInstance {
    catalog_list: CatalogListRef,
    pub db: Database,
}

impl std::fmt::Debug for DatanodeInstance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatanodeInstance")
    }
}

impl DatanodeInstance {
    #[allow(dead_code)]
    pub(crate) fn new(catalog_list: CatalogListRef, db: Database) -> Self {
        Self { catalog_list, db }
    }

    pub(crate) async fn grpc_insert(&self, request: InsertExpr) -> client::Result<ObjectResult> {
        self.db.insert(request).await
    }

    pub(crate) async fn grpc_table_scan(&self, plan: TableScanPlan) -> RecordBatches {
        let mut recordbatches = Vec::new();
        for region in plan.regions.iter() {
            let logical_plan = self.build_logical_plan(&plan, region);
            let sql = to_sql(logical_plan);
            println!("build sql: {} for region {}", sql, region.id);
            let result = self.db.select(Select::Sql(sql)).await.unwrap();
            let output: Output = result.try_into().unwrap();

            let regional_recordbatches = &mut match output {
                Output::Stream(stream) => util::collect(stream).await.unwrap(),
                Output::RecordBatches(x) => x.take(),
                _ => unreachable!(),
            };
            recordbatches.append(regional_recordbatches);
        }

        let schema = recordbatches.first().unwrap().schema.clone();
        RecordBatches::try_new(schema, recordbatches).unwrap()
    }

    fn build_logical_plan(&self, table_scan: &TableScanPlan, region: &Region) -> LogicalPlan {
        let catalog = self.catalog_list.catalog("greptime").unwrap().unwrap();
        let schema = catalog.schema("public").unwrap().unwrap();
        let table = schema.table(&table_scan.table_name).unwrap().unwrap();
        let table_provider = Arc::new(DfTableProviderAdapter::new(table.clone()));

        let mut builder = LogicalPlanBuilder::scan_with_filters(
            table_scan.table_name.clone(),
            table_provider,
            table_scan.projection.clone(),
            table_scan
                .filters
                .iter()
                .map(|x| x.df_expr().clone())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        if let Some(limit) = table_scan.limit {
            builder = builder.limit(limit).unwrap();
        }

        let plan = builder.build().unwrap();
        println!("build logical plan: {:?}", plan);
        LogicalPlan::DfPlan(plan)
    }
}

fn to_sql(plan: LogicalPlan) -> String {
    let plan = match plan {
        LogicalPlan::DfPlan(df_plan) => df_plan,
    };
    let table_scan = match plan {
        DfLogicPlan::TableScan(table_scan) => table_scan,
        _ => unreachable!("unknown plan: {:?}", plan),
    };

    let schema: SchemaRef = Arc::new(table_scan.source.schema().try_into().unwrap());

    let projection = table_scan
        .projection
        .map(|x| {
            x.iter()
                .map(|i| schema.column_name_by_index(*i).to_string())
                .collect::<Vec<String>>()
        })
        .unwrap_or_else(|| {
            schema
                .column_schemas()
                .iter()
                .map(|x| x.name.clone())
                .collect::<Vec<String>>()
        })
        .join(", ");

    let mut sql = format!("select {} from {}", projection, &table_scan.table_name);

    let filters = table_scan
        .filters
        .iter()
        .map(expr_to_sql)
        .collect::<Vec<String>>()
        .join(" AND ");
    if !filters.is_empty() {
        sql.push_str(&format!(" where {}", filters));
    }

    if let Some(limit) = table_scan.limit {
        sql.push_str(&format!(" limit {}", limit));
    }
    sql
}

fn expr_to_sql(expr: &DfExpr) -> String {
    match expr {
        DfExpr::BinaryExpr {
            ref left,
            ref right,
            ref op,
        } => format!(
            "{} {} {}",
            expr_to_sql(left.as_ref()),
            op,
            expr_to_sql(right.as_ref())
        ),
        DfExpr::Column(c) => c.name.clone(),
        DfExpr::Literal(sv) => {
            let v: Value = Value::try_from(sv.clone()).unwrap();
            if v.data_type().is_string() {
                format!("'{}'", sv)
            } else {
                format!("{}", sv)
            }
        }
        _ => unimplemented!("not implemented for {:?}", expr),
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
    fn test_find_region() {
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

        let value: Value = "h".into();
        let region_id = rule.find_region(&vec![value]).unwrap();
        assert_eq!(1, region_id);

        let value: Value = "sa".into();
        let region_id = rule.find_region(&vec![value]).unwrap();
        assert_eq!(2, region_id);

        let value: Value = "sj".into();
        let region_id = rule.find_region(&vec![value]).unwrap();
        assert_eq!(3, region_id);

        let value: Value = "t".into();
        let region_id = rule.find_region(&vec![value]).unwrap();
        assert_eq!(4, region_id);
    }

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
