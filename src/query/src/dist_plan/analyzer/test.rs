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

use std::pin::Pin;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_telemetry::init_default_ut_logging;
use datafusion::datasource::DefaultTableSource;
use datafusion::functions_aggregate::expr_fn::avg;
use datafusion::functions_aggregate::min_max::min;
use datafusion_common::JoinType;
use datafusion_expr::{col, lit, Expr, LogicalPlanBuilder};
use datafusion_sql::TableReference;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use futures::task::{Context, Poll};
use futures::Stream;
use pretty_assertions::assert_eq;
use store_api::data_source::DataSource;
use store_api::storage::ScanRequest;
use table::metadata::{
    FilterPushDownType, TableId, TableInfoBuilder, TableInfoRef, TableMeta, TableType,
};
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::NumbersTable;
use table::{Table, TableRef};

use super::*;

struct TestTable;

impl TestTable {
    pub fn table_with_name(table_id: TableId, name: String) -> TableRef {
        let data_source = Arc::new(TestDataSource::new(Self::schema()));
        let table = Table::new(
            Self::table_info(table_id, name, "test_engine".to_string()),
            FilterPushDownType::Unsupported,
            data_source,
        );
        Arc::new(table)
    }

    pub fn schema() -> SchemaRef {
        let column_schemas = vec![
            ColumnSchema::new("pk1", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("pk2", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("pk3", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
        ];
        let schema = SchemaBuilder::try_from_columns(column_schemas)
            .unwrap()
            .build()
            .unwrap();
        Arc::new(schema)
    }

    pub fn table_info(table_id: TableId, name: String, engine: String) -> TableInfoRef {
        let table_meta = TableMeta {
            schema: Self::schema(),
            primary_key_indices: vec![0, 1, 2],
            value_indices: vec![4],
            engine,
            region_numbers: vec![0, 1],
            next_column_id: 5,
            options: Default::default(),
            created_on: Default::default(),
            partition_key_indices: vec![0, 1],
            column_ids: vec![0, 1, 2, 3, 4],
        };

        let table_info = TableInfoBuilder::default()
            .table_id(table_id)
            .name(name)
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .table_version(0)
            .table_type(TableType::Base)
            .meta(table_meta)
            .build()
            .unwrap();
        Arc::new(table_info)
    }
}

struct TestDataSource {
    schema: SchemaRef,
}

impl TestDataSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl DataSource for TestDataSource {
    fn get_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream, BoxedError> {
        let projected_schema = match &request.projection {
            Some(projection) => Arc::new(self.schema.try_project(projection).unwrap()),
            None => self.schema.clone(),
        };
        Ok(Box::pin(EmptyStream {
            schema: projected_schema,
        }))
    }
}

struct EmptyStream {
    schema: SchemaRef,
}

impl RecordBatchStream for EmptyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

impl Stream for EmptyStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// test plan like:
/// ```
/// Aggregate: min(t.number)
///  Projection: t.number
/// ```
/// which means aggr introduce new column requirements that shouldn't be updated in lower projection
///
/// this help test expand need actually add new column requirements
/// because `Sort`/`Limit` doesn't introduce new column requirements
/// only `Aggregate` does
#[test]
fn expand_proj_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    // TODO(discord9): change to partitioned table
    let test_table = TestTable::table_with_name(0, "numbers".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![Expr::Column(Column::new(
            Some(TableReference::bare("t")),
            "number",
        ))])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: min(t.number)",
        "  Projection: min(min(t.number)) AS min(t.number)",
        "    Aggregate: groupBy=[[]], aggr=[[min(min(t.number))]]",
        "      MergeScan [is_placeholder=false]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// should only expand `Sort`
#[test]
fn expand_proj_sort_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "numbers".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .project(vec![Expr::Column(Column::new(
            Some(TableReference::bare("t")),
            "number",
        ))])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Aggregate: groupBy=[[]], aggr=[[min(t.number)]]",
        "  Projection: t.number",
        "    MergeSort: t.pk1 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// should only expand `Sort`
#[test]
fn expand_proj_sort_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "numbers".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk3").sort(true, false)])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "  Projection: t.number, t.pk1, t.pk2",
        "    MergeSort: t.pk3 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_sort_limit_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "numbers".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk3").sort(true, false)])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "  Projection: t.number, t.pk1, t.pk2",
        "    Limit: skip=0, fetch=10",
        "      MergeSort: t.pk3 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[ignore = "Projection is disabled for https://github.com/apache/arrow-datafusion/issues/6489"]
#[test]
fn transform_simple_projection_filter() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .filter(col("number").lt(lit(10)))
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .distinct()
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Distinct:",
        "  MergeScan [is_placeholder=false]",
        "    Distinct:",
        "      Projection: t.number",
        "        Filter: t.number < Int32(10)",
        "          TableScan: t",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_aggregator() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![avg(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = "Projection: avg(t.number)\
        \n  MergeScan [is_placeholder=false]";
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_distinct_order() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(vec![col("number").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = ["Projection: t.number", "  MergeScan [is_placeholder=false]"].join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_single_limit() {
    let numbers_table = NumbersTable::table(0);
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(numbers_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = "Projection: t.number\
        \n  MergeScan [is_placeholder=false]";
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_unalighed_join_with_alias() {
    let left = NumbersTable::table(0);
    let right = NumbersTable::table(1);
    let left_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(left),
    )));
    let right_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(right),
    )));

    let right_plan = LogicalPlanBuilder::scan_with_filters("t", right_source, None, vec![])
        .unwrap()
        .alias("right")
        .unwrap()
        .build()
        .unwrap();

    let plan = LogicalPlanBuilder::scan_with_filters("t", left_source, None, vec![])
        .unwrap()
        .join_on(
            right_plan,
            JoinType::LeftSemi,
            vec![col("t.number").eq(col("right.number"))],
        )
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=1",
        "  LeftSemi Join:  Filter: t.number = right.number",
        "    Projection: t.number",
        "      MergeScan [is_placeholder=false]",
        "    SubqueryAlias: right",
        "      Projection: t.number",
        "        MergeScan [is_placeholder=false]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}
