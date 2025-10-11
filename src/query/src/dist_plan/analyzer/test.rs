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

use arrow::datatypes::IntervalDayTime;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_function::aggrs::aggr_wrapper::{StateMergeHelper, StateWrapper};
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_telemetry::init_default_ut_logging;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::SessionState;
use datafusion::functions_aggregate::expr_fn::avg;
use datafusion::functions_aggregate::min_max::{max, min};
use datafusion::prelude::SessionContext;
use datafusion_common::JoinType;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{AggregateUDF, Expr, LogicalPlanBuilder, col, lit};
use datafusion_functions::datetime::date_bin;
use datafusion_sql::TableReference;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use futures::Stream;
use futures::task::{Context, Poll};
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

pub(crate) struct TestTable;

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

fn try_encode_decode_substrait(plan: &LogicalPlan, state: SessionState) {
    let sub_plan_bytes = substrait::DFLogicalSubstraitConvertor
        .encode(plan, crate::query_engine::DefaultSerializer)
        .unwrap();
    let inner = sub_plan_bytes.clone();
    let decoded_plan = futures::executor::block_on(async move {
        substrait::DFLogicalSubstraitConvertor
            .decode(inner, state)
            .await
    }).inspect_err(|e|{
use prost::Message;
        let sub_plan = substrait::substrait_proto_df::proto::Plan::decode(sub_plan_bytes).unwrap();
        common_telemetry::error!("Failed to decode substrait plan: {e},substrait plan: {sub_plan:#?}\nlogical plan: {plan:#?}");
    })
    .unwrap();

    assert_eq!(*plan, decoded_plan);
}

#[test]
fn expand_proj_sort_proj() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![col("number"), col("pk1"), col("pk2"), col("pk3")])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk1"),
            col("pk3"),
            col("pk1").eq(col("pk2")),
        ])
        .unwrap()
        .sort(vec![col("t.pk1 = t.pk2").sort(true, true)])
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.number",
        "  MergeSort: t.pk1 = t.pk2 ASC NULLS FIRST",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Projection: t.number, t.pk1 = t.pk2",
        "  Projection: t.number, t.pk1 = t.pk2", // notice both projections added `t.pk1 = t.pk2` column requirement
        "    Sort: t.pk1 = t.pk2 ASC NULLS FIRST",
        "      Projection: t.number, t.pk1, t.pk3, t.pk1 = t.pk2",
        "        Projection: t.number, t.pk1, t.pk2, t.pk3", // notice this projection doesn't add `t.pk1 = t.pk2` column requirement
        "          TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_sort_partial_proj() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![col("number"), col("pk1"), col("pk2"), col("pk3")])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk1"),
            col("pk3"),
            col("pk1").eq(col("pk2")),
        ])
        .unwrap()
        .sort(vec![col("t.pk1 = t.pk2").sort(true, true)])
        .unwrap()
        .project(vec![col("number"), col("t.pk1 = t.pk2").alias("eq_sorted")])
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.number",
        "  MergeSort: eq_sorted ASC NULLS FIRST", // notice how `eq_sorted` is used here
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Projection: t.number, eq_sorted", // notice how `eq_sorted` is added not `t.pk1 = t.pk2`
        "  Projection: t.number, t.pk1 = t.pk2 AS eq_sorted",
        "    Sort: t.pk1 = t.pk2 ASC NULLS FIRST",
        "      Projection: t.number, t.pk1, t.pk3, t.pk1 = t.pk2",
        "        Projection: t.number, t.pk1, t.pk2, t.pk3", // notice this projection doesn't add `t.pk1 = t.pk2` column requirement
        "          TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_sort_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.pk1, t.pk2, t.pk3, t.ts, t.number",
        "  Limit: skip=0, fetch=10",
        "    MergeSort: t.pk1 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Sort: t.pk1 ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Test merge sort can apply enforce dist requirement columns correctly and use the aliased column correctly, as there is
/// a aliased sort column, there is no need to add a duplicate sort column using it's original column name
#[test]
fn expand_sort_alias_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .project(vec![col("pk1").alias("something")])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: something",
        "  Limit: skip=0, fetch=10",
        "    MergeSort: something ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk1 AS something",
        "    Sort: t.pk1 ASC NULLS LAST",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// FIXME(discord9): alias to same name with col req makes it ambiguous
/// for now since it bugged, will use fallback plan rewriter to only push down table scan node
#[test]
fn expand_sort_alias_conflict_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .project(vec![col("pk2").alias("pk1")])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan.clone(), &config);
    assert!(result.is_err(), "Expected error for ambiguous alias");
    assert!(format!("{result:?}").contains("AmbiguousReference"));

    let mut config = ConfigOptions::default();
    config.extensions.insert(DistPlannerOptions {
        allow_query_fallback: true,
    });
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk2 AS pk1",
        "    Sort: t.pk1 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_sort_alias_conflict_but_not_really_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .project(vec![col("pk2").alias("t.pk1")])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.pk1",
        "  Limit: skip=0, fetch=10",
        "    MergeSort: t.pk1 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk2 AS t.pk1, t.pk1",
        "    Sort: t.pk1 ASC NULLS LAST",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// TODO(discord9): it is possible to expand `Sort` and `Limit` in the same step,
/// but it's too complicated to implement now, and probably not worth it since `Limit` already
/// greatly reduces the amount of data to sort.
#[test]
fn expand_limit_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Sort: t.pk1 ASC NULLS LAST",
        "  Projection: t.pk1, t.pk2, t.pk3, t.ts, t.number",
        "    Limit: skip=0, fetch=10",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_sort_limit_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .sort(vec![col("pk1").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Sort: t.pk1 ASC NULLS LAST",
        "  Projection: t.pk1, t.pk2, t.pk3, t.ts, t.number",
        "    Limit: skip=0, fetch=10",
        "      MergeSort: t.pk1 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Sort: t.pk1 ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// test plan like:
/// ```
/// Aggregate: min(t.number)
///  Projection: t.number
/// ```
/// which means aggr introduce new column requirements that shouldn't be updated in lower projection
///
/// this help test expand need actually add new column requirements
/// because ``Limit` doesn't introduce new column requirements
/// only `Sort/Aggregate` does, and for now since `aggregate` get expanded immediately, it's col requirements are not used anyway
#[test]
fn expand_proj_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![col("number")])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: min(t.number)",
        "  Aggregate: groupBy=[[]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__min_state(t.number)]]",
        "  Projection: t.number", // This Projection shouldn't add new column requirements
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Shouldn't push down the fake partition column aggregate(which is steppable)
/// as the `pk1` is a alias for `pk3` which is not partition column
#[test]
fn expand_proj_alias_fake_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk3").alias("pk1"),
            col("pk2").alias("pk3"),
        ])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk1").alias("pk2"),
            col("pk3").alias("pk1"),
        ])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: pk1, pk2, min(t.number)",
        "  Aggregate: groupBy=[[pk1, pk2]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[pk1, pk2]], aggr=[[__min_state(t.number)]]",
        "  Projection: t.number, pk1 AS pk2, pk3 AS pk1",
        "    Projection: t.number, t.pk3 AS pk1, t.pk2 AS pk3",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_alias_aliased_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk1").alias("pk3"),
            col("pk2").alias("pk4"),
        ])
        .unwrap()
        .project(vec![
            col("number"),
            col("pk3").alias("pk42"),
            col("pk4").alias("pk43"),
        ])
        .unwrap()
        .aggregate(vec![col("pk42"), col("pk43")], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: pk42, pk43, min(t.number)",
        "  MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[pk42, pk43]], aggr=[[min(t.number)]]",
        "  Projection: t.number, pk3 AS pk42, pk4 AS pk43",
        "    Projection: t.number, t.pk1 AS pk3, t.pk2 AS pk4",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// notice that step aggr then part col aggr seems impossible as the partition columns for part col aggr
/// can't pass through the step aggr without making step aggr also a part col aggr
/// so here only test part col aggr -> step aggr case
#[test]
fn expand_part_col_aggr_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![max(col("number"))])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("max(t.number)"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: min(max(t.number))",
        "  Aggregate: groupBy=[[]], aggr=[[__min_merge(__min_state(max(t.number))) AS min(max(t.number))]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__min_state(max(t.number))]]",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[max(t.number)]]",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_step_aggr_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![max(col("number"))])
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("max(t.number)"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Aggregate: groupBy=[[]], aggr=[[min(max(t.number))]]",
        "  Projection: max(t.number)",
        "    Aggregate: groupBy=[[]], aggr=[[__max_merge(__max_state(t.number)) AS max(t.number)]]",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__max_state(t.number)]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_part_col_aggr_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![max(col("number"))])
        .unwrap()
        .aggregate(
            vec![col("pk1"), col("pk2")],
            vec![min(col("max(t.number)"))],
        )
        .unwrap()
        .build()
        .unwrap();

    let expected_original = [
        // See DataFusion #14860 for change details.
        "Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(max(t.number))]]",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[max(t.number)]]",
        "    TableScan: t",
    ]
    .join("\n");
    assert_eq!(expected_original, plan.to_string());

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: t.pk1, t.pk2, min(max(t.number))",
        "  MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(max(t.number))]]",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[max(t.number)]]",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_step_aggr_proj() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1")], vec![min(col("number"))])
        .unwrap()
        .project(vec![col("min(t.number)")])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        "Projection: min(t.number)",
        "  Projection: t.pk1, min(t.number)",
        "    Aggregate: groupBy=[[t.pk1]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[t.pk1]], aggr=[[__min_state(t.number)]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// should only expand `Sort`, notice `Sort` before `Aggregate` usually can and
/// will be optimized out, and dist planner shouldn't handle that case, but
/// for now, still handle that be expanding the `Sort` node
#[test]
fn expand_proj_sort_step_aggr_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
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
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=10",
        "  Aggregate: groupBy=[[]], aggr=[[min(t.number)]]",
        "    Projection: t.number",
        "      MergeSort: t.pk1 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Projection: t.number, t.pk1",
        "  Sort: t.pk1 ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_sort_limit_step_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
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
        .limit(0, Some(10))
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
        "    Limit: skip=0, fetch=10",
        "      MergeSort: t.pk1 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number, t.pk1",
        "    Sort: t.pk1 ASC NULLS LAST",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_limit_step_aggr_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
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
        .limit(0, Some(10))
        .unwrap()
        .aggregate(Vec::<Expr>::new(), vec![min(col("number"))])
        .unwrap()
        .sort(vec![col("min(t.number)").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Sort: min(t.number) ASC NULLS LAST",
        "  Aggregate: groupBy=[[]], aggr=[[min(t.number)]]",
        "    Projection: t.number",
        "      Limit: skip=0, fetch=10",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_sort_part_col_aggr_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
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
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=10",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "    Projection: t.number, t.pk1, t.pk2",
        "      MergeSort: t.pk3 ASC NULLS LAST",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Projection: t.number, t.pk1, t.pk2, t.pk3",
        "  Sort: t.pk3 ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_sort_limit_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
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
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number, t.pk1, t.pk2, t.pk3",
        "    Sort: t.pk3 ASC NULLS LAST",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}
#[test]
fn expand_proj_part_col_aggr_limit_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .sort(vec![col("pk2").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Sort: t.pk2 ASC NULLS LAST",
        "  Projection: t.pk1, t.pk2, min(t.number)",
        "    Limit: skip=0, fetch=10",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "    Projection: t.number, t.pk1, t.pk2",
        "      TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_part_col_aggr_sort_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .sort(vec![col("pk2").sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: t.pk1, t.pk2, min(t.number)",
        "  Limit: skip=0, fetch=10",
        "    MergeSort: t.pk2 ASC NULLS LAST",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Sort: t.pk2 ASC NULLS LAST",
        "    Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "      Projection: t.number, t.pk1, t.pk2",
        "        TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_limit_part_col_aggr_sort() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
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
        .sort(vec![col("pk2").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Sort: t.pk2 ASC NULLS LAST",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "    Projection: t.number, t.pk1, t.pk2",
        "      Limit: skip=0, fetch=10",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number, t.pk1, t.pk2",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn expand_proj_limit_sort_part_col_aggr() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .project(vec![
            Expr::Column(Column::new(Some(TableReference::bare("t")), "number")),
            col("pk1"),
            col("pk2"),
        ])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .sort(vec![col("pk2").sort(true, false)])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "  Sort: t.pk2 ASC NULLS LAST",
        "    Projection: t.number, t.pk1, t.pk2",
        "      Limit: skip=0, fetch=10",
        "        MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Projection: t.number, t.pk1, t.pk2",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Notice how this limit can't be push down, or results will be wrong
#[test]
fn expand_step_aggr_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1")], vec![min(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk1, min(t.number)",
        "    Aggregate: groupBy=[[t.pk1]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[t.pk1]], aggr=[[__min_state(t.number)]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// Test how avg get expanded
#[test]
fn expand_step_aggr_avg_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1")], vec![avg(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Limit: skip=0, fetch=10",
        "  Projection: t.pk1, avg(t.number)",
        "    Aggregate: groupBy=[[t.pk1]], aggr=[[__avg_merge(__avg_state(t.number)) AS avg(t.number)]]",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[t.pk1]], aggr=[[__avg_state(CAST(t.number AS Float64))]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

/// notice how `Limit` can still get expanded
#[test]
fn expand_part_col_aggr_limit() {
    // use logging for better debugging
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![col("pk1"), col("pk2")], vec![min(col("number"))])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: t.pk1, t.pk2, min(t.number)",
        "  Limit: skip=0, fetch=10",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Limit: skip=0, fetch=10",
        "  Aggregate: groupBy=[[t.pk1, t.pk2]], aggr=[[min(t.number)]]",
        "    TableScan: t",
        "]]",
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
        \n  MergeScan [is_placeholder=false, remote_input=[\
        \nAggregate: groupBy=[[]], aggr=[[avg(t.number)]]\
        \n  TableScan: t\
        \n]]";
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
    let expected = [
        "Projection: t.number",
        "  MergeScan [is_placeholder=false, remote_input=[
Sort: t.number ASC NULLS LAST
  Distinct:
    TableScan: t
]]",
    ]
    .join("\n");
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
        \n  MergeScan [is_placeholder=false, remote_input=[
Limit: skip=0, fetch=1
  TableScan: t
]]";
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
        "      MergeScan [is_placeholder=false, remote_input=[",
        "TableScan: t",
        "]]",
        "    Projection: right.number",
        "      MergeScan [is_placeholder=false, remote_input=[",
        "SubqueryAlias: right",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_subquery_sort_alias() {
    init_default_ut_logging();

    let test_table = TestTable::table_with_name(0, "numbers".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .alias("a")
        .unwrap()
        .sort(vec![col("a.number").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();
    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: a.pk1, a.pk2, a.pk3, a.ts, a.number",
        "  MergeSort: a.number ASC NULLS LAST",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Sort: a.number ASC NULLS LAST",
        "  SubqueryAlias: a",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn transform_sort_subquery_alias() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "numbers".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .sort(vec![col("t.number").sort(true, false)])
        .unwrap()
        .alias("a")
        .unwrap()
        .build()
        .unwrap();
    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();
    let expected = [
        "Projection: a.pk1, a.pk2, a.pk3, a.ts, a.number",
        "  MergeSort: a.number ASC NULLS LAST",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "SubqueryAlias: a",
        "  Sort: t.number ASC NULLS LAST",
        "    TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn date_bin_ts_group_by() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_source = Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(test_table),
    )));
    let date_bin_call = Expr::ScalarFunction(ScalarFunction::new_udf(
        date_bin(),
        vec![
            lit(datafusion_common::ScalarValue::IntervalDayTime(Some(
                IntervalDayTime::new(0, 60 * 1000), // 1 minute in millis
            ))),
            col("ts"),
        ],
    ));
    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(vec![date_bin_call], vec![min(col("number"))])
        .unwrap()
        .build()
        .unwrap();

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}.analyze(plan, &config).unwrap();

    let expected = [
        r#"Projection: date_bin(IntervalDayTime("IntervalDayTime { days: 0, milliseconds: 60000 }"),t.ts), min(t.number)"#,
        r#"  Aggregate: groupBy=[[date_bin(IntervalDayTime("IntervalDayTime { days: 0, milliseconds: 60000 }"),t.ts)]], aggr=[[__min_merge(__min_state(t.number)) AS min(t.number)]]"#,
        "    MergeScan [is_placeholder=false, remote_input=[",
        r#"Aggregate: groupBy=[[date_bin(IntervalDayTime("IntervalDayTime { days: 0, milliseconds: 60000 }"), t.ts)]], aggr=[[__min_state(t.number)]]"#,
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}

#[test]
fn test_last_value_order_by() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_provider = Arc::new(DfTableProviderAdapter::new(test_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider.clone() as _));
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("t"), table_provider.clone() as _)
        .unwrap();
    ctx.register_udaf(AggregateUDF::new_from_impl(
        StateWrapper::new(
            datafusion::functions_aggregate::first_last::last_value_udaf()
                .as_ref()
                .clone(),
        )
        .unwrap(),
    ));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source.clone(), None, vec![])
        .unwrap()
        .aggregate(
            Vec::<Expr>::new(),
            vec![datafusion::functions_aggregate::first_last::last_value(
                col("ts"),
                vec![col("ts").sort(true, true)],
            )],
        )
        .unwrap()
        .build()
        .unwrap();

    try_encode_decode_substrait(&plan, ctx.state());

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}
        .analyze(plan.clone(), &config)
        .unwrap();

    let expected = [
        "Projection: last_value(t.ts) ORDER BY [t.ts ASC NULLS FIRST]",
        "  Aggregate: groupBy=[[]], aggr=[[__last_value_merge(__last_value_state(t.ts) ORDER BY [t.ts ASC NULLS FIRST]) AS last_value(t.ts) ORDER BY [t.ts ASC NULLS FIRST]]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__last_value_state(t.ts) ORDER BY [t.ts ASC NULLS FIRST]]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());

    let LogicalPlan::Aggregate(aggr_plan) = plan else {
        panic!("expect Aggregate plan");
    };
    let split = StateMergeHelper::split_aggr_node(aggr_plan).unwrap();

    try_encode_decode_substrait(&split.lower_state, ctx.state());
}

/// try remove the order by to see if it still works
#[test]
fn test_last_value_no_order_by() {
    init_default_ut_logging();
    let test_table = TestTable::table_with_name(0, "t".to_string());
    let table_provider = Arc::new(DfTableProviderAdapter::new(test_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider.clone() as _));
    let ctx = SessionContext::new();
    ctx.register_table(TableReference::bare("t"), table_provider.clone() as _)
        .unwrap();
    ctx.register_udaf(AggregateUDF::new_from_impl(
        StateWrapper::new(
            datafusion::functions_aggregate::first_last::last_value_udaf()
                .as_ref()
                .clone(),
        )
        .unwrap(),
    ));

    let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
        .unwrap()
        .aggregate(
            Vec::<Expr>::new(),
            vec![datafusion::functions_aggregate::first_last::last_value(
                col("ts"),
                vec![],
            )],
        )
        .unwrap()
        .build()
        .unwrap();

    let LogicalPlan::Aggregate(aggr_plan) = plan.clone() else {
        panic!("expect Aggregate plan");
    };
    let split = StateMergeHelper::split_aggr_node(aggr_plan).unwrap();

    try_encode_decode_substrait(&split.lower_state, ctx.state());

    let config = ConfigOptions::default();
    let result = DistPlannerAnalyzer {}
        .analyze(plan.clone(), &config)
        .unwrap();

    let expected = [
        "Projection: last_value(t.ts)",
        "  Aggregate: groupBy=[[]], aggr=[[__last_value_merge(__last_value_state(t.ts)) AS last_value(t.ts)]]",
        "    MergeScan [is_placeholder=false, remote_input=[",
        "Aggregate: groupBy=[[]], aggr=[[__last_value_state(t.ts)]]",
        "  TableScan: t",
        "]]",
    ]
    .join("\n");
    assert_eq!(expected, result.to_string());
}
