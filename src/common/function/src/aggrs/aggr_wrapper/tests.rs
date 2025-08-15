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
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, Float64Array, Int64Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use datafusion_common::{Column, TableReference};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::sqlparser::ast::NullTreatment;
use datafusion_expr::{lit, Aggregate, Expr, LogicalPlan, SortExpr, TableScan};
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datatypes::arrow_array::StringArray;
use futures::{Stream, StreamExt as _};
use pretty_assertions::assert_eq;

use super::*;
use crate::aggrs::approximate::hll::HllState;
use crate::aggrs::approximate::uddsketch::UddSketchState;
use crate::aggrs::count_hash::CountHash;
use crate::function::Function as _;
use crate::scalars::hll_count::HllCalcFunction;
use crate::scalars::uddsketch_calc::UddSketchCalcFunction;

#[derive(Debug)]
pub struct MockInputExec {
    input: Vec<RecordBatch>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl MockInputExec {
    pub fn new(input: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self {
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            input,
            schema,
        }
    }
}

impl DisplayAs for MockInputExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        unimplemented!()
    }
}

impl ExecutionPlan for MockInputExec {
    fn name(&self) -> &str {
        "MockInputExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = MockStream {
            stream: self.input.clone(),
            schema: self.schema.clone(),
            idx: 0,
        };
        Ok(Box::pin(stream))
    }
}

struct MockStream {
    stream: Vec<RecordBatch>,
    schema: SchemaRef,
    idx: usize,
}

impl Stream for MockStream {
    type Item = datafusion_common::Result<RecordBatch>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<RecordBatch>>> {
        if self.idx < self.stream.len() {
            let ret = self.stream[self.idx].clone();
            self.idx += 1;
            Poll::Ready(Some(Ok(ret)))
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for MockStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
struct DummyTableProvider {
    schema: Arc<arrow_schema::Schema>,
    record_batch: Mutex<Option<RecordBatch>>,
}

impl DummyTableProvider {
    #[allow(unused)]
    pub fn new(schema: Arc<arrow_schema::Schema>, record_batch: Option<RecordBatch>) -> Self {
        Self {
            schema,
            record_batch: Mutex::new(record_batch),
        }
    }
}

impl Default for DummyTableProvider {
    fn default() -> Self {
        Self {
            schema: Arc::new(arrow_schema::Schema::new(vec![Field::new(
                "number",
                DataType::Int64,
                true,
            )])),
            record_batch: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for DummyTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion_expr::TableType {
        datafusion_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let input: Vec<RecordBatch> = self
            .record_batch
            .lock()
            .unwrap()
            .clone()
            .map(|r| vec![r])
            .unwrap_or_default();
        Ok(Arc::new(MockInputExec::new(input, self.schema.clone())))
    }
}

fn dummy_table_scan() -> LogicalPlan {
    let table_provider = Arc::new(DummyTableProvider::default());
    let table_source = DefaultTableSource::new(table_provider);
    LogicalPlan::TableScan(
        TableScan::try_new(
            TableReference::bare("Number"),
            Arc::new(table_source),
            None,
            vec![],
            None,
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn test_sum_udaf() {
    let ctx = SessionContext::new();

    let sum = datafusion::functions_aggregate::sum::sum_udaf();
    let sum = (*sum).clone();
    let original_aggr = Aggregate::try_new(
        Arc::new(dummy_table_scan()),
        vec![],
        vec![Expr::AggregateFunction(AggregateFunction::new_udf(
            Arc::new(sum.clone()),
            vec![Expr::Column(Column::new_unqualified("number"))],
            false,
            None,
            vec![],
            None,
        ))],
    )
    .unwrap();
    let res = StateMergeHelper::split_aggr_node(original_aggr).unwrap();

    let expected_lower_plan = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(dummy_table_scan()),
            vec![],
            vec![Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(StateWrapper::new(sum.clone()).unwrap().into()),
                vec![Expr::Column(Column::new_unqualified("number"))],
                false,
                None,
                vec![],
                None,
            ))],
        )
        .unwrap(),
    )
    .recompute_schema()
    .unwrap();
    assert_eq!(&res.lower_state, &expected_lower_plan);

    let expected_merge_plan = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(expected_lower_plan),
            vec![],
            vec![Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(
                    MergeWrapper::new(
                        sum.clone(),
                        Arc::new(
                            AggregateExprBuilder::new(
                                Arc::new(sum.clone()),
                                vec![Arc::new(
                                    datafusion::physical_expr::expressions::Column::new(
                                        "number", 0,
                                    ),
                                )],
                            )
                            .schema(Arc::new(dummy_table_scan().schema().as_arrow().clone()))
                            .alias("sum(number)")
                            .build()
                            .unwrap(),
                        ),
                        vec![DataType::Int64],
                    )
                    .unwrap()
                    .into(),
                ),
                vec![Expr::Column(Column::new_unqualified("__sum_state(number)"))],
                false,
                None,
                vec![],
                None,
            ))
            .alias("sum(number)")],
        )
        .unwrap(),
    );
    assert_eq!(&res.upper_merge, &expected_merge_plan);

    let phy_aggr_state_plan = DefaultPhysicalPlanner::default()
        .create_physical_plan(&res.lower_state, &ctx.state())
        .await
        .unwrap();
    let aggr_exec = phy_aggr_state_plan
        .as_any()
        .downcast_ref::<AggregateExec>()
        .unwrap();
    let aggr_func_expr = &aggr_exec.aggr_expr()[0];
    let mut state_accum = aggr_func_expr.create_accumulator().unwrap();

    // evaluate the state function
    let input = Int64Array::from(vec![Some(1), Some(2), None, Some(3)]);
    let values = vec![Arc::new(input) as arrow::array::ArrayRef];

    state_accum.update_batch(&values).unwrap();
    let state = state_accum.state().unwrap();
    assert_eq!(state.len(), 1);
    assert_eq!(state[0], ScalarValue::Int64(Some(6)));

    let eval_res = state_accum.evaluate().unwrap();
    assert_eq!(
        eval_res,
        ScalarValue::Struct(Arc::new(
            StructArray::try_new(
                vec![Field::new("sum[sum]", DataType::Int64, true)].into(),
                vec![Arc::new(Int64Array::from(vec![Some(6)]))],
                None,
            )
            .unwrap(),
        ))
    );

    let phy_aggr_merge_plan = DefaultPhysicalPlanner::default()
        .create_physical_plan(&res.upper_merge, &ctx.state())
        .await
        .unwrap();
    let aggr_exec = phy_aggr_merge_plan
        .as_any()
        .downcast_ref::<AggregateExec>()
        .unwrap();
    let aggr_func_expr = &aggr_exec.aggr_expr()[0];
    let mut merge_accum = aggr_func_expr.create_accumulator().unwrap();

    let merge_input =
        vec![Arc::new(Int64Array::from(vec![Some(6), Some(42), None])) as arrow::array::ArrayRef];
    let merge_input_struct_arr = StructArray::try_new(
        vec![Field::new("sum[sum]", DataType::Int64, true)].into(),
        merge_input,
        None,
    )
    .unwrap();

    merge_accum
        .update_batch(&[Arc::new(merge_input_struct_arr)])
        .unwrap();
    let merge_state = merge_accum.state().unwrap();
    assert_eq!(merge_state.len(), 1);
    assert_eq!(merge_state[0], ScalarValue::Int64(Some(48)));

    let merge_eval_res = merge_accum.evaluate().unwrap();
    assert_eq!(merge_eval_res, ScalarValue::Int64(Some(48)));
}

#[tokio::test]
async fn test_avg_udaf() {
    let ctx = SessionContext::new();

    let avg = datafusion::functions_aggregate::average::avg_udaf();
    let avg = (*avg).clone();

    let original_aggr = Aggregate::try_new(
        Arc::new(dummy_table_scan()),
        vec![],
        vec![Expr::AggregateFunction(AggregateFunction::new_udf(
            Arc::new(avg.clone()),
            vec![Expr::Column(Column::new_unqualified("number"))],
            false,
            None,
            vec![],
            None,
        ))],
    )
    .unwrap();
    let res = StateMergeHelper::split_aggr_node(original_aggr).unwrap();

    let state_func: Arc<AggregateUDF> = Arc::new(StateWrapper::new(avg.clone()).unwrap().into());
    let expected_aggr_state_plan = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(dummy_table_scan()),
            vec![],
            vec![Expr::AggregateFunction(AggregateFunction::new_udf(
                state_func,
                vec![Expr::Column(Column::new_unqualified("number"))],
                false,
                None,
                vec![],
                None,
            ))],
        )
        .unwrap(),
    );
    // type coerced so avg aggr function can function correctly
    let coerced_aggr_state_plan = TypeCoercion::new()
        .analyze(expected_aggr_state_plan.clone(), &Default::default())
        .unwrap();
    assert_eq!(&res.lower_state, &coerced_aggr_state_plan);
    assert_eq!(
        res.lower_state.schema().as_arrow(),
        &arrow_schema::Schema::new(vec![Field::new(
            "__avg_state(number)",
            DataType::Struct(
                vec![
                    Field::new("avg[count]", DataType::UInt64, true),
                    Field::new("avg[sum]", DataType::Float64, true)
                ]
                .into()
            ),
            true,
        )])
    );

    let expected_merge_fn = MergeWrapper::new(
        avg.clone(),
        Arc::new(
            AggregateExprBuilder::new(
                Arc::new(avg.clone()),
                vec![Arc::new(
                    datafusion::physical_expr::expressions::Column::new("number", 0),
                )],
            )
            .schema(Arc::new(dummy_table_scan().schema().as_arrow().clone()))
            .alias("avg(number)")
            .build()
            .unwrap(),
        ),
        // coerced to float64
        vec![DataType::Float64],
    )
    .unwrap();

    let expected_merge_plan = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(coerced_aggr_state_plan.clone()),
            vec![],
            vec![Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(expected_merge_fn.into()),
                vec![Expr::Column(Column::new_unqualified("__avg_state(number)"))],
                false,
                None,
                vec![],
                None,
            ))
            .alias("avg(number)")],
        )
        .unwrap(),
    );
    assert_eq!(&res.upper_merge, &expected_merge_plan);

    let phy_aggr_state_plan = DefaultPhysicalPlanner::default()
        .create_physical_plan(&coerced_aggr_state_plan, &ctx.state())
        .await
        .unwrap();
    let aggr_exec = phy_aggr_state_plan
        .as_any()
        .downcast_ref::<AggregateExec>()
        .unwrap();
    let aggr_func_expr = &aggr_exec.aggr_expr()[0];
    let mut state_accum = aggr_func_expr.create_accumulator().unwrap();

    // evaluate the state function
    let input = Float64Array::from(vec![Some(1.), Some(2.), None, Some(3.)]);
    let values = vec![Arc::new(input) as arrow::array::ArrayRef];

    state_accum.update_batch(&values).unwrap();
    let state = state_accum.state().unwrap();
    assert_eq!(state.len(), 2);
    assert_eq!(state[0], ScalarValue::UInt64(Some(3)));
    assert_eq!(state[1], ScalarValue::Float64(Some(6.)));

    let eval_res = state_accum.evaluate().unwrap();
    let expected = Arc::new(
        StructArray::try_new(
            vec![
                Field::new("avg[count]", DataType::UInt64, true),
                Field::new("avg[sum]", DataType::Float64, true),
            ]
            .into(),
            vec![
                Arc::new(UInt64Array::from(vec![Some(3)])),
                Arc::new(Float64Array::from(vec![Some(6.)])),
            ],
            None,
        )
        .unwrap(),
    );
    assert_eq!(eval_res, ScalarValue::Struct(expected));

    let phy_aggr_merge_plan = DefaultPhysicalPlanner::default()
        .create_physical_plan(&res.upper_merge, &ctx.state())
        .await
        .unwrap();
    let aggr_exec = phy_aggr_merge_plan
        .as_any()
        .downcast_ref::<AggregateExec>()
        .unwrap();
    let aggr_func_expr = &aggr_exec.aggr_expr()[0];

    let mut merge_accum = aggr_func_expr.create_accumulator().unwrap();

    let merge_input = vec![
        Arc::new(UInt64Array::from(vec![Some(3), Some(42), None])) as arrow::array::ArrayRef,
        Arc::new(Float64Array::from(vec![Some(48.), Some(84.), None])),
    ];
    let merge_input_struct_arr = StructArray::try_new(
        vec![
            Field::new("avg[count]", DataType::UInt64, true),
            Field::new("avg[sum]", DataType::Float64, true),
        ]
        .into(),
        merge_input,
        None,
    )
    .unwrap();

    merge_accum
        .update_batch(&[Arc::new(merge_input_struct_arr)])
        .unwrap();
    let merge_state = merge_accum.state().unwrap();
    assert_eq!(merge_state.len(), 2);
    assert_eq!(merge_state[0], ScalarValue::UInt64(Some(45)));
    assert_eq!(merge_state[1], ScalarValue::Float64(Some(132.)));

    let merge_eval_res = merge_accum.evaluate().unwrap();
    // the merge function returns the average, which is 132 / 45
    assert_eq!(merge_eval_res, ScalarValue::Float64(Some(132. / 45_f64)));
}

/// For testing whether the UDAF state fields are correctly implemented.
/// esp. for our own custom UDAF's state fields.
/// By compare eval results before and after split to state/merge functions.
#[tokio::test]
async fn test_udaf_correct_eval_result() {
    struct TestCase {
        func: Arc<AggregateUDF>,
        args: Vec<Expr>,
        input_schema: SchemaRef,
        input: Vec<ArrayRef>,
        expected_output: Option<ScalarValue>,
        expected_fn: Option<ExpectedFn>,
        distinct: bool,
        filter: Option<Box<Expr>>,
        order_by: Vec<SortExpr>,
        null_treatment: Option<NullTreatment>,
    }
    type ExpectedFn = fn(ArrayRef) -> bool;

    let test_cases = vec![
        TestCase {
            func: sum_udaf(),
            input_schema: Arc::new(arrow_schema::Schema::new(vec![Field::new(
                "number",
                DataType::Int64,
                true,
            )])),
            args: vec![Expr::Column(Column::new_unqualified("number"))],
            input: vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                None,
                Some(3),
            ]))],
            expected_output: Some(ScalarValue::Int64(Some(6))),
            expected_fn: None,
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
        TestCase {
            func: avg_udaf(),
            input_schema: Arc::new(arrow_schema::Schema::new(vec![Field::new(
                "number",
                DataType::Int64,
                true,
            )])),
            args: vec![Expr::Column(Column::new_unqualified("number"))],
            input: vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                None,
                Some(3),
            ]))],
            expected_output: Some(ScalarValue::Float64(Some(2.0))),
            expected_fn: None,
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
        TestCase {
            func: Arc::new(CountHash::udf_impl()),
            input_schema: Arc::new(arrow_schema::Schema::new(vec![Field::new(
                "number",
                DataType::Int64,
                true,
            )])),
            args: vec![Expr::Column(Column::new_unqualified("number"))],
            input: vec![Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                None,
                Some(3),
                Some(3),
                Some(3),
            ]))],
            expected_output: Some(ScalarValue::Int64(Some(4))),
            expected_fn: None,
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
        TestCase {
            func: Arc::new(UddSketchState::state_udf_impl()),
            input_schema: Arc::new(arrow_schema::Schema::new(vec![Field::new(
                "number",
                DataType::Float64,
                true,
            )])),
            args: vec![
                lit(128i64),
                lit(0.05f64),
                Expr::Column(Column::new_unqualified("number")),
            ],
            input: vec![Arc::new(Float64Array::from(vec![
                Some(1.),
                Some(2.),
                None,
                Some(3.),
                Some(3.),
                Some(3.),
            ]))],
            expected_output: None,
            expected_fn: Some(|arr| {
                let percent = ScalarValue::Float64(Some(0.5)).to_array().unwrap();
                let percent = datatypes::vectors::Helper::try_into_vector(percent).unwrap();
                let state = datatypes::vectors::Helper::try_into_vector(arr).unwrap();
                let udd_calc = UddSketchCalcFunction;
                let res = udd_calc
                    .eval(&Default::default(), &[percent, state])
                    .unwrap();
                let binding = res.to_arrow_array();
                let res_arr = binding.as_any().downcast_ref::<Float64Array>().unwrap();
                assert!(res_arr.len() == 1);
                assert!((res_arr.value(0) - 2.856578984907706f64).abs() <= f64::EPSILON);
                true
            }),
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
        TestCase {
            func: Arc::new(HllState::state_udf_impl()),
            input_schema: Arc::new(arrow_schema::Schema::new(vec![Field::new(
                "word",
                DataType::Utf8,
                true,
            )])),
            args: vec![Expr::Column(Column::new_unqualified("word"))],
            input: vec![Arc::new(StringArray::from(vec![
                Some("foo"),
                Some("bar"),
                None,
                Some("baz"),
                Some("baz"),
            ]))],
            expected_output: None,
            expected_fn: Some(|arr| {
                let state = datatypes::vectors::Helper::try_into_vector(arr).unwrap();
                let hll_calc = HllCalcFunction;
                let res = hll_calc.eval(&Default::default(), &[state]).unwrap();
                let binding = res.to_arrow_array();
                let res_arr = binding.as_any().downcast_ref::<UInt64Array>().unwrap();
                assert!(res_arr.len() == 1);
                assert_eq!(res_arr.value(0), 3);
                true
            }),
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
        // TODO(discord9): udd_merge/hll_merge/geo_path/quantile_aggr tests
    ];
    let test_table_ref = TableReference::bare("TestTable");

    for case in test_cases {
        let ctx = SessionContext::new();
        let table_provider = DummyTableProvider::new(
            case.input_schema.clone(),
            Some(RecordBatch::try_new(case.input_schema.clone(), case.input.clone()).unwrap()),
        );
        let table_source = DefaultTableSource::new(Arc::new(table_provider));
        let logical_plan = LogicalPlan::TableScan(
            TableScan::try_new(
                test_table_ref.clone(),
                Arc::new(table_source),
                None,
                vec![],
                None,
            )
            .unwrap(),
        );

        let args = case.args;

        let aggr_expr = Expr::AggregateFunction(AggregateFunction::new_udf(
            case.func.clone(),
            args,
            case.distinct,
            case.filter,
            case.order_by,
            case.null_treatment,
        ));

        let aggr_plan = LogicalPlan::Aggregate(
            Aggregate::try_new(Arc::new(logical_plan), vec![], vec![aggr_expr]).unwrap(),
        );

        // make sure the aggr_plan is type coerced
        let aggr_plan = TypeCoercion::new()
            .analyze(aggr_plan, &Default::default())
            .unwrap();

        // first eval the original aggregate function
        let phy_full_aggr_plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&aggr_plan, &ctx.state())
            .await
            .unwrap();

        {
            let unsplit_result = execute_phy_plan(&phy_full_aggr_plan).await.unwrap();
            assert_eq!(unsplit_result.len(), 1);
            let unsplit_batch = &unsplit_result[0];
            assert_eq!(unsplit_batch.num_columns(), 1);
            assert_eq!(unsplit_batch.num_rows(), 1);
            let unsplit_col = unsplit_batch.column(0);
            if let Some(expected_output) = &case.expected_output {
                assert_eq!(unsplit_col.data_type(), &expected_output.data_type());
                assert_eq!(unsplit_col.len(), 1);
                assert_eq!(unsplit_col, &expected_output.to_array().unwrap());
            }

            if let Some(expected_fn) = &case.expected_fn {
                assert!(expected_fn(unsplit_col.clone()));
            }
        }
        let LogicalPlan::Aggregate(aggr_plan) = aggr_plan else {
            panic!("Expected Aggregate plan");
        };
        let split_plan = StateMergeHelper::split_aggr_node(aggr_plan).unwrap();

        let phy_upper_plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(&split_plan.upper_merge, &ctx.state())
            .await
            .unwrap();

        // since upper plan use lower plan as input, execute upper plan should also execute lower plan
        // which should give the same result as the original aggregate function
        {
            let split_res = execute_phy_plan(&phy_upper_plan).await.unwrap();

            assert_eq!(split_res.len(), 1);
            let split_batch = &split_res[0];
            assert_eq!(split_batch.num_columns(), 1);
            assert_eq!(split_batch.num_rows(), 1);
            let split_col = split_batch.column(0);
            if let Some(expected_output) = &case.expected_output {
                assert_eq!(split_col.data_type(), &expected_output.data_type());
                assert_eq!(split_col.len(), 1);
                assert_eq!(split_col, &expected_output.to_array().unwrap());
            }

            if let Some(expected_fn) = &case.expected_fn {
                assert!(expected_fn(split_col.clone()));
            }
        }
    }
}

async fn execute_phy_plan(
    phy_plan: &Arc<dyn ExecutionPlan>,
) -> datafusion_common::Result<Vec<RecordBatch>> {
    let task_ctx = Arc::new(TaskContext::default());
    let mut stream = phy_plan.execute(0, task_ctx)?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}
