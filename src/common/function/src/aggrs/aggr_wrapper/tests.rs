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
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Float64Array, Int64Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use datafusion_common::{Column, DFSchema, TableReference};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{Aggregate, Expr, LogicalPlan, Signature, TableScan};
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use futures::Stream;
use pretty_assertions::assert_eq;

use super::*;
use crate::aggrs::aggr_wrapper::type_lookup::get_possible_state_to_input_types;

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
}

impl DummyTableProvider {
    pub fn new(schema: Arc<arrow_schema::Schema>) -> Self {
        Self { schema }
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
        Ok(Arc::new(MockInputExec::new(vec![], self.schema.clone())))
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

fn dummy_table_scan_with_schema(
    schema: Arc<DFSchema>,
    table_name: impl Into<TableReference>,
) -> LogicalPlan {
    let table_provider = Arc::new(DummyTableProvider::new(schema.inner().clone()));
    let table_source = DefaultTableSource::new(table_provider);
    LogicalPlan::TableScan(
        TableScan::try_new(table_name, Arc::new(table_source), None, vec![], None).unwrap(),
    )
}

fn dummy_aggr_state_plan(state_udf: Arc<AggregateUDF>) -> LogicalPlan {
    let table_scan = dummy_table_scan();
    let aggr_func = AggregateFunction::new_udf(
        state_udf,
        vec![Expr::Column(Column::new_unqualified("number"))],
        false,
        None,
        None,
        None,
    );
    LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(table_scan),
            vec![],
            vec![Expr::AggregateFunction(aggr_func)],
        )
        .unwrap(),
    )
}

fn dummy_aggr_merge_plan(input_schema: Arc<DFSchema>, func: Arc<AggregateUDF>) -> LogicalPlan {
    let table = DummyTableProvider::new(input_schema.inner().clone());
    let table_source = DefaultTableSource::new(Arc::new(table));
    let table_scan = LogicalPlan::TableScan(
        TableScan::try_new(
            TableReference::bare("MergeState"),
            Arc::new(table_source),
            None,
            vec![],
            None,
        )
        .unwrap(),
    );
    let col_name = input_schema.field(0).name().clone();
    let column = Expr::Column(Column::new_unqualified(&col_name));
    let aggr_func = AggregateFunction::new_udf(func, vec![column], false, None, None, None);
    LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(table_scan),
            vec![],
            vec![Expr::AggregateFunction(aggr_func)],
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
            None,
            None,
        ))],
    )
    .unwrap();
    let res = StateMergeHelper::split_aggr_node(original_aggr).unwrap();

    println!("upper: {}\nlower: {}", res.upper, res.lower);
    let expected_lower_plan = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(dummy_table_scan()),
            vec![],
            vec![Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(StateWrapper::new(sum.clone()).unwrap().into()),
                vec![Expr::Column(Column::new_unqualified("number"))],
                false,
                None,
                None,
                None,
            ))],
        )
        .unwrap(),
    )
    .recompute_schema()
    .unwrap();
    assert_eq!(res.lower.as_ref(), &expected_lower_plan);

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
                None,
                None,
            ))
            .alias("sum(number)")],
        )
        .unwrap(),
    );
    assert_eq!(res.upper.as_ref(), &expected_merge_plan);

    let phy_aggr_state_plan = DefaultPhysicalPlanner::default()
        .create_physical_plan(&res.lower, &ctx.state())
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
                vec![Field::new("col_0", DataType::Int64, true)].into(),
                vec![Arc::new(Int64Array::from(vec![Some(6)]))],
                None,
            )
            .unwrap(),
        ))
    );

    let phy_aggr_merge_plan = DefaultPhysicalPlanner::default()
        .create_physical_plan(&res.upper, &ctx.state())
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
        vec![Field::new("state[0]", DataType::Int64, true)].into(),
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
            None,
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
                None,
                None,
            ))],
        )
        .unwrap(),
    );
    // type coerced so avg aggr function can function correctly
    let coerced_aggr_state_plan = TypeCoercion::new()
        .analyze(expected_aggr_state_plan.clone(), &Default::default())
        .unwrap();
    assert_eq!(res.lower.as_ref(), &coerced_aggr_state_plan);
    assert_eq!(
        res.lower.schema().as_arrow(),
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

    let state_schema = res.lower.schema().clone();

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
                None,
                None,
            ))
            .alias("avg(number)")],
        )
        .unwrap(),
    );
    assert_eq!(res.upper.as_ref(), &expected_merge_plan);

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
                Field::new("col_0", DataType::UInt64, true),
                Field::new("col_1", DataType::Float64, true),
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
        .create_physical_plan(&res.upper, &ctx.state())
        .await
        .inspect_err(|e| println!("Error creating physical plan: {}", e))
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
            Field::new("state[0]", DataType::UInt64, true),
            Field::new("state[1]", DataType::Float64, true),
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

    /*
    let ctx = SessionContext::new();

    let avg = datafusion::functions_aggregate::average::avg_udaf();
    let avg = (*avg).clone();

    let avg_type = get_possible_state_to_input_types(&avg).unwrap();
    let expected_avg_type = StateTypeLookup {
        mapping: {
            use DataType::*;
            BTreeMap::from([
                (vec![UInt64, Int8], vec![Int8]),
                (vec![UInt64, Int16], vec![Int16]),
                (vec![UInt64, Int32], vec![Int32]),
                (vec![UInt64, Int64], vec![Int64]),
                (vec![UInt64, UInt8], vec![UInt8]),
                (vec![UInt64, UInt16], vec![UInt16]),
                (vec![UInt64, UInt32], vec![UInt32]),
                (vec![UInt64, UInt64], vec![UInt64]),
                (vec![UInt64, Float32], vec![Float32]),
                (vec![UInt64, Float64], vec![Float64]),
                (vec![UInt64, Decimal128(38, 18)], vec![Decimal128(38, 18)]),
                (vec![UInt64, Decimal256(76, 38)], vec![Decimal256(76, 38)]),
            ])
        },
    };
    assert_eq!(avg_type, expected_avg_type);

    let wrapper = StateMergeHelper::new_with_lookup(avg.clone(), avg_type.clone()).unwrap();

    let expected = StateMergeHelper {
        original: avg.clone(),
        state_function: StateWrapper {
            inner: avg.clone(),
            name: "__avg_state".to_string(),
        },
        merge_function: MergeWrapper {
            inner: avg.clone(),
            name: "__avg_merge".to_string(),
            merge_signature: Signature::user_defined(datafusion_expr::Volatility::Immutable),
            state_to_input_types: avg_type.clone(),
        },
    };
    assert_eq!(wrapper, expected);

    // create the child logical plan for the state function, which usually should be running on datanode
    let aggr_state_plan = dummy_aggr_state_plan(Arc::new(wrapper.state_function().clone().into()));
    let expected_plan = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(dummy_table_scan()),
            vec![],
            vec![Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(wrapper.state_function().clone().into()),
                vec![Expr::Column(Column::new_unqualified("number"))],
                false,
                None,
                None,
                None,
            ))],
        )
        .unwrap(),
    );
    assert_eq!(aggr_state_plan, expected_plan);
    assert_eq!(
        aggr_state_plan.schema().as_arrow(),
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

    // the state schema should be the input schema of the merge function
    let state_schema = aggr_state_plan.schema().clone();

    let expected_merge_plan = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(dummy_table_scan_with_schema(
                state_schema.clone(),
                TableReference::bare("MergeState"),
            )),
            vec![],
            vec![Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(wrapper.merge_function().clone().into()),
                vec![Expr::Column(Column::new_unqualified("__avg_state(number)"))],
                false,
                None,
                None,
                None,
            ))],
        )
        .unwrap(),
    );

    let aggr_merge_plan = dummy_aggr_merge_plan(
        state_schema,
        Arc::new(wrapper.merge_function().clone().into()),
    );
    assert_eq!(aggr_merge_plan, expected_merge_plan);

    // evaluate the state function
    let input = Float64Array::from(vec![Some(1.), Some(2.), None, Some(3.)]);
    let values = vec![Arc::new(input) as arrow::array::ArrayRef];

    // type coerced so avg aggr function can function correctly
    let coerced_aggr_state_plan = TypeCoercion::new()
        .analyze(aggr_state_plan, &Default::default())
        .unwrap();

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

    state_accum.update_batch(&values).unwrap();
    let state = state_accum.state().unwrap();
    assert_eq!(state.len(), 2);
    assert_eq!(state[0], ScalarValue::UInt64(Some(3)));
    assert_eq!(state[1], ScalarValue::Float64(Some(6.)));

    let eval_res = state_accum.evaluate().unwrap();
    let expected = Arc::new(
        StructArray::try_new(
            vec![
                Field::new("col_0", DataType::UInt64, true),
                Field::new("col_1", DataType::Float64, true),
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

    let merge_input = vec![
        Arc::new(UInt64Array::from(vec![Some(3), Some(42), None])) as arrow::array::ArrayRef,
        Arc::new(Float64Array::from(vec![Some(48.), Some(84.), None])),
    ];
    let merge_input_struct_arr = StructArray::try_new(
        vec![
            Field::new("state[0]", DataType::UInt64, true),
            Field::new("state[1]", DataType::Float64, true),
        ]
        .into(),
        merge_input,
        None,
    )
    .unwrap();

    let phy_aggr_merge_plan = DefaultPhysicalPlanner::default()
        .create_physical_plan(&aggr_merge_plan, &ctx.state())
        .await
        .unwrap();
    let aggr_exec = phy_aggr_merge_plan
        .as_any()
        .downcast_ref::<AggregateExec>()
        .unwrap();
    let aggr_func_expr = &aggr_exec.aggr_expr()[0];
    let mut merge_accum = aggr_func_expr.create_accumulator().unwrap();

    merge_accum
        .update_batch(&[Arc::new(merge_input_struct_arr)])
        .unwrap();
    let merge_state = merge_accum.state().unwrap();
    assert_eq!(merge_state.len(), 2);
    assert_eq!(merge_state[0], ScalarValue::UInt64(Some(45)));
    assert_eq!(merge_state[1], ScalarValue::Float64(Some(132.)));

    let merge_eval_res = merge_accum.evaluate().unwrap();
    // the merge function returns the average, which is 132 / 45
    assert_eq!(merge_eval_res, ScalarValue::Float64(Some(132. / 45_f64)));*/
}
