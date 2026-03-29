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

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};
use datafusion_common::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;

/// Removes repartitions that only strengthen an existing `__tsid` distribution into
/// `(__tsid, time)` for partitioned hash joins.
#[derive(Debug)]
pub struct TsidJoinRepartition;

impl PhysicalOptimizerRule for TsidJoinRepartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };

            if *hash_join.partition_mode() != PartitionMode::Partitioned {
                return Ok(Transformed::no(plan));
            }

            let left_join_keys = join_side_column_names(hash_join.on(), true);
            let right_join_keys = join_side_column_names(hash_join.on(), false);
            let Some(left_join_keys) = left_join_keys else {
                return Ok(Transformed::no(plan));
            };
            let Some(right_join_keys) = right_join_keys else {
                return Ok(Transformed::no(plan));
            };

            let Some(left_repartition) =
                hash_join.left().as_any().downcast_ref::<RepartitionExec>()
            else {
                return Ok(Transformed::no(plan));
            };
            let Some(right_repartition) =
                hash_join.right().as_any().downcast_ref::<RepartitionExec>()
            else {
                return Ok(Transformed::no(plan));
            };

            let Some(left_input) = removable_tsid_repartition(left_repartition, &left_join_keys)
            else {
                return Ok(Transformed::no(plan));
            };
            let Some(right_input) = removable_tsid_repartition(right_repartition, &right_join_keys)
            else {
                return Ok(Transformed::no(plan));
            };

            if left_input.output_partitioning().partition_count()
                != right_input.output_partitioning().partition_count()
            {
                return Ok(Transformed::no(plan));
            }

            let new_join = hash_join
                .builder()
                .with_new_children(vec![left_input, right_input])?
                .recompute_properties()
                .reset_state()
                .build_exec()?;

            Ok(Transformed::yes(new_join))
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "TsidJoinRepartition"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

fn removable_tsid_repartition(
    repartition: &RepartitionExec,
    join_keys: &BTreeSet<String>,
) -> Option<Arc<dyn ExecutionPlan>> {
    if repartition.preserve_order() {
        return None;
    }

    let Partitioning::Hash(requested_exprs, requested_partition_count) = repartition.partitioning()
    else {
        return None;
    };
    let Partitioning::Hash(existing_exprs, existing_partition_count) =
        repartition.input().output_partitioning()
    else {
        return None;
    };

    if *requested_partition_count != *existing_partition_count {
        return None;
    }

    let requested_names = column_names(requested_exprs)?;
    if requested_names != *join_keys || !requested_names.contains(DATA_SCHEMA_TSID_COLUMN_NAME) {
        return None;
    }

    let existing_names = column_names(&existing_exprs)?;
    if existing_names != BTreeSet::from([DATA_SCHEMA_TSID_COLUMN_NAME.to_string()]) {
        return None;
    }

    if requested_names.len() <= existing_names.len() {
        return None;
    }

    Some(repartition.input().clone())
}

fn join_side_column_names(
    on: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
    left: bool,
) -> Option<BTreeSet<String>> {
    on.iter()
        .map(|(left_expr, right_expr)| {
            if left {
                physical_column_name(left_expr)
            } else {
                physical_column_name(right_expr)
            }
        })
        .collect()
}

fn column_names(exprs: &[Arc<dyn PhysicalExpr>]) -> Option<BTreeSet<String>> {
    exprs.iter().map(physical_column_name).collect()
}

fn physical_column_name(expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
    expr.as_any()
        .downcast_ref::<PhysicalColumn>()
        .map(|column| column.name().to_string())
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use datafusion::common::NullEquality;
    use datafusion::execution::TaskContext;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, Partitioning, PlanProperties, SendableRecordBatchStream,
        displayable,
    };
    use datafusion_common::Result as DfResult;
    use datafusion_expr::JoinType;
    use datafusion_physical_expr::EquivalenceProperties;
    use datafusion_physical_expr::expressions::Column as PhysicalColumn;

    use super::*;

    #[derive(Debug)]
    struct PartitionedTestExec {
        properties: Arc<PlanProperties>,
    }

    impl PartitionedTestExec {
        fn new(schema: SchemaRef, partitioning: Partitioning) -> Self {
            Self {
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema.clone()),
                    partitioning,
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
            }
        }
    }

    impl DisplayAs for PartitionedTestExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "PartitionedTestExec")
        }
    }

    impl ExecutionPlan for PartitionedTestExec {
        fn name(&self) -> &str {
            "PartitionedTestExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DfResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DfResult<SendableRecordBatchStream> {
            unreachable!("optimizer tests should not execute PartitionedTestExec")
        }
    }

    #[test]
    fn removes_repartition_for_tsid_strengthening_join() {
        let schema = test_schema();
        let left = repartitioned_child(
            schema.clone(),
            vec![partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 0)],
        );
        let right = repartitioned_child(
            schema.clone(),
            vec![partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 0)],
        );
        let join_on = vec![
            (
                partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 0),
                partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 0),
            ),
            (
                partition_column("greptime_timestamp", 1),
                partition_column("greptime_timestamp", 1),
            ),
        ];
        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                join_on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized = TsidJoinRepartition
            .optimize(join, &ConfigOptions::default())
            .unwrap();

        let hash_join = optimized.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert!(!hash_join.left().as_any().is::<RepartitionExec>());
        assert!(!hash_join.right().as_any().is::<RepartitionExec>());

        let plan_str = displayable(optimized.as_ref()).indent(false).to_string();
        assert!(!plan_str.contains("RepartitionExec"), "{plan_str}");
    }

    #[test]
    fn keeps_repartition_without_existing_tsid_distribution() {
        let schema = test_schema();
        let left = repartitioned_child(
            schema.clone(),
            vec![partition_column("greptime_timestamp", 1)],
        );
        let right = repartitioned_child(
            schema.clone(),
            vec![partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 0)],
        );
        let join_on = vec![
            (
                partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 0),
                partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 0),
            ),
            (
                partition_column("greptime_timestamp", 1),
                partition_column("greptime_timestamp", 1),
            ),
        ];
        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                join_on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized = TsidJoinRepartition
            .optimize(join, &ConfigOptions::default())
            .unwrap();

        let hash_join = optimized.as_any().downcast_ref::<HashJoinExec>().unwrap();
        assert!(hash_join.left().as_any().is::<RepartitionExec>());
        assert!(hash_join.right().as_any().is::<RepartitionExec>());
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", DataType::Float64, true),
        ]))
    }

    fn repartitioned_child(
        schema: SchemaRef,
        existing_partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        let input = Arc::new(PartitionedTestExec::new(
            schema,
            Partitioning::Hash(existing_partition_exprs, 32),
        ));
        Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(
                    vec![
                        partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 0),
                        partition_column("greptime_timestamp", 1),
                    ],
                    32,
                ),
            )
            .unwrap(),
        )
    }

    fn partition_column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(PhysicalColumn::new(name, index))
    }
}
