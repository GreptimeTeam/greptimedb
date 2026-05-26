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

use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_common::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::Column;
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;

/// Chooses a broadcast-style hash join for the PromQL vector-vector shape where
/// the build side only carries value, `__tsid`, and timestamp columns.
///
/// PromQL arithmetic joins often keep one side narrow and the other side wide
/// with all output labels. Partitioning both sides shuffles the wide stream.
/// `CollectLeft` only gathers the narrow build side and lets the wide probe side
/// keep its existing partitioning.
#[derive(Debug)]
pub struct PromqlJoinSelection;

impl PhysicalOptimizerRule for PromqlJoinSelection {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(Self::rewrite_join).data()
    }

    fn name(&self) -> &str {
        "PromqlJoinSelection"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl PromqlJoinSelection {
    fn rewrite_join(plan: Arc<dyn ExecutionPlan>) -> DfResult<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(plan));
        };

        if !Self::should_collect_left(hash_join) {
            return Ok(Transformed::no(plan));
        }

        let projection = hash_join
            .contains_projection()
            .then(|| Self::infer_projection(hash_join.join_schema(), &hash_join.schema()))
            .transpose()?;

        let new_join = HashJoinExec::try_new(
            Arc::clone(hash_join.left()),
            Arc::clone(hash_join.right()),
            hash_join.on().to_vec(),
            hash_join.filter().cloned(),
            hash_join.join_type(),
            projection,
            PartitionMode::CollectLeft,
            hash_join.null_equality(),
            false,
        )?;

        Ok(Transformed::yes(Arc::new(new_join)))
    }

    fn should_collect_left(hash_join: &HashJoinExec) -> bool {
        hash_join.partition_mode() == &PartitionMode::Partitioned
            && hash_join.join_type() == &JoinType::Inner
            && hash_join.filter().is_none()
            && hash_join.left().schema().fields().len() <= 3
            && hash_join.right().schema().fields().len() > hash_join.left().schema().fields().len()
            && Self::is_promql_value_tsid_time_schema(&hash_join.left().schema())
            && Self::joins_on_tsid_and_time(hash_join)
    }

    fn is_promql_value_tsid_time_schema(schema: &SchemaRef) -> bool {
        let mut has_value = false;
        let mut has_tsid = false;
        let mut has_time = false;

        for field in schema.fields() {
            match field.name().as_str() {
                "greptime_value" => has_value = true,
                DATA_SCHEMA_TSID_COLUMN_NAME => has_tsid = true,
                _ if matches!(field.data_type(), DataType::Timestamp(_, _)) => has_time = true,
                _ => return false,
            }
        }

        has_value && has_tsid && has_time
    }

    fn joins_on_tsid_and_time(hash_join: &HashJoinExec) -> bool {
        let mut has_tsid = false;
        let mut has_time = false;

        for (left, right) in hash_join.on() {
            let (Some(left_col), Some(right_col)) = (
                left.as_any().downcast_ref::<Column>(),
                right.as_any().downcast_ref::<Column>(),
            ) else {
                return false;
            };

            if left_col.name() == DATA_SCHEMA_TSID_COLUMN_NAME
                && right_col.name() == DATA_SCHEMA_TSID_COLUMN_NAME
            {
                has_tsid = true;
            } else if matches!(
                hash_join
                    .left()
                    .schema()
                    .field(left_col.index())
                    .data_type(),
                DataType::Timestamp(_, _)
            ) && matches!(
                hash_join
                    .right()
                    .schema()
                    .field(right_col.index())
                    .data_type(),
                DataType::Timestamp(_, _)
            ) {
                has_time = true;
            }
        }

        has_tsid && has_time
    }

    fn infer_projection(
        full_schema: &SchemaRef,
        output_schema: &SchemaRef,
    ) -> DfResult<Vec<usize>> {
        let mut used = vec![false; full_schema.fields().len()];
        let mut projection = Vec::with_capacity(output_schema.fields().len());

        for output_field in output_schema.fields() {
            let Some((idx, _)) = full_schema
                .fields()
                .iter()
                .enumerate()
                .find(|(idx, field)| {
                    !used[*idx]
                        && field.name() == output_field.name()
                        && field.data_type() == output_field.data_type()
                })
            else {
                return datafusion_common::plan_err!(
                    "failed to infer PromQL hash join projection for field {}",
                    output_field.name()
                );
            };

            used[idx] = true;
            projection.push(idx);
        }

        Ok(projection)
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::NullEquality;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion_common::config::ConfigOptions;
    use datafusion_physical_expr::PhysicalExpr;

    use super::*;

    #[test]
    fn chooses_collect_left_for_narrow_promql_build_side() {
        let left = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new("greptime_value", DataType::Float64, true),
            Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ])))) as Arc<dyn ExecutionPlan>;
        let right = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new("greptime_value", DataType::Float64, true),
            Field::new("host", DataType::Utf8, true),
            Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ])))) as Arc<dyn ExecutionPlan>;
        let on = vec![
            (
                Arc::new(Column::new(DATA_SCHEMA_TSID_COLUMN_NAME, 1)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new(DATA_SCHEMA_TSID_COLUMN_NAME, 2)) as Arc<dyn PhysicalExpr>,
            ),
            (
                Arc::new(Column::new("greptime_timestamp", 2)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("greptime_timestamp", 3)) as Arc<dyn PhysicalExpr>,
            ),
        ];
        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                Some(vec![0, 3, 4, 5, 6]),
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let original_schema = join.schema();

        let optimized = PromqlJoinSelection
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        let optimized_join = optimized.as_any().downcast_ref::<HashJoinExec>().unwrap();

        assert_eq!(optimized_join.partition_mode(), &PartitionMode::CollectLeft);
        assert_eq!(optimized.schema(), original_schema);
    }

    #[test]
    fn keeps_partitioned_join_when_left_side_carries_labels() {
        let left = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new("greptime_value", DataType::Float64, true),
            Field::new("host", DataType::Utf8, true),
            Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ])))) as Arc<dyn ExecutionPlan>;
        let right = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new("greptime_value", DataType::Float64, true),
            Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ])))) as Arc<dyn ExecutionPlan>;
        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![
                    (
                        Arc::new(Column::new(DATA_SCHEMA_TSID_COLUMN_NAME, 2))
                            as Arc<dyn PhysicalExpr>,
                        Arc::new(Column::new(DATA_SCHEMA_TSID_COLUMN_NAME, 1))
                            as Arc<dyn PhysicalExpr>,
                    ),
                    (
                        Arc::new(Column::new("greptime_timestamp", 3)) as Arc<dyn PhysicalExpr>,
                        Arc::new(Column::new("greptime_timestamp", 2)) as Arc<dyn PhysicalExpr>,
                    ),
                ],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized = PromqlJoinSelection
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        let optimized_join = optimized.as_any().downcast_ref::<HashJoinExec>().unwrap();

        assert_eq!(optimized_join.partition_mode(), &PartitionMode::Partitioned);
    }
}
