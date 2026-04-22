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

use arrow_schema::DataType;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_common::Result as DataFusionResult;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_expr::expressions::{CastExpr, Column as PhysicalColumn};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use store_api::region_engine::PartitionRange;
use table::table::scan::RegionScanExec;

use crate::part_sort::PartSortExec;
use crate::window_sort::WindowedSortExec;

/// Optimize rule for windowed sort.
///
/// This is expected to run after [`ScanHint`] and [`ParallelizeScan`].
/// It would change the original sort to a custom plan. To make sure
/// other rules are applied correctly, this rule can be run as later as
/// possible.
///
/// [`ScanHint`]: crate::optimizer::scan_hint::ScanHintRule
/// [`ParallelizeScan`]: crate::optimizer::parallelize_scan::ParallelizeScan
#[derive(Debug)]
pub struct WindowedSortPhysicalRule;

impl PhysicalOptimizerRule for WindowedSortPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan, config)
    }

    fn name(&self) -> &str {
        "WindowedSortRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

impl WindowedSortPhysicalRule {
    fn do_optimize(
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let result = plan
            .transform_down(|plan| {
                if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
                    // TODO: support multiple expr in windowed sort
                    if sort_exec.expr().len() != 1 {
                        return Ok(Transformed::no(plan));
                    }

                    let preserve_partitioning = sort_exec.preserve_partitioning();

                    let sort_input = remove_repartition(sort_exec.input().clone())?.data;

                    // Gets scanner info from the input without repartition before filter.
                    let Some(scanner_info) = fetch_partition_range(sort_input.clone())? else {
                        return Ok(Transformed::no(plan));
                    };
                    let input_schema = sort_input.schema();

                    let first_sort_expr = sort_exec.expr().first();
                    if let Some(column_expr) = first_sort_expr
                        .expr
                        .as_any()
                        .downcast_ref::<PhysicalColumn>()
                        && matches!(
                            input_schema.field(column_expr.index()).data_type(),
                            DataType::Timestamp(_, _)
                        )
                        && is_time_index_expr(sort_input.clone(), first_sort_expr.expr.clone())?
                        && sort_exec.fetch().is_none()
                    // skip if there is a limit, as dyn filter along is good enough in this case
                    {
                    } else {
                        return Ok(Transformed::no(plan));
                    }

                    // PartSortExec is unnecessary if:
                    // - there is no tag column, and
                    // - the sort is ascending on the time index column
                    let new_input = if scanner_info.tag_columns.is_empty()
                        && !first_sort_expr.options.descending
                    {
                        sort_input
                    } else {
                        Arc::new(PartSortExec::try_new(
                            first_sort_expr.clone(),
                            sort_exec.fetch(),
                            scanner_info.partition_ranges.clone(),
                            sort_input,
                        )?)
                    };

                    let windowed_sort_exec = WindowedSortExec::try_new(
                        first_sort_expr.clone(),
                        sort_exec.fetch(),
                        scanner_info.partition_ranges,
                        new_input,
                    )?;

                    if !preserve_partitioning {
                        let order_preserving_merge = SortPreservingMergeExec::new(
                            sort_exec.expr().clone(),
                            Arc::new(windowed_sort_exec),
                        );
                        return Ok(Transformed {
                            data: Arc::new(order_preserving_merge),
                            transformed: true,
                            tnr: datafusion_common::tree_node::TreeNodeRecursion::Stop,
                        });
                    } else {
                        return Ok(Transformed {
                            data: Arc::new(windowed_sort_exec),
                            transformed: true,
                            tnr: datafusion_common::tree_node::TreeNodeRecursion::Stop,
                        });
                    }
                }

                Ok(Transformed::no(plan))
            })?
            .data;

        Ok(result)
    }
}

#[derive(Debug)]
struct ScannerInfo {
    partition_ranges: Vec<Vec<PartitionRange>>,
    tag_columns: Vec<String>,
}

fn fetch_partition_range(input: Arc<dyn ExecutionPlan>) -> DataFusionResult<Option<ScannerInfo>> {
    let mut partition_ranges = None;
    let mut tag_columns = None;

    input.transform_up(|plan| {
        if plan.as_any().is::<CooperativeExec>() {
            return Ok(Transformed::no(plan));
        }

        // Unappliable case, reset the state.
        if plan.as_any().is::<RepartitionExec>()
            || plan.as_any().is::<CoalescePartitionsExec>()
            || plan.as_any().is::<SortExec>()
            || plan.as_any().is::<WindowedSortExec>()
        {
            partition_ranges = None;
        }

        // only a very limited set of plans can exist between region scan and sort exec
        // other plans might make this optimize wrong, so be safe here by limiting it
        if !(plan.as_any().is::<ProjectionExec>() || plan.as_any().is::<FilterExec>()) {
            partition_ranges = None;
        }

        if let Some(region_scan_exec) = plan.as_any().downcast_ref::<RegionScanExec>() {
            // `PerSeries` distribution is not supported in windowed sort.
            if region_scan_exec.distribution()
                == Some(store_api::storage::TimeSeriesDistribution::PerSeries)
            {
                partition_ranges = None;
                return Ok(Transformed::no(plan));
            }

            partition_ranges = Some(region_scan_exec.get_uncollapsed_partition_ranges());
            tag_columns = Some(region_scan_exec.tag_columns());

            region_scan_exec.with_distinguish_partition_range(true);
        }

        Ok(Transformed::no(plan))
    })?;

    let result = try {
        ScannerInfo {
            partition_ranges: partition_ranges?,
            tag_columns: tag_columns?,
        }
    };

    Ok(result)
}

fn is_time_index_expr(
    plan: Arc<dyn ExecutionPlan>,
    expr: Arc<dyn PhysicalExpr>,
) -> DataFusionResult<bool> {
    if let Some(column_expr) = expr.as_any().downcast_ref::<PhysicalColumn>() {
        return is_time_index_column(plan, column_expr);
    }

    if let Some(cast_expr) = expr.as_any().downcast_ref::<CastExpr>() {
        return if matches!(cast_expr.cast_type(), DataType::Timestamp(_, _)) {
            is_time_index_expr(plan, cast_expr.expr().clone())
        } else {
            Ok(false)
        };
    }

    if let Some(scalar_function_expr) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
        return if is_supported_time_index_wrapper(scalar_function_expr)
            && scalar_function_expr.args().len() == 1
        {
            is_time_index_expr(plan, scalar_function_expr.args()[0].clone())
        } else {
            Ok(false)
        };
    }

    Ok(false)
}

fn is_time_index_column(
    plan: Arc<dyn ExecutionPlan>,
    column_expr: &PhysicalColumn,
) -> DataFusionResult<bool> {
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let Some(projection_expr) = projection.expr().get(column_expr.index()) else {
            return Ok(false);
        };
        return is_time_index_expr(projection.input().clone(), projection_expr.expr.clone());
    }

    if let Some(region_scan_exec) = plan.as_any().downcast_ref::<RegionScanExec>() {
        return Ok(matches!(
            plan.schema().field(column_expr.index()).data_type(),
            DataType::Timestamp(_, _)
        ) && plan.schema().field(column_expr.index()).name().as_ref()
            == region_scan_exec.time_index());
    }

    let Some(child) = passthrough_child(plan.as_ref()) else {
        return Ok(false);
    };
    is_time_index_expr(child, Arc::new(column_expr.clone()))
}

fn passthrough_child(plan: &dyn ExecutionPlan) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.as_any().is::<FilterExec>()
        || plan.as_any().is::<CoalescePartitionsExec>()
        || plan.as_any().is::<RepartitionExec>()
        || plan.as_any().is::<CooperativeExec>()
    {
        return plan.children().first().cloned().cloned();
    }

    None
}

fn is_supported_time_index_wrapper(expr: &ScalarFunctionExpr) -> bool {
    matches!(
        expr.name(),
        "to_timestamp"
            | "to_timestamp_seconds"
            | "to_timestamp_millis"
            | "to_timestamp_micros"
            | "to_timestamp_nanos"
    ) && matches!(expr.return_type(), DataType::Timestamp(_, _))
}

/// Removes the repartition plan between the filter and region scan.
fn remove_repartition(
    plan: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Transformed<Arc<dyn ExecutionPlan>>> {
    plan.transform_down(|plan| {
        if plan.as_any().is::<FilterExec>() {
            // Checks child.
            let maybe_repartition = plan.children()[0];
            if maybe_repartition.as_any().is::<RepartitionExec>() {
                let maybe_scan = maybe_repartition.children()[0];
                if maybe_scan.as_any().is::<RegionScanExec>() {
                    let new_filter = plan.clone().with_new_children(vec![maybe_scan.clone()])?;
                    return Ok(Transformed::yes(new_filter));
                }
            }
        }

        Ok(Transformed::no(plan))
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use arrow_schema::{Field, TimeUnit};
    use common_recordbatch::RecordBatches;
    use datafusion::config::ConfigOptions;
    use datafusion_functions::datetime::to_timestamp_millis;
    use datafusion_physical_expr::expressions::CastExpr;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::region_engine::SinglePartitionScanner;
    use store_api::storage::{RegionId, ScanRequest};

    use super::*;

    #[test]
    fn test_is_time_index_expr_tracks_aliases_through_projection() {
        let scan = new_region_scan();
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    Arc::new(PhysicalColumn::new("ts", 1)) as Arc<dyn PhysicalExpr>,
                    "alias_ts".to_string(),
                )],
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(
            is_time_index_expr(projection, Arc::new(PhysicalColumn::new("alias_ts", 0))).unwrap()
        );
    }

    #[test]
    fn test_is_time_index_expr_tracks_multi_level_aliases() {
        let scan = new_region_scan();
        let first_projection = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    Arc::new(PhysicalColumn::new("ts", 1)) as Arc<dyn PhysicalExpr>,
                    "alias_1".to_string(),
                )],
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let second_projection = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    Arc::new(PhysicalColumn::new("alias_1", 0)) as Arc<dyn PhysicalExpr>,
                    "alias_2".to_string(),
                )],
                first_projection,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(
            is_time_index_expr(
                second_projection,
                Arc::new(PhysicalColumn::new("alias_2", 0))
            )
            .unwrap()
        );
    }

    #[test]
    fn test_is_time_index_expr_tracks_wrapped_aliases_through_projection() {
        let scan = new_region_scan();
        let config = Arc::new(ConfigOptions::default());
        let return_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    Arc::new(ScalarFunctionExpr::new(
                        "to_timestamp_millis",
                        to_timestamp_millis(config.as_ref()),
                        vec![Arc::new(PhysicalColumn::new("ts", 1))],
                        return_field,
                        config,
                    )) as Arc<dyn PhysicalExpr>,
                    "ts".to_string(),
                )],
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(is_time_index_expr(projection, Arc::new(PhysicalColumn::new("ts", 0))).unwrap());
    }

    #[test]
    fn test_is_time_index_expr_tracks_cast_aliases_through_projection() {
        let scan = new_region_scan();
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    Arc::new(CastExpr::new(
                        Arc::new(PhysicalColumn::new("ts", 1)),
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        None,
                    )) as Arc<dyn PhysicalExpr>,
                    "ts_ms".to_string(),
                )],
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(is_time_index_expr(projection, Arc::new(PhysicalColumn::new("ts_ms", 0))).unwrap());
    }

    #[test]
    fn test_is_time_index_expr_rejects_unsupported_wrappers() {
        let scan = new_region_scan();
        let config = Arc::new(ConfigOptions::default());
        let return_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    Arc::new(ScalarFunctionExpr::new(
                        "date_trunc",
                        to_timestamp_millis(config.as_ref()),
                        vec![Arc::new(PhysicalColumn::new("ts", 1))],
                        return_field,
                        config,
                    )) as Arc<dyn PhysicalExpr>,
                    "ts".to_string(),
                )],
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(!is_time_index_expr(projection, Arc::new(PhysicalColumn::new("ts", 0))).unwrap());
    }

    #[test]
    fn test_is_time_index_expr_rejects_non_timestamp_casts() {
        let scan = new_region_scan();
        let cast_expr = CastExpr::new(
            Arc::new(PhysicalColumn::new("ts", 1)),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            None,
        );
        assert!(is_time_index_expr(scan.clone(), Arc::new(cast_expr)).unwrap());

        let non_timestamp_cast = CastExpr::new(
            Arc::new(PhysicalColumn::new("ts", 1)),
            DataType::Int64,
            None,
        );
        assert!(!is_time_index_expr(scan, Arc::new(non_timestamp_cast)).unwrap());
    }

    fn new_region_scan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_nanosecond_datatype(),
                false,
            ),
        ]));
        let recordbatches = RecordBatches::try_new(schema.clone(), vec![]).unwrap();
        let stream = recordbatches.as_stream();

        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "value",
                    ConcreteDataType::int32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_nanosecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            });

        let scanner = Box::new(SinglePartitionScanner::new(
            stream,
            false,
            Arc::new(builder.build().unwrap()),
            None,
        ));
        Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap())
    }
}
