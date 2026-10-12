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
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
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
                if let Some(sort_exec) = plan.downcast_ref::<SortExec>() {
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
                    if let Some(column_expr) = first_sort_expr.expr.downcast_ref::<PhysicalColumn>()
                        && matches!(
                            input_schema.field(column_expr.index()).data_type(),
                            DataType::Timestamp(_, _)
                        )
                        && is_time_index_expr(&sort_input, &first_sort_expr.expr)?
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
        if plan.is::<CooperativeExec>() {
            return Ok(Transformed::no(plan));
        }

        // Unappliable case, reset the state.
        if plan.is::<RepartitionExec>()
            || plan.is::<CoalescePartitionsExec>()
            || plan.is::<SortExec>()
            || plan.is::<WindowedSortExec>()
        {
            partition_ranges = None;
        }

        // only a very limited set of plans can exist between region scan and sort exec
        // other plans might make this optimize wrong, so be safe here by limiting it
        if !(plan.is::<ProjectionExec>() || plan.is::<FilterExec>()) {
            partition_ranges = None;
        }

        if let Some(region_scan_exec) = plan.downcast_ref::<RegionScanExec>() {
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

    let result = (|| {
        Some(ScannerInfo {
            partition_ranges: partition_ranges?,
            tag_columns: tag_columns?,
        })
    })();

    Ok(result)
}

fn is_time_index_expr(
    plan: &Arc<dyn ExecutionPlan>,
    expr: &Arc<dyn PhysicalExpr>,
) -> DataFusionResult<bool> {
    if let Some(column_expr) = expr.downcast_ref::<PhysicalColumn>() {
        return is_time_index_column(plan, column_expr);
    }

    if let Some(cast_expr) = expr.downcast_ref::<CastExpr>() {
        let source_type = cast_expr.expr().data_type(plan.schema().as_ref())?;
        return if preserves_timestamp_values(cast_expr.cast_type(), &source_type) {
            is_time_index_expr(plan, cast_expr.expr())
        } else {
            Ok(false)
        };
    }

    if let Some(scalar_function_expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
        if !is_supported_time_index_wrapper(scalar_function_expr)
            || scalar_function_expr.args().len() != 1
        {
            return Ok(false);
        }

        // Wrappers such as `to_timestamp_seconds` cast their argument to another time unit
        // inside the function. That cast does not show up as a `CastExpr` in the plan, but it
        // changes the values of the sort column exactly like a cast in the plan does, so the
        // wrapper is only still the time index if its result keeps both the unit and the values
        // of its argument.
        let arg = &scalar_function_expr.args()[0];
        let return_type = scalar_function_expr.data_type(plan.schema().as_ref())?;
        let arg_type = arg.data_type(plan.schema().as_ref())?;
        return if preserves_timestamp_values(&return_type, &arg_type) {
            is_time_index_expr(plan, arg)
        } else {
            Ok(false)
        };
    }

    Ok(false)
}

/// Returns true if a cast, or a function wrapper, that turns `source` into `target` keeps both the
/// time unit and the underlying values of a timestamp.
///
/// A cast (or a hidden cast inside a function wrapper) that changes the time unit changes the
/// values of the sort column, so the working ranges derived from the time index, which keep the
/// unit of the time index, no longer apply. A cast adding a timezone to a naive timestamp
/// re-interprets the underlying values in the same way. Only a target that keeps both the unit
/// and the values of its source can still be the time index.
fn preserves_timestamp_values(target: &DataType, source: &DataType) -> bool {
    matches!(
        (target, source),
        (
            DataType::Timestamp(target_unit, target_tz),
            DataType::Timestamp(source_unit, source_tz)
        ) if target_unit == source_unit && (source_tz.is_some() || target_tz.is_none())
    )
}

fn is_time_index_column(
    plan: &Arc<dyn ExecutionPlan>,
    column_expr: &PhysicalColumn,
) -> DataFusionResult<bool> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let Some(projection_expr) = projection.expr().get(column_expr.index()) else {
            return Ok(false);
        };
        return is_time_index_expr(projection.input(), &projection_expr.expr);
    }

    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let child_column_expr = filter
            .projection()
            .as_ref()
            .and_then(|projection| projection.get(column_expr.index()).copied())
            .map(|input_index| {
                PhysicalColumn::new(
                    filter.input().schema().field(input_index).name(),
                    input_index,
                )
            })
            .unwrap_or_else(|| column_expr.clone());
        let child_expr = Arc::new(child_column_expr) as Arc<dyn PhysicalExpr>;
        return is_time_index_expr(filter.input(), &child_expr);
    }

    if let Some(region_scan_exec) = plan.downcast_ref::<RegionScanExec>() {
        let schema = plan.schema();
        let column_field = schema.field(column_expr.index());
        return Ok(
            matches!(column_field.data_type(), DataType::Timestamp(_, _))
                && *column_field.name() == region_scan_exec.time_index(),
        );
    }

    let Some(child) = passthrough_child(plan.as_ref()) else {
        return Ok(false);
    };
    let child_expr = Arc::new(column_expr.clone()) as Arc<dyn PhysicalExpr>;
    is_time_index_expr(&child, &child_expr)
}

fn passthrough_child(plan: &dyn ExecutionPlan) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.is::<CoalescePartitionsExec>()
        || plan.is::<RepartitionExec>()
        || plan.is::<CooperativeExec>()
    {
        return schema_preserving_child(plan);
    }

    None
}

fn schema_preserving_child(plan: &dyn ExecutionPlan) -> Option<Arc<dyn ExecutionPlan>> {
    let child = plan.children().first().cloned().cloned()?;
    (plan.schema().as_ref() == child.schema().as_ref()).then_some(child)
}

fn is_supported_time_index_wrapper(expr: &ScalarFunctionExpr) -> bool {
    (expr.name().eq_ignore_ascii_case("to_timestamp")
        || expr.name().eq_ignore_ascii_case("to_timestamp_seconds")
        || expr.name().eq_ignore_ascii_case("to_timestamp_millis")
        || expr.name().eq_ignore_ascii_case("to_timestamp_micros")
        || expr.name().eq_ignore_ascii_case("to_timestamp_nanos"))
        && matches!(expr.return_type(), DataType::Timestamp(_, _))
}

/// Removes the repartition plan between the filter and region scan.
fn remove_repartition(
    plan: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Transformed<Arc<dyn ExecutionPlan>>> {
    plan.transform_down(|plan| {
        if plan.is::<FilterExec>() {
            // Checks child.
            let maybe_repartition = plan.children()[0];
            if maybe_repartition.is::<RepartitionExec>() {
                let maybe_scan = maybe_repartition.children()[0];
                if maybe_scan.is::<RegionScanExec>() {
                    let new_filter = plan.clone().replace_children(
                        vec![maybe_scan.clone()],
                        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                    )?;
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
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion_common::ScalarValue;
    use datafusion_functions::datetime::{to_timestamp, to_timestamp_millis, to_timestamp_seconds};
    use datafusion_physical_expr::expressions::{CastExpr, Literal};
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
        let expr = Arc::new(PhysicalColumn::new("alias_ts", 0)) as Arc<dyn PhysicalExpr>;

        assert!(is_time_index_expr(&projection, &expr).unwrap());
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
        let expr = Arc::new(PhysicalColumn::new("alias_2", 0)) as Arc<dyn PhysicalExpr>;

        assert!(is_time_index_expr(&second_projection, &expr).unwrap());
    }

    #[test]
    fn test_is_time_index_expr_tracks_value_preserving_wrapper_through_projection() {
        let scan = new_region_scan_with_unit(TimeUnit::Millisecond);
        let config = Arc::new(ConfigOptions::default());
        let wrapper = ScalarFunctionExpr::try_new(
            to_timestamp_millis(config.as_ref()),
            vec![Arc::new(PhysicalColumn::new("ts", 1))],
            scan.schema().as_ref(),
            config,
        )
        .unwrap();
        // `to_timestamp_millis` keeps both the unit and the values of a millisecond timestamp.
        assert_eq!(
            wrapper.data_type(scan.schema().as_ref()).unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(Arc::new(wrapper) as Arc<dyn PhysicalExpr>, "ts".to_string())],
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let expr = Arc::new(PhysicalColumn::new("ts", 0)) as Arc<dyn PhysicalExpr>;

        // So the wrapper is still the time index.
        assert!(is_time_index_expr(&projection, &expr).unwrap());
    }

    #[test]
    fn test_is_time_index_expr_rejects_unit_changing_wrapper() {
        let config = Arc::new(ConfigOptions::default());

        // `to_timestamp_millis` casts its argument to milliseconds, which truncates the values of
        // a nanosecond time index, so the wrapper is no longer the time index.
        let nanosecond_scan = new_region_scan();
        let truncates_to_millis = Arc::new(
            ScalarFunctionExpr::try_new(
                to_timestamp_millis(config.as_ref()),
                vec![Arc::new(PhysicalColumn::new("ts", 1))],
                nanosecond_scan.schema().as_ref(),
                config.clone(),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        assert_eq!(
            truncates_to_millis
                .data_type(nanosecond_scan.schema().as_ref())
                .unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert!(!is_time_index_expr(&nanosecond_scan, &truncates_to_millis).unwrap());

        // Same for a millisecond time index wrapped in `to_timestamp_seconds`.
        let millisecond_scan = new_region_scan_with_unit(TimeUnit::Millisecond);
        let truncates_to_seconds = Arc::new(
            ScalarFunctionExpr::try_new(
                to_timestamp_seconds(config.as_ref()),
                vec![Arc::new(PhysicalColumn::new("ts", 1))],
                millisecond_scan.schema().as_ref(),
                config.clone(),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        assert_eq!(
            truncates_to_seconds
                .data_type(millisecond_scan.schema().as_ref())
                .unwrap(),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert!(!is_time_index_expr(&millisecond_scan, &truncates_to_seconds).unwrap());

        // A supported wrapper whose argument is not a timestamp is never the time index.
        let from_integer = Arc::new(
            ScalarFunctionExpr::try_new(
                to_timestamp_millis(config.as_ref()),
                vec![Arc::new(Literal::new(ScalarValue::Int64(Some(0))))],
                nanosecond_scan.schema().as_ref(),
                config,
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        assert!(!is_time_index_expr(&nanosecond_scan, &from_integer).unwrap());
    }

    #[test]
    fn test_is_time_index_expr_rejects_unit_changing_cast_through_projection() {
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
        let expr = Arc::new(PhysicalColumn::new("ts_ms", 0)) as Arc<dyn PhysicalExpr>;

        // The cast changes the time unit (ns → ms), so it is no longer the time index.
        assert!(!is_time_index_expr(&projection, &expr).unwrap());
    }

    #[test]
    fn test_is_time_index_expr_rejects_timezone_adding_cast() {
        let scan = new_region_scan();

        // The cast keeps the time unit but adds a timezone, which re-interprets the
        // underlying values, so it is no longer the time index.
        let adds_timezone = Arc::new(CastExpr::new(
            Arc::new(PhysicalColumn::new("ts", 1)),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+07:00"))),
            None,
        )) as Arc<dyn PhysicalExpr>;
        assert!(!is_time_index_expr(&scan, &adds_timezone).unwrap());

        // Adding the `UTC` timezone also shifts the underlying values, so it is rejected
        // as well.
        let adds_utc_timezone = Arc::new(CastExpr::new(
            Arc::new(PhysicalColumn::new("ts", 1)),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            None,
        )) as Arc<dyn PhysicalExpr>;
        assert!(!is_time_index_expr(&scan, &adds_utc_timezone).unwrap());

        // `to_timestamp` returns an aware timestamp in the session time zone for a naive
        // timestamp argument. That re-interprets the underlying values in the same way as a
        // timezone-adding cast, so the wrapper is rejected as well.
        let mut config = ConfigOptions::default();
        config.execution.time_zone = Some("+08:00".to_string());
        let config = Arc::new(config);
        let aware_source = ScalarFunctionExpr::try_new(
            to_timestamp(config.as_ref()),
            vec![Arc::new(PhysicalColumn::new("ts", 1))],
            scan.schema().as_ref(),
            config.clone(),
        )
        .unwrap();
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    Arc::new(aware_source) as Arc<dyn PhysicalExpr>,
                    "ts_aware".to_string(),
                )],
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        assert_eq!(
            projection.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+08:00")))
        );
        let expr = Arc::new(PhysicalColumn::new("ts_aware", 0)) as Arc<dyn PhysicalExpr>;

        assert!(!is_time_index_expr(&projection, &expr).unwrap());

        // A cast between two aware timestamps of the same unit keeps the underlying values, see
        // `test_preserves_timestamp_values`, but it is still rejected here as its input is not
        // the time index.
        let aware_cast = Arc::new(CastExpr::new(
            Arc::new(PhysicalColumn::new("ts_aware", 0)),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+07:00"))),
            None,
        )) as Arc<dyn PhysicalExpr>;

        assert!(!is_time_index_expr(&projection, &aware_cast).unwrap());
    }

    #[test]
    fn test_preserves_timestamp_values() {
        let naive_nanosecond = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let naive_millisecond = DataType::Timestamp(TimeUnit::Millisecond, None);
        let aware_nanosecond = DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+08:00")));
        let other_aware_nanosecond =
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+07:00")));

        // Keeping the unit and the values of a timestamp.
        assert!(preserves_timestamp_values(
            &naive_nanosecond,
            &naive_nanosecond
        ));
        assert!(preserves_timestamp_values(
            &aware_nanosecond,
            &other_aware_nanosecond
        ));

        // Changing the time unit, adding a timezone to a naive timestamp, or leaving the timestamp
        // domain entirely all change the values of the timestamp.
        assert!(!preserves_timestamp_values(
            &naive_millisecond,
            &naive_nanosecond
        ));
        assert!(!preserves_timestamp_values(
            &aware_nanosecond,
            &naive_nanosecond
        ));
        assert!(!preserves_timestamp_values(
            &naive_nanosecond,
            &DataType::Int64
        ));
        assert!(!preserves_timestamp_values(
            &DataType::Int64,
            &naive_nanosecond
        ));

        // Dropping the timezone of an aware timestamp keeps the underlying values, so it is
        // value-preserving; only the reverse direction re-interprets them.
        assert!(preserves_timestamp_values(
            &naive_nanosecond,
            &aware_nanosecond
        ));
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
        let expr = Arc::new(PhysicalColumn::new("ts", 0)) as Arc<dyn PhysicalExpr>;

        assert!(!is_time_index_expr(&projection, &expr).unwrap());
    }

    #[test]
    fn test_is_supported_time_index_wrapper_ignores_function_name_case() {
        let config = Arc::new(ConfigOptions::default());
        let return_field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ));
        let expr = ScalarFunctionExpr::new(
            "To_Timestamp_Millis",
            to_timestamp_millis(config.as_ref()),
            vec![Arc::new(PhysicalColumn::new("ts", 1))],
            return_field,
            config,
        );

        assert!(is_supported_time_index_wrapper(&expr));
    }

    #[test]
    fn test_is_time_index_expr_rejects_non_timestamp_casts() {
        let scan = new_region_scan();
        let cast_expr = Arc::new(CastExpr::new(
            Arc::new(PhysicalColumn::new("ts", 1)),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            None,
        )) as Arc<dyn PhysicalExpr>;
        // The cast changes the time unit (ns → ms), so it is no longer the time index.
        assert!(!is_time_index_expr(&scan, &cast_expr).unwrap());

        let non_timestamp_cast = Arc::new(CastExpr::new(
            Arc::new(PhysicalColumn::new("ts", 1)),
            DataType::Int64,
            None,
        )) as Arc<dyn PhysicalExpr>;
        assert!(!is_time_index_expr(&scan, &non_timestamp_cast).unwrap());
    }

    #[test]
    fn test_is_time_index_expr_tracks_time_index_through_filter() {
        let scan = new_region_scan();
        let filter = Arc::new(
            FilterExec::try_new(
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
                scan,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let expr = Arc::new(PhysicalColumn::new("ts", 1)) as Arc<dyn PhysicalExpr>;

        assert!(is_time_index_expr(&filter, &expr).unwrap());
    }

    #[test]
    fn test_is_time_index_expr_tracks_time_index_through_passthrough_wrapper_and_filter_projection()
    {
        let scan = new_region_scan();
        let projected_filter = Arc::new(
            FilterExecBuilder::new(
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
                scan,
            )
            .apply_projection(Some(vec![1]))
            .unwrap()
            .build()
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let cooperative =
            Arc::new(CooperativeExec::new(projected_filter)) as Arc<dyn ExecutionPlan>;
        let expr = Arc::new(PhysicalColumn::new("ts", 0)) as Arc<dyn PhysicalExpr>;

        assert!(is_time_index_expr(&cooperative, &expr).unwrap());
    }

    #[test]
    fn test_schema_preserving_child_rejects_schema_changing_projection() {
        let scan = new_region_scan();
        let projection = ProjectionExec::try_new(
            vec![(
                Arc::new(PhysicalColumn::new("ts", 1)) as Arc<dyn PhysicalExpr>,
                "ts".to_string(),
            )],
            scan,
        )
        .unwrap();

        assert!(schema_preserving_child(&projection).is_none());
    }

    #[test]
    fn test_cooperative_exec_satisfies_passthrough_schema_contract() {
        let child = new_region_scan();
        let plan = Arc::new(CooperativeExec::new(child.clone())) as Arc<dyn ExecutionPlan>;

        assert_passthrough_schema_contract(plan, child);
    }

    #[test]
    fn test_repartition_exec_satisfies_passthrough_schema_contract() {
        let child = new_region_scan();
        let plan = Arc::new(
            RepartitionExec::try_new(
                child.clone(),
                datafusion_physical_expr::Partitioning::RoundRobinBatch(2),
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert_passthrough_schema_contract(plan, child);
    }

    #[test]
    fn test_coalesce_partitions_exec_satisfies_passthrough_schema_contract() {
        let child = new_region_scan();
        let plan = Arc::new(CoalescePartitionsExec::new(child.clone())) as Arc<dyn ExecutionPlan>;

        assert_passthrough_schema_contract(plan, child);
    }

    fn assert_passthrough_schema_contract(
        plan: Arc<dyn ExecutionPlan>,
        child: Arc<dyn ExecutionPlan>,
    ) {
        assert_eq!(plan.schema().as_ref(), child.schema().as_ref());

        let passthrough = passthrough_child(plan.as_ref()).expect("wrapper should preserve schema");
        assert_eq!(passthrough.schema().as_ref(), child.schema().as_ref());
    }

    fn new_region_scan() -> Arc<dyn ExecutionPlan> {
        new_region_scan_with_unit(TimeUnit::Nanosecond)
    }

    fn new_region_scan_with_unit(unit: TimeUnit) -> Arc<dyn ExecutionPlan> {
        let timestamp_type = match unit {
            TimeUnit::Second => ConcreteDataType::timestamp_second_datatype(),
            TimeUnit::Millisecond => ConcreteDataType::timestamp_millisecond_datatype(),
            TimeUnit::Microsecond => ConcreteDataType::timestamp_microsecond_datatype(),
            TimeUnit::Nanosecond => ConcreteDataType::timestamp_nanosecond_datatype(),
        };
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("ts", timestamp_type.clone(), false),
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
                column_schema: ColumnSchema::new("ts", timestamp_type, false),
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
