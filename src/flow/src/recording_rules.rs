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

//! Run flow as recording rule which is time-window-aware normal query triggered every tick set by user

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_recordbatch::DfRecordBatch;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::Unparser;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{Column, DFSchema};
use datafusion_expr::LogicalPlan;
use datafusion_physical_expr::PhysicalExprRef;
use datatypes::prelude::DataType;
use datatypes::value::Value;
use datatypes::vectors::{
    TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
    TimestampSecondVector, Vector,
};
use query::parser::QueryLanguageParser;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{ArrowSnafu, DatafusionSnafu, DatatypesSnafu, ExternalSnafu, UnexpectedSnafu};
use crate::Error;

pub async fn sql_to_df_plan(
    query_ctx: QueryContextRef,
    engine: QueryEngineRef,
    sql: &str,
) -> Result<LogicalPlan, Error> {
    let stmt = QueryLanguageParser::parse_sql(sql, &query_ctx)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    let plan = engine
        .planner()
        .plan(&stmt, query_ctx)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    Ok(plan)
}

/// Find the lower bound of time window in given `expr` and `current` timestamp.
///
/// i.e. for `current="2021-07-01 00:01:01.000"` and `expr=date_bin(INTERVAL '5 minutes', ts) as time_window` and `ts_col=ts`,
/// return `Some("2021-07-01 00:00:00.000")` since it's the lower bound
/// of current time window given the current timestamp
///
/// if return None, meaning this time window have no lower bound
fn find_time_window_lower_bound(
    expr: &Expr,
    time_index_col: &str,
    current: Timestamp,
) -> Result<Option<Timestamp>, Error> {
    use std::cmp::Ordering;
    let refs = expr.column_refs();

    ensure!(
        refs.contains(&Column::from_qualified_name(time_index_col)),
        UnexpectedSnafu {
            reason: format!(
                "Expected column {} to be referenced in expression {expr:?}",
                time_index_col
            ),
        }
    );

    ensure!(
        refs.len() == 1,
        UnexpectedSnafu {
            reason: format!(
                "Expect only one column to be referenced in expression {expr:?}, found {refs:?}"
            ),
        }
    );

    let ty = Value::from(current).data_type();

    let arrow_schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        time_index_col,
        ty.as_arrow_type(),
        false,
    )]);
    let df_schema =
        DFSchema::try_from(arrow_schema.clone()).with_context(|_e| DatafusionSnafu {
            context: format!("Failed to create DFSchema from arrow schema {arrow_schema:?}"),
        })?;

    let phy_planner = DefaultPhysicalPlanner::default();

    let phy_expr: PhysicalExprRef = phy_planner
        .create_physical_expr(expr, &df_schema, &SessionContext::new().state())
        .with_context(|_e| DatafusionSnafu {
            context: format!(
                "Failed to create physical expression from {expr:?} using {df_schema:?}"
            ),
        })?;

    let cur_time_window = eval_ts_to_ts(&phy_expr, time_index_col, current)?;

    // search to find the lower bound
    let mut offset: i64 = 1;
    let lower_bound;
    let mut upper_bound = Some(current);
    // first expontial probe to found a range for binary search
    loop {
        let Some(next_val) = current.value().checked_sub(offset) else {
            // no lower bound
            return Ok(None);
        };

        let prev_time_probe = common_time::Timestamp::new(next_val, current.unit());

        let prev_time_window = eval_ts_to_ts(&phy_expr, time_index_col, prev_time_probe)?;

        match prev_time_window.cmp(&cur_time_window) {
            Ordering::Less => {
                lower_bound = Some(prev_time_probe);
                break;
            }
            Ordering::Equal => {
                upper_bound = Some(prev_time_probe);
            }
            Ordering::Greater => {
                UnexpectedSnafu {
                    reason: format!(
                        "Unsupported time window expression, expect monotonic increasing for time window expression {expr:?}"
                    ),
                }
                .fail()?
            }
        }

        let Some(new_offset) = offset.checked_mul(2) else {
            // no lower bound
            return Ok(None);
        };
        offset = new_offset;
    }

    // binary search for the exact lower bound

    ensure!(lower_bound.map(|v|v.unit())==upper_bound.map(|v|v.unit()), UnexpectedSnafu{
        reason: format!(" unit mismatch for time window expression {expr:?}, found {lower_bound:?} and {upper_bound:?}"),
    });

    let output_unit = lower_bound
        .context(UnexpectedSnafu {
            reason: "should have lower bound",
        })?
        .unit();

    let mut low = lower_bound
        .context(UnexpectedSnafu {
            reason: "should have lower bound",
        })?
        .value();
    let mut high = upper_bound
        .context(UnexpectedSnafu {
            reason: "should have upper bound",
        })?
        .value();
    while low < high {
        let mid = (low + high) / 2;
        let mid_probe = common_time::Timestamp::new(mid, output_unit);
        let mid_time_window = eval_ts_to_ts(&phy_expr, time_index_col, mid_probe)?;

        match mid_time_window.cmp(&cur_time_window) {
            Ordering::Less => low = mid + 1,
            Ordering::Equal => high = mid,
            Ordering::Greater => UnexpectedSnafu {
                reason: format!("Binary search failed for time window expression {expr:?}"),
            }
            .fail()?,
        }
    }

    let final_lower_bound_for_time_window = common_time::Timestamp::new(low, output_unit);

    Ok(Some(final_lower_bound_for_time_window))
}

fn eval_ts_to_ts(
    phy: &PhysicalExprRef,
    time_index_col: &str,
    value: Timestamp,
) -> Result<Timestamp, Error> {
    let ts_vector = match value.unit() {
        TimeUnit::Second => TimestampSecondVector::from_vec(vec![value.value()]).to_arrow_array(),
        TimeUnit::Millisecond => {
            TimestampMillisecondVector::from_vec(vec![value.value()]).to_arrow_array()
        }
        TimeUnit::Microsecond => {
            TimestampMicrosecondVector::from_vec(vec![value.value()]).to_arrow_array()
        }
        TimeUnit::Nanosecond => {
            TimestampNanosecondVector::from_vec(vec![value.value()]).to_arrow_array()
        }
    };

    let ty = Value::from(value).data_type();

    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        time_index_col,
        ty.as_arrow_type(),
        false,
    )]));

    let rb = DfRecordBatch::try_new(arrow_schema.clone(), vec![ts_vector.clone()]).with_context(
        |_| ArrowSnafu {
            context: format!(
                "Failed to create record batch from {arrow_schema:?} and {ts_vector:?}"
            ),
        },
    )?;

    let eval_res = phy.evaluate(&rb).with_context(|_| DatafusionSnafu {
        context: format!("Failed to evaluate physical expression {phy:?} on {rb:?}"),
    })?;

    let val = match eval_res {
        datafusion_expr::ColumnarValue::Array(array) => match value.unit() {
            TimeUnit::Second => TimestampSecondVector::try_from_arrow_array(array.clone())
                .with_context(|_| DatatypesSnafu {
                    extra: format!("Failed to create vector from arrow array {array:?}"),
                })?
                .get(0),
            TimeUnit::Millisecond => {
                TimestampMillisecondVector::try_from_arrow_array(array.clone())
                    .with_context(|_| DatatypesSnafu {
                        extra: format!("Failed to create vector from arrow array {array:?}"),
                    })?
                    .get(0)
            }
            TimeUnit::Microsecond => {
                TimestampMicrosecondVector::try_from_arrow_array(array.clone())
                    .with_context(|_| DatatypesSnafu {
                        extra: format!("Failed to create vector from arrow array {array:?}"),
                    })?
                    .get(0)
            }
            TimeUnit::Nanosecond => TimestampNanosecondVector::try_from_arrow_array(array.clone())
                .with_context(|_| DatatypesSnafu {
                    extra: format!("Failed to create vector from arrow array {array:?}"),
                })?
                .get(0),
        },
        datafusion_expr::ColumnarValue::Scalar(scalar) => Value::try_from(scalar.clone())
            .with_context(|_| DatatypesSnafu {
                extra: format!("Failed to convert scalar {scalar:?} to value"),
            })?,
    };

    if let Value::Timestamp(ts) = val {
        Ok(ts)
    } else {
        UnexpectedSnafu {
            reason: format!("Expected timestamp in expression {phy:?} but got {val:?}"),
        }
        .fail()?
    }
}

// TODO(discord9): a method to found out the precise time window

/// Find out the `Filter` Node corresponding to outermost `WHERE` and add a new filter expr to it
#[derive(Debug)]
pub struct AddFilterRewriter {
    extra_filter: Expr,
    is_rewritten: bool,
}

impl AddFilterRewriter {
    fn new(filter: Expr) -> Self {
        Self {
            extra_filter: filter,
            is_rewritten: false,
        }
    }
}

impl TreeNodeRewriter for AddFilterRewriter {
    type Node = LogicalPlan;
    fn f_down(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        if self.is_rewritten {
            return Ok(Transformed::no(node));
        }
        match node {
            LogicalPlan::Filter(mut filter) if !filter.having => {
                filter.predicate = filter.predicate.and(self.extra_filter.clone());
                self.is_rewritten = true;
                Ok(Transformed::yes(LogicalPlan::Filter(filter)))
            }
            LogicalPlan::TableScan(_) => {
                // add a new filter
                let filter =
                    datafusion_expr::Filter::try_new(self.extra_filter.clone(), Arc::new(node))?;
                self.is_rewritten = true;
                Ok(Transformed::yes(LogicalPlan::Filter(filter)))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

fn df_plan_to_sql(plan: &LogicalPlan) -> Result<String, Error> {
    let unparser = Unparser::default();
    let sql = unparser
        .plan_to_sql(plan)
        .with_context(|_e| DatafusionSnafu {
            context: format!("Failed to unparse logical plan {plan:?}"),
        })?;
    Ok(sql.to_string())
}

#[cfg(test)]
mod test {
    use datafusion_common::tree_node::TreeNode;
    use session::context::QueryContext;

    use super::sql_to_df_plan;
    use crate::recording_rules::{df_plan_to_sql, AddFilterRewriter};
    use crate::test_utils::create_test_query_engine;

    #[tokio::test]
    async fn test_add_filter() {
        use datafusion_expr::{col, lit};
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();

        let sql = "SELECT number FROM numbers_with_ts";
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql)
            .await
            .unwrap();

        let mut add_filter = AddFilterRewriter::new(col("number").gt(lit(4u32)));
        let plan = plan.rewrite(&mut add_filter).unwrap().data;
        let new_sql = df_plan_to_sql(&plan).unwrap();
        assert_eq!(
            new_sql,
            "SELECT numbers_with_ts.number FROM numbers_with_ts WHERE (number > 4)"
        );

        let sql = "SELECT number FROM numbers_with_ts WHERE number < 2 OR number >10";
        let plan = sql_to_df_plan(ctx, query_engine, sql).await.unwrap();

        let mut add_filter = AddFilterRewriter::new(col("number").gt(lit(4u32)));
        let plan = plan.rewrite(&mut add_filter).unwrap().data;
        let new_sql = df_plan_to_sql(&plan).unwrap();
        assert_eq!(new_sql, "SELECT numbers_with_ts.number FROM numbers_with_ts WHERE (((numbers_with_ts.number < 2) OR (numbers_with_ts.number > 10)) AND (number > 4))");
    }
}
