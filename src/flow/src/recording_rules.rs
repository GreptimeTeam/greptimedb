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

mod engine;
mod frontend_client;

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use api::helper::pb_value_to_value_ref;
use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_recordbatch::DfRecordBatch;
use common_telemetry::warn;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::Unparser;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion_common::{Column, DFSchema, TableReference};
use datafusion_expr::{ColumnarValue, LogicalPlan};
use datafusion_physical_expr::PhysicalExprRef;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::scalars::ScalarVector;
use datatypes::value::Value;
use datatypes::vectors::{
    TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
    TimestampSecondVector, Vector,
};
pub use engine::RecordingRuleEngine;
pub use frontend_client::FrontendClient;
use itertools::Itertools;
use query::parser::QueryLanguageParser;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};

use crate::adapter::util::from_proto_to_data_type;
use crate::df_optimizer::apply_df_optimizer;
use crate::error::{ArrowSnafu, DatafusionSnafu, DatatypesSnafu, ExternalSnafu, UnexpectedSnafu};
use crate::expr::error::DataTypeSnafu;
use crate::Error;

#[derive(Debug, Clone)]
pub struct TimeWindowExpr {
    expr: PhysicalExprRef,
    column_name: String,
}

impl TimeWindowExpr {
    pub fn new(expr: PhysicalExprRef, column_name: String) -> Self {
        Self { expr, column_name }
    }

    pub fn from_expr(expr: &Expr, column_name: &str, df_schema: &DFSchema) -> Result<Self, Error> {
        let phy_planner = DefaultPhysicalPlanner::default();

        let phy_expr: PhysicalExprRef = phy_planner
            .create_physical_expr(expr, df_schema, &SessionContext::new().state())
            .with_context(|_e| DatafusionSnafu {
                context: format!(
                    "Failed to create physical expression from {expr:?} using {df_schema:?}"
                ),
            })?;
        Ok(Self {
            expr: phy_expr,
            column_name: column_name.to_string(),
        })
    }

    /// Find timestamps from rows using time window expr
    pub async fn handle_rows(
        &self,
        rows_list: Vec<api::v1::Rows>,
    ) -> Result<BTreeSet<Timestamp>, Error> {
        let mut time_windows = BTreeSet::new();

        for rows in rows_list {
            // pick the time index column and use it to eval on `self.expr`
            let ts_col_index = rows
                .schema
                .iter()
                .map(|col| col.column_name.clone())
                .position(|name| name == self.column_name);
            let Some(ts_col_index) = ts_col_index else {
                warn!("can't found time index column in schema: {:?}", rows.schema);
                continue;
            };
            let col_schema = &rows.schema[ts_col_index];
            let cdt = from_proto_to_data_type(col_schema)?;

            let column_values = rows
                .rows
                .iter()
                .map(|row| &row.values[ts_col_index])
                .collect_vec();

            let mut vector = cdt.create_mutable_vector(column_values.len());
            for value in column_values {
                let value = pb_value_to_value_ref(value, &None);
                vector.try_push_value_ref(value).context(DataTypeSnafu {
                    msg: "Failed to convert rows to columns",
                })?;
            }
            let vector = vector.to_vector();

            let df_schema = create_df_schema_for_ts_column(&self.column_name, cdt)?;

            let rb =
                DfRecordBatch::try_new(df_schema.inner().clone(), vec![vector.to_arrow_array()])
                    .with_context(|_e| ArrowSnafu {
                        context: format!(
                            "Failed to create record batch from {df_schema:?} and {vector:?}"
                        ),
                    })?;

            let eval_res = self.expr.evaluate(&rb).with_context(|_| DatafusionSnafu {
                context: format!(
                    "Failed to evaluate physical expression {:?} on {rb:?}",
                    self.expr
                ),
            })?;

            let res = columnar_to_ts_vector(&eval_res)?;

            for ts in res.into_iter().flatten() {
                time_windows.insert(ts);
            }
        }

        Ok(time_windows)
    }
}

fn create_df_schema_for_ts_column(name: &str, cdt: ConcreteDataType) -> Result<DFSchema, Error> {
    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        name,
        cdt.as_arrow_type(),
        false,
    )]));

    let df_schema = DFSchema::from_field_specific_qualified_schema(
        vec![Some(TableReference::bare("TimeIndexOnlyTable"))],
        &arrow_schema,
    )
    .with_context(|_e| DatafusionSnafu {
        context: format!("Failed to create DFSchema from arrow schema {arrow_schema:?}"),
    })?;

    Ok(df_schema)
}

/// Convert `ColumnarValue` to `Vec<Option<Timestamp>>`
fn columnar_to_ts_vector(columnar: &ColumnarValue) -> Result<Vec<Option<Timestamp>>, Error> {
    let val = match columnar {
        datafusion_expr::ColumnarValue::Array(array) => {
            let ty = array.data_type();
            let ty = ConcreteDataType::from_arrow_type(ty);
            let time_unit = if let ConcreteDataType::Timestamp(ty) = ty {
                ty.unit()
            } else {
                return UnexpectedSnafu {
                    reason: format!("Non-timestamp type: {ty:?}"),
                }
                .fail();
            };

            match time_unit {
                TimeUnit::Second => TimestampSecondVector::try_from_arrow_array(array.clone())
                    .with_context(|_| DatatypesSnafu {
                        extra: format!("Failed to create vector from arrow array {array:?}"),
                    })?
                    .iter_data()
                    .map(|d| d.map(|d| d.0))
                    .collect_vec(),
                TimeUnit::Millisecond => {
                    TimestampMillisecondVector::try_from_arrow_array(array.clone())
                        .with_context(|_| DatatypesSnafu {
                            extra: format!("Failed to create vector from arrow array {array:?}"),
                        })?
                        .iter_data()
                        .map(|d| d.map(|d| d.0))
                        .collect_vec()
                }
                TimeUnit::Microsecond => {
                    TimestampMicrosecondVector::try_from_arrow_array(array.clone())
                        .with_context(|_| DatatypesSnafu {
                            extra: format!("Failed to create vector from arrow array {array:?}"),
                        })?
                        .iter_data()
                        .map(|d| d.map(|d| d.0))
                        .collect_vec()
                }
                TimeUnit::Nanosecond => {
                    TimestampNanosecondVector::try_from_arrow_array(array.clone())
                        .with_context(|_| DatatypesSnafu {
                            extra: format!("Failed to create vector from arrow array {array:?}"),
                        })?
                        .iter_data()
                        .map(|d| d.map(|d| d.0))
                        .collect_vec()
                }
            }
        }
        datafusion_expr::ColumnarValue::Scalar(scalar) => {
            let value = Value::try_from(scalar.clone()).with_context(|_| DatatypesSnafu {
                extra: format!("Failed to convert scalar {scalar:?} to value"),
            })?;
            let ts = value.as_timestamp().context(UnexpectedSnafu {
                reason: format!("Expect Timestamp, found {:?}", value),
            })?;
            vec![Some(ts)]
        }
    };
    Ok(val)
}

/// Convert sql to datafusion logical plan
pub async fn sql_to_df_plan(
    query_ctx: QueryContextRef,
    engine: QueryEngineRef,
    sql: &str,
    optimize: bool,
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
    let plan = if optimize {
        apply_df_optimizer(plan).await?
    } else {
        plan
    };
    Ok(plan)
}

/// Return (the column name of time index column, the time window expr, the expected time unit of time index column, the expr's schema for evaluating the time window)
async fn find_time_window_expr(
    plan: &LogicalPlan,
    catalog_man: CatalogManagerRef,
    query_ctx: QueryContextRef,
) -> Result<(String, Option<datafusion_expr::Expr>, TimeUnit, DFSchema), Error> {
    // TODO(discord9): find the expr that do time window

    let mut table_name = None;
    // first find the table source in the logical plan
    plan.apply(|plan| {
        let LogicalPlan::TableScan(table_scan) = plan else {
            return Ok(TreeNodeRecursion::Continue);
        };
        table_name = Some(table_scan.table_name.clone());
        Ok(TreeNodeRecursion::Stop)
    })
    .with_context(|_| DatafusionSnafu {
        context: format!("Can't find table source in plan {plan:?}"),
    })?;
    let Some(table_name) = table_name else {
        UnexpectedSnafu {
            reason: format!("Can't find table source in plan {plan:?}"),
        }
        .fail()?
    };

    let current_schema = query_ctx.current_schema();

    let catalog_name = table_name.catalog().unwrap_or(query_ctx.current_catalog());
    let schema_name = table_name.schema().unwrap_or(&current_schema);
    let table_name = table_name.table();

    let Some(table_ref) = catalog_man
        .table(catalog_name, schema_name, table_name, Some(&query_ctx))
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?
    else {
        UnexpectedSnafu {
            reason: format!(
                "Can't find table {table_name:?} in catalog {catalog_name:?}/{schema_name:?}"
            ),
        }
        .fail()?
    };

    let schema = &table_ref.table_info().meta.schema;

    let ts_index = schema.timestamp_column().context(UnexpectedSnafu {
        reason: format!("Can't find timestamp column in table {table_name:?}"),
    })?;

    let ts_col_name = ts_index.name.clone();

    let expected_time_unit = ts_index.data_type.as_timestamp().with_context(|| UnexpectedSnafu {
        reason: format!(
            "Expected timestamp column {ts_col_name:?} in table {table_name:?} to be timestamp, but got {ts_index:?}"
        ),
    })?.unit();

    let ts_columns: HashSet<_> = HashSet::from_iter(vec![
        format!("{catalog_name}.{schema_name}.{table_name}.{ts_col_name}"),
        format!("{schema_name}.{table_name}.{ts_col_name}"),
        format!("{table_name}.{ts_col_name}"),
        format!("{ts_col_name}"),
    ]);
    let ts_columns: HashSet<_> = ts_columns
        .into_iter()
        .map(Column::from_qualified_name)
        .collect();

    let ts_columns_ref: HashSet<&Column> = ts_columns.iter().collect();

    // find the time window expr which refers to the time index column
    let mut time_window_expr: Option<Expr> = None;
    let find_time_window_expr = |plan: &LogicalPlan| {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return Ok(TreeNodeRecursion::Continue);
        };

        for group_expr in &aggregate.group_expr {
            let refs = group_expr.column_refs();
            if refs.len() != 1 {
                continue;
            }
            let ref_col = refs.iter().next().unwrap();
            if ts_columns_ref.contains(ref_col) {
                time_window_expr = Some(group_expr.clone());
                break;
            }
        }

        Ok(TreeNodeRecursion::Stop)
    };
    plan.apply(find_time_window_expr)
        .with_context(|_| DatafusionSnafu {
            context: format!("Can't find time window expr in plan {plan:?}"),
        })?;

    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        ts_col_name.clone(),
        ts_index.data_type.as_arrow_type(),
        false,
    )]));

    let df_schema = DFSchema::from_field_specific_qualified_schema(
        vec![Some(TableReference::bare(table_name))],
        &arrow_schema,
    )
    .with_context(|_e| DatafusionSnafu {
        context: format!("Failed to create DFSchema from arrow schema {arrow_schema:?}"),
    })?;
    Ok((ts_col_name, time_window_expr, expected_time_unit, df_schema))
}

/// Find nearest lower bound for time `current` in given `plan` for the time window expr.
/// i.e. for time window expr being `date_bin(INTERVAL '5 minutes', ts) as time_window` and `current="2021-07-01 00:01:01.000"`,
/// return `Some("2021-07-01 00:00:00.000")`
/// if `plan` doesn't contain a `TIME INDEX` column, return `None`
///
/// Time window expr is a expr that:
/// 1. ref only to a time index column
/// 2. is monotonic increasing
/// 3. show up in GROUP BY clause
///
/// note this plan should only contain one TableScan
pub async fn find_plan_time_window_bound(
    plan: &LogicalPlan,
    current: Timestamp,
    query_ctx: QueryContextRef,
    engine: QueryEngineRef,
) -> Result<(String, Option<Timestamp>, Option<Timestamp>), Error> {
    // TODO(discord9): find the expr that do time window
    let catalog_man = engine.engine_state().catalog_manager();

    let (ts_col_name, time_window_expr, expected_time_unit, df_schema) =
        find_time_window_expr(plan, catalog_man.clone(), query_ctx).await?;

    // cast current to ts_index's type
    let new_current = current
        .convert_to(expected_time_unit)
        .with_context(|| UnexpectedSnafu {
            reason: format!("Failed to cast current timestamp {current:?} to {expected_time_unit}"),
        })?;

    // if no time_window_expr is found, return None
    if let Some(time_window_expr) = time_window_expr {
        let lower_bound =
            find_expr_time_window_lower_bound(&time_window_expr, &df_schema, new_current)?;
        let upper_bound =
            find_expr_time_window_upper_bound(&time_window_expr, &df_schema, new_current)?;
        Ok((ts_col_name, lower_bound, upper_bound))
    } else {
        Ok((ts_col_name, None, None))
    }
}

/// Find the lower bound of time window in given `expr` and `current` timestamp.
///
/// i.e. for `current="2021-07-01 00:01:01.000"` and `expr=date_bin(INTERVAL '5 minutes', ts) as time_window` and `ts_col=ts`,
/// return `Some("2021-07-01 00:00:00.000")` since it's the lower bound
/// of current time window given the current timestamp
///
/// if return None, meaning this time window have no lower bound
fn find_expr_time_window_lower_bound(
    expr: &Expr,
    df_schema: &DFSchema,
    current: Timestamp,
) -> Result<Option<Timestamp>, Error> {
    use std::cmp::Ordering;

    let phy_planner = DefaultPhysicalPlanner::default();

    let phy_expr: PhysicalExprRef = phy_planner
        .create_physical_expr(expr, df_schema, &SessionContext::new().state())
        .with_context(|_e| DatafusionSnafu {
            context: format!(
                "Failed to create physical expression from {expr:?} using {df_schema:?}"
            ),
        })?;

    let cur_time_window = eval_ts_to_ts(&phy_expr, df_schema, current)?;
    if cur_time_window == current {
        return Ok(Some(current));
    }

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

        let prev_time_window = eval_ts_to_ts(&phy_expr, df_schema, prev_time_probe)?;

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

    let input_time_unit = lower_bound
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
        let mid_probe = common_time::Timestamp::new(mid, input_time_unit);
        let mid_time_window = eval_ts_to_ts(&phy_expr, df_schema, mid_probe)?;

        match mid_time_window.cmp(&cur_time_window) {
            Ordering::Less => low = mid + 1,
            Ordering::Equal => high = mid,
            Ordering::Greater => UnexpectedSnafu {
                reason: format!("Binary search failed for time window expression {expr:?}"),
            }
            .fail()?,
        }
    }

    let final_lower_bound_for_time_window = common_time::Timestamp::new(low, input_time_unit);

    Ok(Some(final_lower_bound_for_time_window))
}

/// Find the upper bound for time window expression
fn find_expr_time_window_upper_bound(
    expr: &Expr,
    df_schema: &DFSchema,
    current: Timestamp,
) -> Result<Option<Timestamp>, Error> {
    use std::cmp::Ordering;

    let phy_planner = DefaultPhysicalPlanner::default();

    let phy_expr: PhysicalExprRef = phy_planner
        .create_physical_expr(expr, df_schema, &SessionContext::new().state())
        .with_context(|_e| DatafusionSnafu {
            context: format!(
                "Failed to create physical expression from {expr:?} using {df_schema:?}"
            ),
        })?;

    let cur_time_window = eval_ts_to_ts(&phy_expr, df_schema, current)?;

    // search to find the lower bound
    let mut offset: i64 = 1;
    let mut lower_bound = Some(current);
    let upper_bound;
    // first expontial probe to found a range for binary search
    loop {
        let Some(next_val) = current.value().checked_add(offset) else {
            // no upper bound if overflow
            return Ok(None);
        };

        let next_time_probe = common_time::Timestamp::new(next_val, current.unit());

        let next_time_window = eval_ts_to_ts(&phy_expr, df_schema, next_time_probe)?;

        match next_time_window.cmp(&cur_time_window) {
            Ordering::Less => {UnexpectedSnafu {
                reason: format!(
                    "Unsupported time window expression, expect monotonic increasing for time window expression {expr:?}"
                ),
            }
            .fail()?
            }
            Ordering::Equal => {
                lower_bound = Some(next_time_probe);
            }
            Ordering::Greater => {
                upper_bound = Some(next_time_probe);
                break
            }
        }

        let Some(new_offset) = offset.checked_mul(2) else {
            // no upper bound if overflow
            return Ok(None);
        };
        offset = new_offset;
    }

    // binary search for the exact upper bound

    ensure!(lower_bound.map(|v|v.unit())==upper_bound.map(|v|v.unit()), UnexpectedSnafu{
        reason: format!(" unit mismatch for time window expression {expr:?}, found {lower_bound:?} and {upper_bound:?}"),
    });

    let output_unit = upper_bound
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
        let mid_time_window = eval_ts_to_ts(&phy_expr, df_schema, mid_probe)?;

        match mid_time_window.cmp(&cur_time_window) {
            Ordering::Less => UnexpectedSnafu {
                reason: format!("Binary search failed for time window expression {expr:?}"),
            }
            .fail()?,
            Ordering::Equal => low = mid + 1,
            Ordering::Greater => high = mid,
        }
    }

    let final_upper_bound_for_time_window = common_time::Timestamp::new(high, output_unit);

    Ok(Some(final_upper_bound_for_time_window))
}

fn eval_ts_to_ts(
    phy: &PhysicalExprRef,
    df_schema: &DFSchema,
    input_value: Timestamp,
) -> Result<Timestamp, Error> {
    let ts_vector = match input_value.unit() {
        TimeUnit::Second => {
            TimestampSecondVector::from_vec(vec![input_value.value()]).to_arrow_array()
        }
        TimeUnit::Millisecond => {
            TimestampMillisecondVector::from_vec(vec![input_value.value()]).to_arrow_array()
        }
        TimeUnit::Microsecond => {
            TimestampMicrosecondVector::from_vec(vec![input_value.value()]).to_arrow_array()
        }
        TimeUnit::Nanosecond => {
            TimestampNanosecondVector::from_vec(vec![input_value.value()]).to_arrow_array()
        }
    };

    let rb = DfRecordBatch::try_new(df_schema.inner().clone(), vec![ts_vector.clone()])
        .with_context(|_| ArrowSnafu {
            context: format!("Failed to create record batch from {df_schema:?} and {ts_vector:?}"),
        })?;

    let eval_res = phy.evaluate(&rb).with_context(|_| DatafusionSnafu {
        context: format!("Failed to evaluate physical expression {phy:?} on {rb:?}"),
    })?;

    if let Some(Some(ts)) = columnar_to_ts_vector(&eval_res)?.first() {
        Ok(*ts)
    } else {
        UnexpectedSnafu {
            reason: format!(
                "Expected timestamp in expression {phy:?} but got {:?}",
                eval_res
            ),
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
    use pretty_assertions::assert_eq;
    use session::context::QueryContext;

    use super::{sql_to_df_plan, *};
    use crate::recording_rules::{df_plan_to_sql, AddFilterRewriter};
    use crate::test_utils::create_test_query_engine;

    #[tokio::test]
    async fn test_add_filter() {
        let testcases = vec![
            (
                "SELECT number FROM numbers_with_ts GROUP BY number","SELECT numbers_with_ts.number FROM numbers_with_ts WHERE (number > 4) GROUP BY numbers_with_ts.number"
            ),
            (
                "SELECT number FROM numbers_with_ts WHERE number < 2 OR number >10",
                "SELECT numbers_with_ts.number FROM numbers_with_ts WHERE (((numbers_with_ts.number < 2) OR (numbers_with_ts.number > 10)) AND (number > 4))"
            ),
            (
                "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window",
                "SELECT date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE (number > 4) GROUP BY date_bin('5 minutes', numbers_with_ts.ts)"
            )
        ];
        use datafusion_expr::{col, lit};
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();

        for (before, after) in testcases {
            let sql = before;
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();

            let mut add_filter = AddFilterRewriter::new(col("number").gt(lit(4u32)));
            let plan = plan.rewrite(&mut add_filter).unwrap().data;
            let new_sql = df_plan_to_sql(&plan).unwrap();
            assert_eq!(after, new_sql);
        }
    }

    #[tokio::test]
    async fn test_plan_time_window_lower_bound() {
        use datafusion_expr::{col, lit};
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();

        let testcases = [
            // same alias is not same column
            (
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS ts FROM numbers_with_ts GROUP BY ts;",
                Timestamp::new(1740394109, TimeUnit::Second),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(1740394109000, TimeUnit::Millisecond)),
                    Some(Timestamp::new(1740394109001, TimeUnit::Millisecond)),
                ),
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS ts FROM numbers_with_ts WHERE ((ts >= CAST('2025-02-24 10:48:29' AS TIMESTAMP)) AND (ts <= CAST('2025-02-24 10:48:29.001' AS TIMESTAMP))) GROUP BY numbers_with_ts.ts"
            ),
            // complex time window index
            (
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS time_window FROM numbers_with_ts GROUP BY time_window;",
                Timestamp::new(1740394109, TimeUnit::Second),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(1740394080, TimeUnit::Second)),
                    Some(Timestamp::new(1740394140, TimeUnit::Second)),
                ),
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS time_window FROM numbers_with_ts WHERE ((ts >= CAST('2025-02-24 10:48:00' AS TIMESTAMP)) AND (ts <= CAST('2025-02-24 10:49:00' AS TIMESTAMP))) GROUP BY arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)')"
            ),
            // no time index
            (
                "SELECT date_bin('5 minutes', ts) FROM numbers_with_ts;",
                Timestamp::new(23, TimeUnit::Millisecond),
                ("ts".to_string(), None, None),
                "SELECT date_bin('5 minutes', ts) FROM numbers_with_ts;"
            ),
            // time index
            (
                "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window;",
                Timestamp::new(23, TimeUnit::Nanosecond),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(0, TimeUnit::Millisecond)),
                    Some(Timestamp::new(300000, TimeUnit::Millisecond)),
                ),
                "SELECT date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE ((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts <= CAST('1970-01-01 00:05:00' AS TIMESTAMP))) GROUP BY date_bin('5 minutes', numbers_with_ts.ts)"
            ),
            // on spot
            (
                "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window;",
                Timestamp::new(0, TimeUnit::Nanosecond),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(0, TimeUnit::Millisecond)),
                    Some(Timestamp::new(300000, TimeUnit::Millisecond)),
                ),
                "SELECT date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE ((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts <= CAST('1970-01-01 00:05:00' AS TIMESTAMP))) GROUP BY date_bin('5 minutes', numbers_with_ts.ts)"
            ),
            // different time unit
            (
                "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window;",
                Timestamp::new(23_000_000, TimeUnit::Nanosecond),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(0, TimeUnit::Millisecond)),
                    Some(Timestamp::new(300000, TimeUnit::Millisecond)),
                ),
                "SELECT date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE ((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts <= CAST('1970-01-01 00:05:00' AS TIMESTAMP))) GROUP BY date_bin('5 minutes', numbers_with_ts.ts)"
            ),
            // time index with other fields
            (
                "SELECT sum(number) as sum_up, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window;",
                Timestamp::new(23, TimeUnit::Millisecond),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(0, TimeUnit::Millisecond)),
                    Some(Timestamp::new(300000, TimeUnit::Millisecond)),
                ),
                "SELECT sum(numbers_with_ts.number) AS sum_up, date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE ((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts <= CAST('1970-01-01 00:05:00' AS TIMESTAMP))) GROUP BY date_bin('5 minutes', numbers_with_ts.ts)"
            ),
            // time index with other pks
            (
                "SELECT number, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window, number;",
                Timestamp::new(23, TimeUnit::Millisecond),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(0, TimeUnit::Millisecond)),
                    Some(Timestamp::new(300000, TimeUnit::Millisecond)),
                ),
                "SELECT numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE ((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts <= CAST('1970-01-01 00:05:00' AS TIMESTAMP))) GROUP BY date_bin('5 minutes', numbers_with_ts.ts), numbers_with_ts.number"
            ),
        ];

        for (sql, current, expected, unparsed) in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, true)
                .await
                .unwrap();

            let real =
                find_plan_time_window_bound(&plan, current, ctx.clone(), query_engine.clone())
                    .await
                    .unwrap();
            assert_eq!(expected, real);

            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();
            let (col_name, lower, upper) = real;
            let new_sql = if lower.is_some() {
                let to_df_literal = |value| {
                    let value = Value::from(value);

                    value.try_to_scalar_value(&value.data_type()).unwrap()
                };
                let lower = to_df_literal(lower.unwrap());
                let upper = to_df_literal(upper.unwrap());
                let expr = col(&col_name)
                    .gt_eq(lit(lower))
                    .and(col(&col_name).lt_eq(lit(upper)));
                let mut add_filter = AddFilterRewriter::new(expr);
                let plan = plan.rewrite(&mut add_filter).unwrap().data;
                df_plan_to_sql(&plan).unwrap()
            } else {
                sql.to_string()
            };
            assert_eq!(unparsed, new_sql);
        }
    }
}
