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

//! Time window expr and helper functions
//!

use std::collections::BTreeSet;
use std::sync::Arc;

use api::helper::pb_value_to_value_ref;
use arrow::array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_recordbatch::DfRecordBatch;
use common_telemetry::warn;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion::error::Result as DfResult;
use datafusion::execution::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion_common::{DFSchema, TableReference};
use datafusion_expr::{ColumnarValue, LogicalPlan};
use datafusion_physical_expr::PhysicalExprRef;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::TIME_INDEX_KEY;
use datatypes::value::Value;
use datatypes::vectors::{
    TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
    TimestampSecondVector, Vector,
};
use itertools::Itertools;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};

use crate::adapter::util::from_proto_to_data_type;
use crate::error::{
    ArrowSnafu, DatafusionSnafu, DatatypesSnafu, ExternalSnafu, PlanSnafu, UnexpectedSnafu,
};
use crate::expr::error::DataTypeSnafu;
use crate::Error;

/// Time window expr like `date_bin(INTERVAL '1' MINUTE, ts)`, this type help with
/// evaluating the expr using given timestamp
///
/// The time window expr must satisfies following conditions:
/// 1. The expr must be monotonic non-decreasing
/// 2. The expr must only have one and only one input column with timestamp type, and the output column must be timestamp type
/// 3. The expr must be deterministic
///
/// An example of time window expr is `date_bin(INTERVAL '1' MINUTE, ts)`
#[derive(Debug, Clone)]
pub struct TimeWindowExpr {
    phy_expr: PhysicalExprRef,
    pub column_name: String,
    logical_expr: Expr,
    df_schema: DFSchema,
}

impl std::fmt::Display for TimeWindowExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeWindowExpr")
            .field("phy_expr", &self.phy_expr.to_string())
            .field("column_name", &self.column_name)
            .field("logical_expr", &self.logical_expr.to_string())
            .field("df_schema", &self.df_schema)
            .finish()
    }
}

impl TimeWindowExpr {
    pub fn from_expr(
        expr: &Expr,
        column_name: &str,
        df_schema: &DFSchema,
        session: &SessionState,
    ) -> Result<Self, Error> {
        let phy_expr: PhysicalExprRef = to_phy_expr(expr, df_schema, session)?;
        Ok(Self {
            phy_expr,
            column_name: column_name.to_string(),
            logical_expr: expr.clone(),
            df_schema: df_schema.clone(),
        })
    }

    pub fn eval(
        &self,
        current: Timestamp,
    ) -> Result<(Option<Timestamp>, Option<Timestamp>), Error> {
        let lower_bound =
            calc_expr_time_window_lower_bound(&self.phy_expr, &self.df_schema, current)?;
        let upper_bound =
            probe_expr_time_window_upper_bound(&self.phy_expr, &self.df_schema, current)?;
        Ok((lower_bound, upper_bound))
    }

    /// Find timestamps from rows using time window expr
    ///
    /// use column of name `self.column_name` from input rows list as input to time window expr
    pub async fn handle_rows(
        &self,
        rows_list: Vec<api::v1::Rows>,
    ) -> Result<BTreeSet<Timestamp>, Error> {
        let mut time_windows = BTreeSet::new();

        for rows in rows_list {
            // pick the time index column and use it to eval on `self.expr`
            // TODO(discord9): handle case where time index column is not present(i.e. DEFAULT constant value)
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

            let mut vector = cdt.create_mutable_vector(rows.rows.len());
            for row in rows.rows {
                let value = pb_value_to_value_ref(&row.values[ts_col_index], &None);
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

            let eval_res = self
                .phy_expr
                .evaluate(&rb)
                .with_context(|_| DatafusionSnafu {
                    context: format!(
                        "Failed to evaluate physical expression {:?} on {rb:?}",
                        self.phy_expr
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
                TimeUnit::Second => array
                    .as_ref()
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .with_context(|| PlanSnafu {
                        reason: format!("Failed to create vector from arrow array {array:?}"),
                    })?
                    .values()
                    .iter()
                    .map(|d| Some(Timestamp::new(*d, time_unit)))
                    .collect_vec(),
                TimeUnit::Millisecond => array
                    .as_ref()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .with_context(|| PlanSnafu {
                        reason: format!("Failed to create vector from arrow array {array:?}"),
                    })?
                    .values()
                    .iter()
                    .map(|d| Some(Timestamp::new(*d, time_unit)))
                    .collect_vec(),
                TimeUnit::Microsecond => array
                    .as_ref()
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .with_context(|| PlanSnafu {
                        reason: format!("Failed to create vector from arrow array {array:?}"),
                    })?
                    .values()
                    .iter()
                    .map(|d| Some(Timestamp::new(*d, time_unit)))
                    .collect_vec(),
                TimeUnit::Nanosecond => array
                    .as_ref()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .with_context(|| PlanSnafu {
                        reason: format!("Failed to create vector from arrow array {array:?}"),
                    })?
                    .values()
                    .iter()
                    .map(|d| Some(Timestamp::new(*d, time_unit)))
                    .collect_vec(),
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

/// Return (`the column name of time index column`, `the time window expr`, `the expected time unit of time index column`, `the expr's schema for evaluating the time window`)
///
/// The time window expr is expected to have one input column with Timestamp type, and also return Timestamp type, the time window expr is expected
/// to be monotonic increasing and appears in the innermost GROUP BY clause
///
/// note this plan should only contain one TableScan
pub async fn find_time_window_expr(
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

    let ts_index = schema.timestamp_column().with_context(|| UnexpectedSnafu {
        reason: format!("Can't find timestamp column in table {table_name:?}"),
    })?;

    let ts_col_name = ts_index.name.clone();

    let expected_time_unit = ts_index.data_type.as_timestamp().with_context(|| UnexpectedSnafu {
        reason: format!(
            "Expected timestamp column {ts_col_name:?} in table {table_name:?} to be timestamp, but got {ts_index:?}"
        ),
    })?.unit();

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

    // find the time window expr which refers to the time index column
    let mut aggr_expr = None;
    let mut time_window_expr: Option<Expr> = None;

    let find_inner_aggr_expr = |plan: &LogicalPlan| {
        if let LogicalPlan::Aggregate(aggregate) = plan {
            aggr_expr = Some(aggregate.clone());
        };

        Ok(TreeNodeRecursion::Continue)
    };
    plan.apply(find_inner_aggr_expr)
        .with_context(|_| DatafusionSnafu {
            context: format!("Can't find aggr expr in plan {plan:?}"),
        })?;

    if let Some(aggregate) = aggr_expr {
        for group_expr in &aggregate.group_expr {
            let refs = group_expr.column_refs();
            if refs.len() != 1 {
                continue;
            }
            let ref_col = refs.iter().next().unwrap();

            let index = aggregate.input.schema().maybe_index_of_column(ref_col);
            let Some(index) = index else {
                continue;
            };
            let field = aggregate.input.schema().field(index);

            // TODO(discord9): need to ensure the field has the meta key for the time index
            let is_time_index =
                field.metadata().get(TIME_INDEX_KEY).map(|s| s.as_str()) == Some("true");

            if is_time_index {
                let rewrite_column = group_expr.clone();
                let rewritten = rewrite_column
                    .rewrite(&mut RewriteColumn {
                        table_name: table_name.to_string(),
                    })
                    .with_context(|_| DatafusionSnafu {
                        context: format!("Rewrite expr failed, expr={:?}", group_expr),
                    })?
                    .data;
                struct RewriteColumn {
                    table_name: String,
                }

                impl TreeNodeRewriter for RewriteColumn {
                    type Node = Expr;
                    fn f_down(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
                        let Expr::Column(mut column) = node else {
                            return Ok(Transformed::no(node));
                        };

                        column.relation = Some(TableReference::bare(self.table_name.clone()));

                        Ok(Transformed::yes(Expr::Column(column)))
                    }
                }

                time_window_expr = Some(rewritten);
                break;
            }
        }
        Ok((ts_col_name, time_window_expr, expected_time_unit, df_schema))
    } else {
        // can't found time window expr, return None
        Ok((ts_col_name, None, expected_time_unit, df_schema))
    }
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
#[cfg(test)]
pub async fn find_plan_time_window_bound(
    plan: &LogicalPlan,
    current: Timestamp,
    query_ctx: QueryContextRef,
    engine: query::QueryEngineRef,
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
        let phy_expr = to_phy_expr(
            &time_window_expr,
            &df_schema,
            &engine.engine_state().session_state(),
        )?;
        let lower_bound = calc_expr_time_window_lower_bound(&phy_expr, &df_schema, new_current)?;
        let upper_bound = probe_expr_time_window_upper_bound(&phy_expr, &df_schema, new_current)?;
        Ok((ts_col_name, lower_bound, upper_bound))
    } else {
        Ok((ts_col_name, None, None))
    }
}

/// Find the lower bound of time window in given `expr` and `current` timestamp.
///
/// i.e. for `current="2021-07-01 00:01:01.000"` and `expr=date_bin(INTERVAL '5 minutes', ts) as time_window` and `ts_col=ts`,
/// return `Some("2021-07-01 00:00:00.000")` since it's the lower bound
/// return `Some("2021-07-01 00:00:00.000")` since it's the lower bound
/// of current time window given the current timestamp
///
/// if return None, meaning this time window have no lower bound
fn calc_expr_time_window_lower_bound(
    phy_expr: &PhysicalExprRef,
    df_schema: &DFSchema,
    current: Timestamp,
) -> Result<Option<Timestamp>, Error> {
    let cur_time_window = eval_phy_time_window_expr(phy_expr, df_schema, current)?;
    let input_time_unit = cur_time_window.unit();
    Ok(cur_time_window.convert_to(input_time_unit))
}

/// Probe for the upper bound for time window expression
fn probe_expr_time_window_upper_bound(
    phy_expr: &PhysicalExprRef,
    df_schema: &DFSchema,
    current: Timestamp,
) -> Result<Option<Timestamp>, Error> {
    // TODO(discord9): special handling `date_bin` for faster path
    use std::cmp::Ordering;

    let cur_time_window = eval_phy_time_window_expr(phy_expr, df_schema, current)?;

    // search to find the lower bound
    let mut offset: i64 = 1;
    let mut lower_bound = Some(current);
    let upper_bound;
    // first expontial probe to found a range for binary search
    loop {
        let Some(next_val) = current.value().checked_add(offset) else {
            // no upper bound if overflow, which is ok
            return Ok(None);
        };

        let next_time_probe = common_time::Timestamp::new(next_val, current.unit());

        let next_time_window = eval_phy_time_window_expr(phy_expr, df_schema, next_time_probe)?;

        match next_time_window.cmp(&cur_time_window) {
            Ordering::Less => UnexpectedSnafu {
                    reason: format!(
                        "Unsupported time window expression, expect monotonic increasing for time window expression {phy_expr:?}"
                    ),
                }
                .fail()?,
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

    binary_search_expr(
        lower_bound,
        upper_bound,
        cur_time_window,
        phy_expr,
        df_schema,
    )
    .map(Some)
}

fn binary_search_expr(
    lower_bound: Option<Timestamp>,
    upper_bound: Option<Timestamp>,
    cur_time_window: Timestamp,
    phy_expr: &PhysicalExprRef,
    df_schema: &DFSchema,
) -> Result<Timestamp, Error> {
    ensure!(lower_bound.map(|v|v.unit()) == upper_bound.map(|v| v.unit()), UnexpectedSnafu {
        reason: format!(" unit mismatch for time window expression {phy_expr:?}, found {lower_bound:?} and {upper_bound:?}"),
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
        let mid_time_window = eval_phy_time_window_expr(phy_expr, df_schema, mid_probe)?;

        match mid_time_window.cmp(&cur_time_window) {
            std::cmp::Ordering::Less => UnexpectedSnafu {
                reason: format!("Binary search failed for time window expression {phy_expr:?}"),
            }
            .fail()?,
            std::cmp::Ordering::Equal => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
        }
    }

    let final_upper_bound_for_time_window = common_time::Timestamp::new(high, output_unit);
    Ok(final_upper_bound_for_time_window)
}

/// Expect the `phy` expression only have one input column with Timestamp type, and also return Timestamp type
fn eval_phy_time_window_expr(
    phy: &PhysicalExprRef,
    df_schema: &DFSchema,
    input_value: Timestamp,
) -> Result<Timestamp, Error> {
    let schema_ty = df_schema.field(0).data_type();
    let schema_cdt = ConcreteDataType::from_arrow_type(schema_ty);
    let schema_unit = if let ConcreteDataType::Timestamp(ts) = schema_cdt {
        ts.unit()
    } else {
        return UnexpectedSnafu {
            reason: format!("Expect Timestamp, found {:?}", schema_cdt),
        }
        .fail();
    };
    let input_value = input_value
        .convert_to(schema_unit)
        .with_context(|| UnexpectedSnafu {
            reason: format!("Failed to convert timestamp {input_value:?} to {schema_unit}"),
        })?;
    let ts_vector = match schema_unit {
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

fn to_phy_expr(
    expr: &Expr,
    df_schema: &DFSchema,
    session: &SessionState,
) -> Result<PhysicalExprRef, Error> {
    let phy_planner = DefaultPhysicalPlanner::default();

    let phy_expr: PhysicalExprRef = phy_planner
        .create_physical_expr(expr, df_schema, session)
        .with_context(|_e| DatafusionSnafu {
            context: format!(
                "Failed to create physical expression from {expr:?} using {df_schema:?}"
            ),
        })?;
    Ok(phy_expr)
}

#[cfg(test)]
mod test {
    use datafusion_common::tree_node::TreeNode;
    use pretty_assertions::assert_eq;
    use session::context::QueryContext;

    use super::*;
    use crate::batching_mode::utils::{df_plan_to_sql, sql_to_df_plan, AddFilterRewriter};
    use crate::test_utils::create_test_query_engine;

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
                r#"SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS ts FROM numbers_with_ts WHERE ((ts >= CAST('2025-02-24 10:48:29' AS TIMESTAMP)) AND (ts <= CAST('2025-02-24 10:48:29.001' AS TIMESTAMP))) GROUP BY numbers_with_ts.ts"#
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
            // complex time window index with where
            (
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS time_window FROM numbers_with_ts WHERE number in (2, 3, 4) GROUP BY time_window;",
                Timestamp::new(1740394109, TimeUnit::Second),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(1740394080, TimeUnit::Second)),
                    Some(Timestamp::new(1740394140, TimeUnit::Second)),
                ),
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS time_window FROM numbers_with_ts WHERE numbers_with_ts.number IN (2, 3, 4) AND ((ts >= CAST('2025-02-24 10:48:00' AS TIMESTAMP)) AND (ts <= CAST('2025-02-24 10:49:00' AS TIMESTAMP))) GROUP BY arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)')"
            ),
            // complex time window index with between and
            (
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS time_window FROM numbers_with_ts WHERE number BETWEEN 2 AND 4 GROUP BY time_window;",
                Timestamp::new(1740394109, TimeUnit::Second),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(1740394080, TimeUnit::Second)),
                    Some(Timestamp::new(1740394140, TimeUnit::Second)),
                ),
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS time_window FROM numbers_with_ts WHERE (numbers_with_ts.number BETWEEN 2 AND 4) AND ((ts >= CAST('2025-02-24 10:48:00' AS TIMESTAMP)) AND (ts <= CAST('2025-02-24 10:49:00' AS TIMESTAMP))) GROUP BY arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)')"
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
            // subquery
            (
                "SELECT number, time_window FROM (SELECT number, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window, number);",
                Timestamp::new(23, TimeUnit::Millisecond),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(0, TimeUnit::Millisecond)),
                    Some(Timestamp::new(300000, TimeUnit::Millisecond)),
                ),
                "SELECT numbers_with_ts.number, time_window FROM (SELECT numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE ((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts <= CAST('1970-01-01 00:05:00' AS TIMESTAMP))) GROUP BY date_bin('5 minutes', numbers_with_ts.ts), numbers_with_ts.number)"
            ),
            // cte
            (
                "with cte as (select number, date_bin('5 minutes', ts) as time_window from numbers_with_ts GROUP BY time_window, number) select number, time_window from cte;",
                Timestamp::new(23, TimeUnit::Millisecond),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(0, TimeUnit::Millisecond)),
                    Some(Timestamp::new(300000, TimeUnit::Millisecond)),
                ),
                "SELECT cte.number, cte.time_window FROM (SELECT numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE ((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts <= CAST('1970-01-01 00:05:00' AS TIMESTAMP))) GROUP BY date_bin('5 minutes', numbers_with_ts.ts), numbers_with_ts.number) AS cte"
            ),
            // complex subquery without alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) GROUP BY number, time_window, bucket_name;",
                Timestamp::new(23, TimeUnit::Millisecond),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(0, TimeUnit::Millisecond)),
                    Some(Timestamp::new(300000, TimeUnit::Millisecond)),
                ),
                "SELECT sum(numbers_with_ts.number), numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window, bucket_name FROM (SELECT numbers_with_ts.number, numbers_with_ts.ts, CASE WHEN (numbers_with_ts.number < 5) THEN 'bucket_0_5' WHEN (numbers_with_ts.number >= 5) THEN 'bucket_5_inf' END AS bucket_name FROM numbers_with_ts WHERE ((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts <= CAST('1970-01-01 00:05:00' AS TIMESTAMP)))) GROUP BY numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts), bucket_name"
            ),
            // complex subquery alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) as cte GROUP BY number, time_window, bucket_name;",
                Timestamp::new(23, TimeUnit::Millisecond),
                (
                    "ts".to_string(),
                    Some(Timestamp::new(0, TimeUnit::Millisecond)),
                    Some(Timestamp::new(300000, TimeUnit::Millisecond)),
                ),
                "SELECT sum(cte.number), cte.number, date_bin('5 minutes', cte.ts) AS time_window, cte.bucket_name FROM (SELECT numbers_with_ts.number, numbers_with_ts.ts, CASE WHEN (numbers_with_ts.number < 5) THEN 'bucket_0_5' WHEN (numbers_with_ts.number >= 5) THEN 'bucket_5_inf' END AS bucket_name FROM numbers_with_ts WHERE ((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts <= CAST('1970-01-01 00:05:00' AS TIMESTAMP)))) AS cte GROUP BY cte.number, date_bin('5 minutes', cte.ts), cte.bucket_name"
            ),
        ];

        for (sql, current, expected, expected_unparsed) in testcases {
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
            assert_eq!(expected_unparsed, new_sql);
        }
    }
}
