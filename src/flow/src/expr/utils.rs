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

//! This module contains utility functions for expressions.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use datatypes::value::Value;
use snafu::{ensure, OptionExt};

use crate::error::UnexpectedSnafu;
use crate::expr::ScalarExpr;
use crate::plan::TypedPlan;
use crate::Result;

/// Find lower bound for time `current` in given `plan` for the time window expr.
///
/// i.e. for time window expr being `date_bin(INTERVAL '5 minutes', ts) as time_window` and `current="2021-07-01 00:01:01.000"`,
/// return `Some("2021-07-01 00:00:00.000")`
///
/// if `plan` doesn't contain a `TIME INDEX` column, return `None`
pub fn find_plan_time_window_expr_lower_bound(
    plan: &TypedPlan,
    current: common_time::Timestamp,
) -> Result<Option<common_time::Timestamp>> {
    let typ = plan.schema.typ();
    let Some(mut time_index) = typ.time_index else {
        return Ok(None);
    };

    let mut cur_plan = plan;
    let mut expr_time_index;

    loop {
        // follow upward and find deepest time index expr that is not a column ref
        expr_time_index = Some(cur_plan.plan.get_nth_expr(time_index).context(
            UnexpectedSnafu {
                reason: "Failed to find time index expr",
            },
        )?);

        if let Some(ScalarExpr::Column(i)) = expr_time_index {
            time_index = i;
        } else {
            break;
        }
        if let Some(input) = cur_plan.plan.get_first_input_plan() {
            cur_plan = input;
        } else {
            break;
        }
    }

    let expr_time_index = expr_time_index.context(UnexpectedSnafu {
        reason: "Failed to find time index expr",
    })?;

    let ts_col = expr_time_index
        .get_all_ref_columns()
        .first()
        .cloned()
        .context(UnexpectedSnafu {
            reason: "Failed to find time index column",
        })?;

    find_time_window_lower_bound(&expr_time_index, ts_col, current)
}

/// Find the lower bound of time window in given `expr` and `current` timestamp.
///
/// i.e. for `current="2021-07-01 00:01:01.000"` and `expr=date_bin(INTERVAL '5 minutes', ts) as time_window` and `ts_col=ts`,
/// return `Some("2021-07-01 00:00:00.000")` since it's the lower bound
/// of current time window given the current timestamp
///
/// if return None, meaning this time window have no lower bound
pub fn find_time_window_lower_bound(
    expr: &ScalarExpr,
    ts_col_idx: usize,
    current: common_time::Timestamp,
) -> Result<Option<common_time::Timestamp>> {
    let all_ref_columns = expr.get_all_ref_columns();

    ensure!(
        all_ref_columns.contains(&ts_col_idx),
        UnexpectedSnafu {
            reason: format!(
                "Expected column {} to be referenced in expression {expr:?}",
                ts_col_idx
            ),
        }
    );

    ensure!(all_ref_columns.len() == 1, UnexpectedSnafu {
        reason: format!(
            "Expect only one column to be referenced in expression {expr:?}, found {all_ref_columns:?}"
        ),
    });

    let permute_map = BTreeMap::from([(ts_col_idx, 0usize)]);

    let mut rewrote_expr = expr.clone();

    rewrote_expr.permute_map(&permute_map)?;

    fn eval_to_timestamp(expr: &ScalarExpr, values: &[Value]) -> Result<common_time::Timestamp> {
        let val = expr.eval(values)?;
        if let Value::Timestamp(ts) = val {
            Ok(ts)
        } else {
            UnexpectedSnafu {
                reason: format!("Expected timestamp in expression {expr:?} but got {val:?}"),
            }
            .fail()?
        }
    }

    let cur_time_window = eval_to_timestamp(&rewrote_expr, &[current.into()])?;

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

        let prev_time_window = eval_to_timestamp(&rewrote_expr, &[prev_time_probe.into()])?;

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
                        "Unsupported time window expression {rewrote_expr:?}, expect monotonic increasing for time window expression {expr:?}"
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

    // binary search for the lower bound

    ensure!(lower_bound.map(|v|v.unit())==upper_bound.map(|v|v.unit()), UnexpectedSnafu{
        reason: format!(" unit mismatch for time window expression {expr:?}, found {lower_bound:?} and {upper_bound:?}"),
    });

    let output_unit = lower_bound.expect("should have lower bound").unit();

    let mut low = lower_bound.expect("should have lower bound").value();
    let mut high = upper_bound.expect("should have upper bound").value();
    while low < high {
        let mid = (low + high) / 2;
        let mid_probe = common_time::Timestamp::new(mid, output_unit);
        let mid_time_window = eval_to_timestamp(&rewrote_expr, &[mid_probe.into()])?;

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

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::plan::{Plan, TypedPlan};
    use crate::test_utils::{create_test_ctx, create_test_query_engine, sql_to_substrait};

    #[tokio::test]
    async fn test_plan_time_window_lower_bound() {
        let testcases = [
            // no time index
            (
                "SELECT date_bin('5 minutes', ts) FROM numbers_with_ts;",
                "2021-07-01 00:01:01.000",
                None,
            ),
            // time index
            (
                "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window;",
                "2021-07-01 00:01:01.000",
                Some("2021-07-01 00:00:00.000"),
            ),
            // time index with other fields
            (
                "SELECT sum(number) as sum_up, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window;",
                "2021-07-01 00:01:01.000",
                Some("2021-07-01 00:00:00.000"),
            ),
            // time index with other pks
            (
                "SELECT number, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window, number;",
                "2021-07-01 00:01:01.000",
                Some("2021-07-01 00:00:00.000"),
            ),
        ];
        let engine = create_test_query_engine();

        for (sql, current, expected) in &testcases {
            let plan = sql_to_substrait(engine.clone(), sql).await;

            let mut ctx = create_test_ctx();
            let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
                .await
                .unwrap();

            let current = common_time::Timestamp::from_str(current, None).unwrap();

            let expected =
                expected.map(|expected| common_time::Timestamp::from_str(expected, None).unwrap());

            assert_eq!(
                find_plan_time_window_expr_lower_bound(&flow_plan, current).unwrap(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn test_timewindow_lower_bound() {
        let testcases = [
            (
                ("'5 minutes'", "ts", Some("2021-07-01 00:00:00.000")),
                "2021-07-01 00:01:01.000",
                "2021-07-01 00:00:00.000",
            ),
            (
                ("'5 minutes'", "ts", None),
                "2021-07-01 00:01:01.000",
                "2021-07-01 00:00:00.000",
            ),
            (
                ("'5 minutes'", "ts", None),
                "2021-07-01 00:00:00.000",
                "2021-07-01 00:00:00.000",
            ),
            // test edge cases
            (
                ("'5 minutes'", "ts", None),
                "2021-07-01 00:05:00.000",
                "2021-07-01 00:05:00.000",
            ),
            (
                ("'5 minutes'", "ts", None),
                "2021-07-01 00:04:59.999",
                "2021-07-01 00:00:00.000",
            ),
            (
                ("'5 minutes'", "ts", None),
                "2021-07-01 00:04:59.999999999",
                "2021-07-01 00:00:00.000",
            ),
            (
                ("'5 minutes'", "ts", None),
                "2021-07-01 00:04:59.999999999999",
                "2021-07-01 00:00:00.000",
            ),
            (
                ("'5 minutes'", "ts", None),
                "2021-07-01 00:04:59.999999999999999",
                "2021-07-01 00:00:00.000",
            ),
        ];
        let engine = create_test_query_engine();

        for (args, current, expected) in testcases {
            let sql = if let Some(origin) = args.2 {
                format!(
                    "SELECT date_bin({}, {}, '{origin}') FROM numbers_with_ts;",
                    args.0, args.1
                )
            } else {
                format!(
                    "SELECT date_bin({}, {}) FROM numbers_with_ts;",
                    args.0, args.1
                )
            };
            let plan = sql_to_substrait(engine.clone(), &sql).await;
            let mut ctx = create_test_ctx();
            let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
                .await
                .unwrap();

            let expr = {
                let mfp = flow_plan.plan;
                let Plan::Mfp { mfp, .. } = mfp else {
                    unreachable!()
                };
                mfp.expressions[0].clone()
            };

            let current = common_time::Timestamp::from_str(current, None).unwrap();

            let res = find_time_window_lower_bound(&expr, 1, current).unwrap();

            let expected = Some(common_time::Timestamp::from_str(expected, None).unwrap());

            assert_eq!(res, expected);
        }
    }
}
