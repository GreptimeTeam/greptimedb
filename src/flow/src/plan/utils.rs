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

use datatypes::schema::ColumnSchema;
use snafu::OptionExt;

use crate::error::{Error, InvalidQuerySnafu, UnexpectedSnafu};
use crate::plan::{ScalarExpr, TypedPlan};

/// Fix the time index of a flow plan to match the real schema.
///
/// This function will traverse the plan to find the deepest time index expr, and set that expr as time index,
/// so that internal `EXPIRE AFTER` mechanism can work properly.
pub fn fix_time_index_for_flow_plan(
    flow_plan: &TypedPlan,
    real_schema: &[ColumnSchema],
) -> Result<TypedPlan, Error> {
    let outer_time_index = {
        let mut outer_time_index = None;
        for (idx, real_col) in real_schema.iter().enumerate() {
            if real_col.is_time_index() {
                outer_time_index = Some(idx);
                break;
            }
        }

        match outer_time_index {
            Some(outer) => outer,
            None => {
                // meaning auto created table, return early
                return Ok(flow_plan.clone());
            }
        }
    };
    let mut rewrite_plan = flow_plan.clone();

    let mut cur_plan = &mut rewrite_plan;
    let mut time_index = outer_time_index;
    let mut expr_time_index;
    loop {
        // follow upward and find deepest time index expr that is not a column ref, and set time index to it.
        if let Some(ty) = cur_plan.schema.typ.column_types.get(time_index)
            && ty.scalar_type.is_timestamp()
        {
            let is_key_or_ti = cur_plan
                .schema
                .typ
                .keys
                .iter()
                .any(|k| k.column_indices.contains(&time_index))
                || cur_plan.schema.typ.time_index == Some(time_index);
            if !is_key_or_ti {
                InvalidQuerySnafu {
                    reason: format!(
                        "The time index column in the sink table is not a key column in the flow's SQL. It is expected to be a key column (i.e. included in the `GROUP BY` clause). The column's name in the flow is {:?}",
                        flow_plan.schema.names.get(outer_time_index)
                    ),
                }
                .fail()?
            }
            cur_plan.schema.typ = cur_plan
                .schema
                .typ
                .clone()
                .with_time_index(Some(time_index));
        } else {
            UnexpectedSnafu {
                reason: format!(
                    "Time index column type mismatch, expect timestamp got {:?}",
                    cur_plan.schema.typ.column_types.get(time_index)
                ),
            }
            .fail()?
        }

        expr_time_index = Some(cur_plan.plan.get_nth_expr(time_index).context(
            UnexpectedSnafu {
                reason: "Failed to find time index expr",
            },
        )?);
        let inner_time_index = if let Some(ScalarExpr::Column(i)) = expr_time_index {
            i
        } else {
            break;
        };
        let inner_plan = if let Some(input) = cur_plan.plan.get_mut_first_input_plan() {
            input
        } else {
            break;
        };

        time_index = inner_time_index;
        cur_plan = inner_plan;
    }

    Ok(rewrite_plan)
}

#[cfg(test)]
mod test {
    use datatypes::data_type::ConcreteDataType;

    use super::*;
    use crate::repr::RelationDesc;
    use crate::test_utils::{create_test_ctx, create_test_query_engine, sql_to_substrait};

    fn find_all_nested_relation(plan: &TypedPlan) -> Vec<RelationDesc> {
        let mut types = vec![];
        let mut cur_plan = plan;
        loop {
            types.push(cur_plan.schema.clone());
            if let Some(input) = cur_plan.plan.get_first_input_plan() {
                cur_plan = input;
            } else {
                break;
            }
        }

        types
    }

    /// Test if sink table's time index is not flow's key column in flow's plan
    /// should cause error with a friendly error message.
    #[tokio::test]
    async fn test_handle_non_key_ts_column() {
        let sql = "SELECT count(ts)::timestamp as ts_cnt, date_bin('10 minutes', ts) as tw2 FROM numbers_with_ts GROUP BY tw2";

        let engine = create_test_query_engine();
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let real_schema = vec![
            ColumnSchema::new(
                "tw1",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "tw2",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        assert_eq!(
            find_all_nested_relation(&flow_plan)[1].typ().time_index,
            Some(0)
        );
        let fixed_plan = fix_time_index_for_flow_plan(&flow_plan, &real_schema);

        assert!(fixed_plan.unwrap_err().to_string().contains(
            r#"The time index column in the sink table is not a key column in the flow's SQL. It is expected to be a key column (i.e. included in the `GROUP BY` clause). The column's name in the flow is Some(Some("ts_cnt"))"#
        ));
    }

    // TODO(discord9): also test non-key timestamp column
    #[tokio::test]
    async fn test_two_ts_column() {
        let sql = "SELECT date_bin('5 minutes', ts) as tw1, date_bin('10 minutes', ts) as tw2 FROM numbers_with_ts GROUP BY tw1, tw2";

        let engine = create_test_query_engine();
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let real_schema = vec![
            ColumnSchema::new(
                "tw1",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new(
                "tw2",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];
        assert_eq!(
            find_all_nested_relation(&flow_plan)[1].typ().time_index,
            Some(0)
        );
        let fixed_plan = fix_time_index_for_flow_plan(&flow_plan, &real_schema).unwrap();
        assert_eq!(
            find_all_nested_relation(&fixed_plan)[1].typ().time_index,
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_normal_ts_case_column() {
        let sql = "SELECT count(number), date_bin('10 minutes', ts) as tw2 FROM numbers_with_ts GROUP BY tw2";

        let engine = create_test_query_engine();
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let real_schema = vec![
            ColumnSchema::new("number", ConcreteDataType::uint64_datatype(), false),
            ColumnSchema::new(
                "tw2",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];
        assert_eq!(
            find_all_nested_relation(&flow_plan)[1].typ().time_index,
            Some(0)
        );
        let fixed_plan = fix_time_index_for_flow_plan(&flow_plan, &real_schema).unwrap();
        assert_eq!(
            find_all_nested_relation(&fixed_plan)[1].typ().time_index,
            Some(0)
        );
    }
}
