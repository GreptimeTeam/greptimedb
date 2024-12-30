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

use crate::error::{Error, UnexpectedSnafu};
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
                return UnexpectedSnafu {
                    reason: "No time index found in real schema".to_string(),
                }
                .fail()?
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
    use super::*;
}
