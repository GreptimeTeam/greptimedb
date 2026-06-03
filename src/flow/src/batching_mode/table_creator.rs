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

use api::v1::CreateTableExpr;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use operator::expr_helper::column_schemas_to_defs;
use snafu::ResultExt;

use crate::Error;
use crate::adapter::{AUTO_CREATED_PLACEHOLDER_TS_COL, AUTO_CREATED_UPDATE_AT_TS_COL};
use crate::batching_mode::utils::FindGroupByFinalName;
use crate::error::{ConvertColumnSchemaSnafu, DatafusionSnafu};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryType {
    /// query is a tql query
    Tql,
    /// query is a sql query
    Sql,
}

// auto created table have a auto added column `update_at`, and optional have a `AUTO_CREATED_PLACEHOLDER_TS_COL` column for time index placeholder if no timestamp column is specified
// TODO(discord9): for now no default value is set for auto added column for compatibility reason with streaming mode, but this might change in favor of simpler code?
pub(super) fn create_table_with_expr(
    plan: &LogicalPlan,
    sink_table_name: &[String; 3],
    query_type: &QueryType,
) -> Result<CreateTableExpr, Error> {
    let table_def = match query_type {
        &QueryType::Sql => {
            if let Some(def) = build_pk_from_aggr(plan)? {
                def
            } else {
                build_by_sql_schema(plan)?
            }
        }
        QueryType::Tql => {
            // first try build from aggr, then from tql schema because tql query might not have aggr node
            if let Some(table_def) = build_pk_from_aggr(plan)? {
                table_def
            } else {
                build_by_tql_schema(plan)?
            }
        }
    };
    let first_time_stamp = table_def.ts_col;
    let primary_keys = table_def.pks;

    let mut column_schemas = Vec::new();
    for field in plan.schema().fields() {
        let name = field.name();
        let ty = ConcreteDataType::from_arrow_type(field.data_type());
        let col_schema = if first_time_stamp == Some(name.clone()) {
            ColumnSchema::new(name, ty, false).with_time_index(true)
        } else {
            ColumnSchema::new(name, ty, true)
        };

        match query_type {
            QueryType::Sql => {
                column_schemas.push(col_schema);
            }
            QueryType::Tql => {
                // if is val column, need to rename as val DOUBLE NULL
                // if is tag column, need to cast type as STRING NULL
                let is_tag_column = primary_keys.contains(name);
                let is_val_column = !is_tag_column && first_time_stamp.as_ref() != Some(name);
                if is_val_column {
                    let col_schema =
                        ColumnSchema::new(name, ConcreteDataType::float64_datatype(), true);
                    column_schemas.push(col_schema);
                } else if is_tag_column {
                    let col_schema =
                        ColumnSchema::new(name, ConcreteDataType::string_datatype(), true);
                    column_schemas.push(col_schema);
                } else {
                    // time index column
                    column_schemas.push(col_schema);
                }
            }
        }
    }

    if query_type == &QueryType::Sql {
        let update_at_schema = ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        );
        column_schemas.push(update_at_schema);
    }

    let time_index = if let Some(time_index) = first_time_stamp {
        time_index
    } else {
        column_schemas.push(
            ColumnSchema::new(
                AUTO_CREATED_PLACEHOLDER_TS_COL,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        AUTO_CREATED_PLACEHOLDER_TS_COL.to_string()
    };

    let column_defs =
        column_schemas_to_defs(column_schemas, &primary_keys).context(ConvertColumnSchemaSnafu)?;
    Ok(CreateTableExpr {
        catalog_name: sink_table_name[0].clone(),
        schema_name: sink_table_name[1].clone(),
        table_name: sink_table_name[2].clone(),
        desc: "Auto created table by flow engine".to_string(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: None,
        engine: "mito".to_string(),
    })
}

/// simply build by schema, return first timestamp column and no primary key
fn build_by_sql_schema(plan: &LogicalPlan) -> Result<TableDef, Error> {
    let first_time_stamp = plan.schema().fields().iter().find_map(|f| {
        if ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp() {
            Some(f.name().clone())
        } else {
            None
        }
    });
    Ok(TableDef {
        ts_col: first_time_stamp,
        pks: vec![],
    })
}

/// Return first timestamp column found in output schema and all string columns
fn build_by_tql_schema(plan: &LogicalPlan) -> Result<TableDef, Error> {
    let first_time_stamp = plan.schema().fields().iter().find_map(|f| {
        if ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp() {
            Some(f.name().clone())
        } else {
            None
        }
    });
    let string_columns = plan
        .schema()
        .fields()
        .iter()
        .filter_map(|f| {
            if ConcreteDataType::from_arrow_type(f.data_type()).is_string() {
                Some(f.name().clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    Ok(TableDef {
        ts_col: first_time_stamp,
        pks: string_columns,
    })
}

struct TableDef {
    ts_col: Option<String>,
    pks: Vec<String>,
}

/// Return first timestamp column which is in group by clause and other columns which are also in group by clause
///
/// # Returns
///
/// * `Option<String>` - first timestamp column which is in group by clause
/// * `Vec<String>` - other columns which are also in group by clause
///
/// if no aggregation found, return None
fn build_pk_from_aggr(plan: &LogicalPlan) -> Result<Option<TableDef>, Error> {
    let fields = plan.schema().fields();
    let mut pk_names = FindGroupByFinalName::default();

    plan.visit(&mut pk_names)
        .with_context(|_| DatafusionSnafu {
            context: format!("Can't find aggr expr in plan {plan:?}"),
        })?;

    // if no group by clause, return empty with first timestamp column found in output schema
    let Some(pk_final_names) = pk_names.get_group_expr_names() else {
        return Ok(None);
    };
    if pk_final_names.is_empty() {
        let first_ts_col = fields
            .iter()
            .find(|f| ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp())
            .map(|f| f.name().clone());
        return Ok(Some(TableDef {
            ts_col: first_ts_col,
            pks: vec![],
        }));
    }

    let all_pk_cols: Vec<_> = fields
        .iter()
        .filter(|f| pk_final_names.contains(f.name()))
        .map(|f| f.name().clone())
        .collect();
    // Auto-created tables use the first timestamp column in the group-by keys
    // as the time index. It is possible that timestamp columns appear only as
    // aggregate outputs (for example `max(ts)`) and are not group-by keys; in
    // that case `first_time_stamp` stays `None` and the caller falls back to a
    // placeholder time index column.
    let first_time_stamp = fields
        .iter()
        .find(|f| {
            all_pk_cols.contains(&f.name().clone())
                && ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp()
        })
        .map(|f| f.name().clone());

    let all_pk_cols: Vec<_> = all_pk_cols
        .into_iter()
        .filter(|col| first_time_stamp.as_ref() != Some(col))
        .collect();

    Ok(Some(TableDef {
        ts_col: first_time_stamp,
        pks: all_pk_cols,
    }))
}

#[cfg(test)]
mod test {
    use api::v1::column_def::try_as_column_schema;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use pretty_assertions::assert_eq;
    use session::context::QueryContext;

    use super::*;
    use crate::adapter::{AUTO_CREATED_PLACEHOLDER_TS_COL, AUTO_CREATED_UPDATE_AT_TS_COL};
    use crate::batching_mode::utils::sql_to_df_plan;
    use crate::test_utils::create_test_query_engine;

    #[tokio::test]
    async fn test_gen_create_table_sql() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        struct TestCase {
            sql: String,
            sink_table_name: String,
            column_schemas: Vec<ColumnSchema>,
            primary_keys: Vec<String>,
            time_index: String,
        }

        let update_at_schema = ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        );

        let ts_placeholder_schema = ColumnSchema::new(
            AUTO_CREATED_PLACEHOLDER_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true);

        let testcases = vec![
            TestCase {
                sql: "SELECT number, ts FROM numbers_with_ts".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    update_at_schema.clone(),
                ],
                primary_keys: vec![],
                time_index: "ts".to_string(),
            },
            TestCase {
                sql: "SELECT number, max(ts) FROM numbers_with_ts GROUP BY number".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "max(numbers_with_ts.ts)",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        true,
                    ),
                    update_at_schema.clone(),
                    ts_placeholder_schema.clone(),
                ],
                primary_keys: vec!["number".to_string()],
                time_index: AUTO_CREATED_PLACEHOLDER_TS_COL.to_string(),
            },
            TestCase {
                sql: "SELECT max(number), ts FROM numbers_with_ts GROUP BY ts".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new(
                        "max(numbers_with_ts.number)",
                        ConcreteDataType::uint32_datatype(),
                        true,
                    ),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    update_at_schema.clone(),
                ],
                primary_keys: vec![],
                time_index: "ts".to_string(),
            },
            TestCase {
                sql: "SELECT number, ts FROM numbers_with_ts GROUP BY ts, number".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    update_at_schema.clone(),
                ],
                primary_keys: vec!["number".to_string()],
                time_index: "ts".to_string(),
            },
        ];

        for tc in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), &tc.sql, true)
                .await
                .unwrap();
            let expr = create_table_with_expr(
                &plan,
                &[
                    "greptime".to_string(),
                    "public".to_string(),
                    tc.sink_table_name.clone(),
                ],
                &QueryType::Sql,
            )
            .unwrap();
            // TODO(discord9): assert expr
            let column_schemas = expr
                .column_defs
                .iter()
                .map(|c| try_as_column_schema(c).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(tc.column_schemas, column_schemas, "{:?}", tc.sql);
            assert_eq!(tc.primary_keys, expr.primary_keys, "{:?}", tc.sql);
            assert_eq!(tc.time_index, expr.time_index, "{:?}", tc.sql);
        }
    }
}
