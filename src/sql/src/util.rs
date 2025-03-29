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

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use api::helper::ColumnDataTypeWrapper;
use api::v1::column_def::options_from_column_schema;
use api::v1::{ColumnDataType, ColumnDataTypeExtension, CreateTableExpr, SemanticType};
use common_time::Timezone;
use datatypes::schema::{ColumnSchema, COMMENT_KEY};
use snafu::{ensure, OptionExt, ResultExt};
use sqlparser::ast::{
    ColumnOption, Expr, ObjectName, Query, SetExpr, SqlOption, TableFactor, Value,
};
use table::requests::TableOptions;

use crate::error::{
    self, ColumnDataTypeSnafu, ConvertColumnDefaultConstraintSnafu, IllegalPrimaryKeysDefSnafu,
    InvalidSqlSnafu, InvalidTableOptionValueSnafu, Result, UnrecognizedTableOptionSnafu,
};
use crate::statements::column_to_schema;
use crate::statements::create::{Column, CreateTable, TableConstraint};

/// Format an [ObjectName] without any quote of its idents.
pub fn format_raw_object_name(name: &ObjectName) -> String {
    struct Inner<'a> {
        name: &'a ObjectName,
    }

    impl Display for Inner<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut delim = "";
            for ident in self.name.0.iter() {
                write!(f, "{delim}")?;
                delim = ".";
                write!(f, "{}", ident.value)?;
            }
            Ok(())
        }
    }

    format!("{}", Inner { name })
}

pub fn parse_option_string(option: SqlOption) -> Result<(String, String)> {
    let SqlOption::KeyValue { key, value } = option else {
        return InvalidSqlSnafu {
            msg: "Expecting a key-value pair in the option",
        }
        .fail();
    };
    let v = match value {
        Expr::Value(Value::SingleQuotedString(v)) | Expr::Value(Value::DoubleQuotedString(v)) => v,
        Expr::Identifier(v) => v.value,
        Expr::Value(Value::Number(v, _)) => v.to_string(),
        value => return InvalidTableOptionValueSnafu { key, value }.fail(),
    };
    let k = key.value.to_lowercase();
    Ok((k, v))
}

/// Walk through a [Query] and extract all the tables referenced in it.
pub fn extract_tables_from_query(query: &Query) -> impl Iterator<Item = ObjectName> {
    let mut names = HashSet::new();

    extract_tables_from_set_expr(&query.body, &mut names);

    names.into_iter()
}

/// Helper function for [extract_tables_from_query].
///
/// Handle [SetExpr].
fn extract_tables_from_set_expr(set_expr: &SetExpr, names: &mut HashSet<ObjectName>) {
    match set_expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                table_factor_to_object_name(&from.relation, names);
                for join in &from.joins {
                    table_factor_to_object_name(&join.relation, names);
                }
            }
        }
        SetExpr::Query(query) => {
            extract_tables_from_set_expr(&query.body, names);
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_set_expr(left, names);
            extract_tables_from_set_expr(right, names);
        }
        SetExpr::Values(_) | SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Table(_) => {}
    };
}

/// Helper function for [extract_tables_from_query].
///
/// Handle [TableFactor].
fn table_factor_to_object_name(table_factor: &TableFactor, names: &mut HashSet<ObjectName>) {
    if let TableFactor::Table { name, .. } = table_factor {
        names.insert(name.to_owned());
    }
}

/// Convert `CreateTable` statement to [`CreateTableExpr`] gRPC request.
pub fn create_to_expr(
    create: &CreateTable,
    current_catalog: impl Into<String>,
    current_schema: impl Into<String>,
    current_timezone: &Timezone,
) -> error::Result<CreateTableExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name, current_catalog, current_schema)?;

    let time_index = find_time_index(&create.constraints)?;
    let table_options = HashMap::from(
        &TableOptions::try_from_iter(create.options.to_str_map())
            .context(UnrecognizedTableOptionSnafu)?,
    );

    let primary_keys = find_primary_keys(&create.columns, &create.constraints)?;

    let expr = CreateTableExpr {
        catalog_name,
        schema_name,
        table_name,
        desc: String::default(),
        column_defs: columns_to_expr(
            &create.columns,
            &time_index,
            &primary_keys,
            Some(current_timezone),
        )?,
        time_index,
        primary_keys,
        create_if_not_exists: create.if_not_exists,
        table_options,
        table_id: None,
        engine: create.engine.to_string(),
    };

    validate_create_expr(&expr)?;
    Ok(expr)
}

pub fn find_time_index(constraints: &[TableConstraint]) -> Result<String> {
    let time_index = constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::TimeIndex { column, .. } => Some(&column.value),
            _ => None,
        })
        .collect::<Vec<&String>>();
    ensure!(
        time_index.len() == 1,
        InvalidSqlSnafu {
            msg: "must have one and only one TimeIndex columns",
        }
    );
    Ok(time_index.first().unwrap().to_string())
}

pub fn find_primary_keys(
    columns: &[Column],
    constraints: &[TableConstraint],
) -> Result<Vec<String>> {
    let columns_pk = columns
        .iter()
        .filter_map(|x| {
            if x.options().iter().any(|o| {
                matches!(
                    o.option,
                    ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
                )
            }) {
                Some(x.name().value.clone())
            } else {
                None
            }
        })
        .collect::<Vec<String>>();

    ensure!(
        columns_pk.len() <= 1,
        IllegalPrimaryKeysDefSnafu {
            msg: "not allowed to inline multiple primary keys in columns options"
        }
    );

    let constraints_pk = constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::PrimaryKey { columns, .. } => {
                Some(columns.iter().map(|ident| ident.value.clone()))
            }
            _ => None,
        })
        .flatten()
        .collect::<Vec<String>>();

    ensure!(
        columns_pk.is_empty() || constraints_pk.is_empty(),
        IllegalPrimaryKeysDefSnafu {
            msg: "found definitions of primary keys in multiple places"
        }
    );

    let mut primary_keys = Vec::with_capacity(columns_pk.len() + constraints_pk.len());
    primary_keys.extend(columns_pk);
    primary_keys.extend(constraints_pk);
    Ok(primary_keys)
}

fn columns_to_expr(
    column_defs: &[Column],
    time_index: &str,
    primary_keys: &[String],
    timezone: Option<&Timezone>,
) -> Result<Vec<api::v1::ColumnDef>> {
    let column_schemas = columns_to_column_schemas(column_defs, time_index, timezone)?;
    column_schemas_to_defs(column_schemas, primary_keys)
}

pub fn columns_to_column_schemas(
    columns: &[Column],
    time_index: &str,
    timezone: Option<&Timezone>,
) -> Result<Vec<ColumnSchema>> {
    columns
        .iter()
        .map(|c| column_to_schema(c, time_index, timezone))
        .collect::<Result<Vec<ColumnSchema>>>()
}

pub fn column_schemas_to_defs(
    column_schemas: Vec<ColumnSchema>,
    primary_keys: &[String],
) -> Result<Vec<api::v1::ColumnDef>> {
    let column_datatypes: Vec<(ColumnDataType, Option<ColumnDataTypeExtension>)> = column_schemas
        .iter()
        .map(|c| {
            ColumnDataTypeWrapper::try_from(c.data_type.clone())
                .map(|w| w.to_parts())
                .context(ColumnDataTypeSnafu)
        })
        .collect::<Result<Vec<_>>>()?;

    column_schemas
        .iter()
        .zip(column_datatypes)
        .map(|(schema, datatype)| {
            let semantic_type = if schema.is_time_index() {
                SemanticType::Timestamp
            } else if primary_keys.contains(&schema.name) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            } as i32;
            let comment = schema
                .metadata()
                .get(COMMENT_KEY)
                .cloned()
                .unwrap_or_default();

            Ok(api::v1::ColumnDef {
                name: schema.name.clone(),
                data_type: datatype.0 as i32,
                is_nullable: schema.is_nullable(),
                default_constraint: match schema.default_constraint() {
                    None => vec![],
                    Some(v) => {
                        v.clone()
                            .try_into()
                            .context(ConvertColumnDefaultConstraintSnafu {
                                column_name: &schema.name,
                            })?
                    }
                },
                semantic_type,
                comment,
                datatype_extension: datatype.1,
                options: options_from_column_schema(schema),
            })
        })
        .collect()
}

/// Converts maybe fully-qualified table name (`<catalog>.<schema>.<table>`) to tuple.
pub fn table_idents_to_full_name(
    obj_name: &ObjectName,
    current_catalog: impl Into<String>,
    current_schema: impl Into<String>,
) -> Result<(String, String, String)> {
    match &obj_name.0[..] {
        [table] => Ok((
            current_catalog.into(),
            current_schema.into(),
            table.value.clone(),
        )),
        [schema, table] => Ok((
            current_catalog.into(),
            schema.value.clone(),
            table.value.clone(),
        )),
        [catalog, schema, table] => Ok((
            catalog.value.clone(),
            schema.value.clone(),
            table.value.clone(),
        )),
        _ => InvalidSqlSnafu {
            msg: format!(
                "expect table name to be <catalog>.<schema>.<table>, <schema>.<table> or <table>, actual: {obj_name}",
            ),
        }.fail(),
    }
}

/// Validate the [`CreateTableExpr`] request.
pub fn validate_create_expr(create: &CreateTableExpr) -> Result<()> {
    // construct column list
    let mut column_to_indices = HashMap::with_capacity(create.column_defs.len());
    for (idx, column) in create.column_defs.iter().enumerate() {
        if let Some(indices) = column_to_indices.get(&column.name) {
            return InvalidSqlSnafu {
                msg: format!(
                    "column name `{}` is duplicated at index {} and {}",
                    column.name, indices, idx
                ),
            }
            .fail();
        }
        column_to_indices.insert(&column.name, idx);
    }

    // verify time_index exists
    let _ = column_to_indices
        .get(&create.time_index)
        .with_context(|| InvalidSqlSnafu {
            msg: format!(
                "column name `{}` is not found in column list",
                create.time_index
            ),
        })?;

    // verify primary_key exists
    for pk in &create.primary_keys {
        let _ = column_to_indices
            .get(&pk)
            .with_context(|| InvalidSqlSnafu {
                msg: format!("column name `{}` is not nd in column list", pk),
            })?;
    }

    // construct primary_key set
    let mut pk_set = HashSet::new();
    for pk in &create.primary_keys {
        if !pk_set.insert(pk) {
            return InvalidSqlSnafu {
                msg: format!("column name `{}` is duplicated in primary keys", pk),
            }
            .fail();
        }
    }

    // verify time index is not primary key
    if pk_set.contains(&create.time_index) {
        return InvalidSqlSnafu {
            msg: format!(
                "column name `{}` is both primary key and time index",
                create.time_index
            ),
        }
        .fail();
    }

    for column in &create.column_defs {
        // verify do not contain interval type column issue #3235
        if is_interval_type(&column.data_type()) {
            return InvalidSqlSnafu {
                msg: format!(
                    "column name `{}` is interval type, which is not supported",
                    column.name
                ),
            }
            .fail();
        }
        // verify do not contain datetime type column issue #5489
        if is_date_time_type(&column.data_type()) {
            return InvalidSqlSnafu {
                msg: format!(
                    "column name `{}` is datetime type, which is not supported, please use `timestamp` type instead",
                    column.name
                ),
            }
            .fail();
        }
    }
    Ok(())
}

pub fn is_interval_type(data_type: &ColumnDataType) -> bool {
    matches!(
        data_type,
        ColumnDataType::IntervalYearMonth
            | ColumnDataType::IntervalDayTime
            | ColumnDataType::IntervalMonthDayNano
    )
}

pub fn is_date_time_type(data_type: &ColumnDataType) -> bool {
    matches!(data_type, ColumnDataType::Datetime)
}
