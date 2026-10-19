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

//! Side-effect-free destination validation for OTLP source points. Catalog
//! failures propagate; attributable input errors remove a point's entire fan-out.

use std::collections::{HashMap, HashSet};

use api::helper::{ColumnDataTypeWrapper, to_grpc_value};
use api::v1::alter_table_expr::Kind;
use api::v1::column_def::try_as_column_schema;
use api::v1::{ColumnDef, ColumnSchema, RowInsertRequests, Rows, SemanticType, Value};
use common_query::native_histogram::native_histogram_value_type;
use common_query::prelude::{greptime_timestamp, greptime_value};
use common_time::timestamp::TimeUnit;
use datatypes::schema::ColumnSchema as DataColumn;
use session::context::QueryContextRef;
use snafu::prelude::*;
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, PHYSICAL_TABLE_METADATA_KEY};
use table::TableRef;

use crate::error::{self, InvalidInsertRequestSnafu, Result};
use crate::insert::{
    AutoCreateTableType, Inserter, convert_rows_time_unit, request_is_native_histogram,
    table_is_native_histogram, table_time_index_unit, validate_column_count_match,
};
use crate::req_convert::insert::{ImpureDefaultFiller, value_matches_type};

pub struct MetricInsertPreflight {
    pub rejections: Vec<Option<String>>,
    pub physical_tables: std::collections::HashMap<String, String>,
}

struct PhysicalColumns {
    by_name: HashMap<String, (DataColumn, SemanticType)>,
    fields: Vec<String>,
}

impl PhysicalColumns {
    fn new(columns: Vec<(DataColumn, SemanticType)>) -> Self {
        let fields = columns
            .iter()
            .filter(|(_, semantic)| *semantic == SemanticType::Field)
            .map(|(column, _)| column.name.clone())
            .collect();
        let by_name = columns
            .into_iter()
            .map(|(column, semantic)| (column.name.clone(), (column, semantic)))
            .collect();
        Self { by_name, fields }
    }
}

impl Inserter {
    /// Validate each point independently against a single catalog snapshot.
    /// Fresh cross-point conflicts are resolved by the converter after this pass.
    pub async fn preflight_otlp_metrics(
        &self,
        points: &mut [RowInsertRequests],
        ctx: &QueryContextRef,
        metric_engine: bool,
        physical_table: &str,
    ) -> Result<MetricInsertPreflight> {
        let disabled = self.auto_create_disabled_reason(ctx)?;
        let mut tables = HashMap::new();
        let mut physical_tables = std::collections::HashMap::new();
        for point in points.iter() {
            for request in &point.inserts {
                if !tables.contains_key(&request.table_name) {
                    let table = self
                        .get_table(
                            ctx.current_catalog(),
                            &ctx.current_schema(),
                            &request.table_name,
                        )
                        .await?;
                    tables.insert(request.table_name.clone(), table);
                }
                let backing = match tables[&request.table_name].as_ref() {
                    Some(table) => table
                        .table_info()
                        .meta
                        .options
                        .extra_options
                        .get(LOGICAL_TABLE_METADATA_KEY)
                        .cloned(),
                    None if metric_engine => Some(physical_table.to_string()),
                    None => None,
                };
                if let Some(backing) = backing {
                    physical_tables.insert(request.table_name.clone(), backing);
                }
            }
        }
        // Shared schemas accumulate tags across logical tables; index them once per request.
        let mut physical_columns = HashMap::new();
        for backing in physical_tables.values() {
            if !tables.contains_key(backing) {
                let table = self
                    .get_table(ctx.current_catalog(), &ctx.current_schema(), backing)
                    .await?;
                tables.insert(backing.clone(), table);
            }
            physical_columns.entry(backing.as_str()).or_insert_with(|| {
                PhysicalColumns::new(
                    tables[backing]
                        .as_ref()
                        .map(table_columns)
                        .unwrap_or_else(default_physical_columns),
                )
            });
        }
        // Match ordinary insertion: impure defaults are evaluated once per table
        // and request, while supplied nulls remain supplied nulls.
        let mut fillers = HashMap::new();
        let mut rejections = Vec::with_capacity(points.len());
        for point in points {
            let result = (|| {
                ensure!(
                    point.inserts.iter().all(|request| request.rows.is_some()),
                    InvalidInsertRequestSnafu {
                        reason: "missing metric rows"
                    }
                );
                validate_column_count_match(point)?;
                for request in &mut point.inserts {
                    let auto_create = disabled.is_none()
                        || Self::is_auto_create_exempt_private_table(
                            &ctx.current_schema(),
                            &request.table_name,
                        );
                    let target = tables[&request.table_name].as_ref();
                    let backing = physical_tables.get(&request.table_name);
                    let physical = backing.and_then(|name| tables[name].as_ref());
                    if let Some(physical) = physical {
                        ensure!(
                            physical
                                .table_info()
                                .meta
                                .options
                                .extra_options
                                .contains_key(PHYSICAL_TABLE_METADATA_KEY),
                            InvalidInsertRequestSnafu {
                                reason: "selected backing table is not a physical metric table"
                            }
                        );
                    }
                    if metric_engine {
                        ensure!(
                            backing.is_none() || physical.is_some() || auto_create,
                            InvalidInsertRequestSnafu {
                                reason: format!(
                                    "missing physical table and {}",
                                    disabled.unwrap_or_default()
                                ),
                            }
                        );
                        let unit = target
                            .and_then(table_time_index_unit)
                            .or_else(|| physical.and_then(table_time_index_unit))
                            .unwrap_or(TimeUnit::Millisecond);
                        if let Some(rows) = &mut request.rows {
                            convert_rows_time_unit(rows, unit)?;
                        }
                    }
                    let mut columns = if let Some(table) = target {
                        ensure!(
                            !table
                                .table_info()
                                .meta
                                .options
                                .extra_options
                                .contains_key(PHYSICAL_TABLE_METADATA_KEY),
                            InvalidInsertRequestSnafu {
                                reason: "direct writes to a physical metric table are forbidden",
                            }
                        );
                        ensure!(
                            request_is_native_histogram(&request.rows.as_ref().unwrap().schema)
                                == table_is_native_histogram(table),
                            InvalidInsertRequestSnafu {
                                reason: format!(
                                    "Table `{}` cannot mix native histogram and float sample fields",
                                    request.table_name
                                ),
                            }
                        );
                        let mut columns = table_columns(table);
                        if auto_create {
                            // Ordinary OTLP insertion does not rename columns.
                            if let Some(alter) = self.get_alter_table_expr_on_demand(
                                request,
                                table,
                                ctx,
                                metric_engine,
                                true,
                                true,
                            )? && let Some(Kind::AddColumns(add)) = alter.kind
                            {
                                for column in add.add_columns {
                                    let def = column.column_def.with_context(|| {
                                        InvalidInsertRequestSnafu {
                                            reason: "missing column definition",
                                        }
                                    })?;
                                    columns.push(column_definition(&def)?);
                                }
                            }
                        }
                        columns
                    } else {
                        ensure!(
                            auto_create,
                            InvalidInsertRequestSnafu {
                                reason: format!(
                                    "missing table `{}` and {}",
                                    request.table_name,
                                    disabled.unwrap_or_default()
                                ),
                            }
                        );
                        let create_type = if metric_engine {
                            AutoCreateTableType::Logical(physical_table.to_string())
                        } else {
                            AutoCreateTableType::Physical
                        };
                        let create =
                            self.get_create_table_expr_on_demand(request, &create_type, ctx, None)?;
                        if metric_engine {
                            ensure!(
                                create
                                    .column_defs
                                    .iter()
                                    .filter(|c| c.semantic_type == SemanticType::Field as i32)
                                    .count()
                                    == 1,
                                InvalidInsertRequestSnafu {
                                    reason: "logical metrics require exactly one sample field"
                                }
                            );
                        }
                        create
                            .column_defs
                            .iter()
                            .map(column_definition)
                            .collect::<Result<Vec<_>>>()?
                    };
                    let Some(rows) = &mut request.rows else {
                        continue;
                    };
                    if let Some(backing) = backing {
                        // Logical tag nullability follows the shared physical schema.
                        for (column, semantic) in &mut columns {
                            if *semantic == SemanticType::Tag {
                                column.set_nullable();
                            }
                        }
                        validate_physical_columns(
                            rows,
                            &physical_columns[backing.as_str()],
                            auto_create,
                        )?;
                    }
                    if let Some(table) = target {
                        if !fillers.contains_key(&request.table_name) {
                            fillers.insert(
                                request.table_name.clone(),
                                ImpureDefaultFiller::new(table.table_info())?,
                            );
                        }
                        fillers[&request.table_name].fill_rows(rows);
                    }
                    validate_and_fill_rows(rows, &columns)?;
                }
                Ok(())
            })();
            match result {
                Ok(()) => rejections.push(None),
                Err(err) if attributable(&err) => rejections.push(Some(err.to_string())),
                Err(err) => return Err(err),
            }
        }
        Ok(MetricInsertPreflight {
            rejections,
            physical_tables,
        })
    }
}

fn attributable(err: &error::Error) -> bool {
    matches!(
        err,
        error::Error::InvalidInsertRequest { .. }
            | error::Error::BuildCreateExprOnInsertion { .. }
            | error::Error::FindNewColumnsOnInsertion { .. }
            | error::Error::InvalidColumnDef { .. }
    )
}

fn invalid(reason: impl Into<String>) -> error::Error {
    InvalidInsertRequestSnafu {
        reason: reason.into(),
    }
    .build()
}

fn column_definition(def: &ColumnDef) -> Result<(DataColumn, SemanticType)> {
    let column = try_as_column_schema(def).map_err(|err| invalid(err.to_string()))?;
    let semantic =
        SemanticType::try_from(def.semantic_type).map_err(|err| invalid(err.to_string()))?;
    Ok((column, semantic))
}

fn table_columns(table: &TableRef) -> Vec<(DataColumn, SemanticType)> {
    let info = table.table_info();
    let primary_keys: HashSet<_> = info.meta.primary_key_indices.iter().copied().collect();
    info.meta
        .schema
        .column_schemas()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let semantic = if column.is_time_index() {
                SemanticType::Timestamp
            } else if primary_keys.contains(&index) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            };
            (column.clone(), semantic)
        })
        .collect()
}

fn default_physical_columns() -> Vec<(DataColumn, SemanticType)> {
    use datatypes::prelude::ConcreteDataType;
    vec![
        (
            DataColumn::new(
                greptime_timestamp(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            SemanticType::Timestamp,
        ),
        (
            DataColumn::new(greptime_value(), ConcreteDataType::float64_datatype(), true),
            SemanticType::Field,
        ),
    ]
}

fn check_definition(
    source: &ColumnSchema,
    target: &DataColumn,
    semantic: SemanticType,
) -> Result<()> {
    ensure!(
        api::helper::is_column_type_value_eq(
            source.datatype,
            source.datatype_extension.clone(),
            &target.data_type
        ) && source.semantic_type == semantic as i32,
        InvalidInsertRequestSnafu {
            reason: format!(
                "incompatible datatype or semantic type for column `{}`",
                source.column_name
            ),
        }
    );
    Ok(())
}

fn validate_physical_columns(
    rows: &Rows,
    columns: &PhysicalColumns,
    auto_create: bool,
) -> Result<()> {
    for source in &rows.schema {
        if let Some((target, semantic)) = columns.by_name.get(&source.column_name) {
            check_definition(source, target, *semantic)?;
        } else {
            ensure!(
                auto_create,
                InvalidInsertRequestSnafu {
                    reason: format!(
                        "missing physical column `{}` while auto-DDL is disabled",
                        source.column_name
                    )
                }
            );
            ensure!(
                source.semantic_type == SemanticType::Tag as i32
                    || (source.semantic_type == SemanticType::Field as i32
                        && api::helper::is_column_type_value_eq(
                            source.datatype,
                            source.datatype_extension.clone(),
                            native_histogram_value_type()
                        )),
                InvalidInsertRequestSnafu {
                    reason: format!("cannot add physical column `{}`", source.column_name)
                }
            );
        }
    }
    for name in &columns.fields {
        if !rows.schema.iter().any(|c| c.column_name == *name) {
            let (column, _) = &columns.by_name[name];
            ensure!(
                !column.is_default_impure(),
                InvalidInsertRequestSnafu {
                    reason: format!("impure default for physical field `{}`", column.name)
                }
            );
            ensure!(
                column
                    .create_default()
                    .map_err(|e| invalid(e.to_string()))?
                    .is_some(),
                InvalidInsertRequestSnafu {
                    reason: format!("missing required physical field `{}`", column.name)
                }
            );
        }
    }
    Ok(())
}

fn validate_and_fill_rows(rows: &mut Rows, columns: &[(DataColumn, SemanticType)]) -> Result<()> {
    let mut names = HashSet::new();
    for (index, source) in rows.schema.iter().enumerate() {
        ensure!(
            names.insert(source.column_name.as_str()),
            InvalidInsertRequestSnafu {
                reason: format!("duplicate column `{}`", source.column_name)
            }
        );
        let (target, semantic) = columns
            .iter()
            .find(|(c, _)| c.name == source.column_name)
            .ok_or_else(|| invalid(format!("unknown column `{}`", source.column_name)))?;
        check_definition(source, target, *semantic)?;
        for row in &rows.rows {
            let value = &row.values[index];
            ensure!(
                value_matches_type(value, source.datatype, source.datatype_extension.as_ref()),
                InvalidInsertRequestSnafu {
                    reason: format!("value datatype differs for `{}`", source.column_name)
                }
            );
            ensure!(
                value.value_data.is_some() || target.is_nullable(),
                InvalidInsertRequestSnafu {
                    reason: format!(
                        "null supplied for non-nullable column `{}`",
                        source.column_name
                    )
                }
            );
        }
    }
    // Resolve omissions before schemas are merged; an explicit null never uses a default.
    let missing = columns
        .iter()
        .filter(|(c, _)| !names.contains(c.name.as_str()))
        .collect::<Vec<_>>();
    for (column, semantic) in missing {
        let value = column
            .create_default()
            .map_err(|e| invalid(e.to_string()))?
            .ok_or_else(|| invalid(format!("missing required column `{}`", column.name)))?;
        let datatype = ColumnDataTypeWrapper::try_from(column.data_type.clone())
            .map_err(|e| invalid(e.to_string()))?;
        let (datatype, datatype_extension) = datatype.to_parts();
        rows.schema.push(ColumnSchema {
            column_name: column.name.clone(),
            datatype: datatype as i32,
            semantic_type: *semantic as i32,
            datatype_extension,
            options: None,
        });
        let value: Value = to_grpc_value(value);
        for row in &mut rows.rows {
            row.values.push(value.clone());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnDefaultConstraint;
    use datatypes::value::Value as DataValue;

    use super::*;

    #[test]
    fn test_preflight_omissions_defaults_and_explicit_nulls() {
        let mut columns = default_physical_columns();
        columns.push((
            DataColumn::new("host", ConcreteDataType::string_datatype(), false)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(DataValue::String(
                    "unknown".into(),
                ))))
                .unwrap(),
            SemanticType::Tag,
        ));
        let row = || Rows {
            schema: vec![ColumnSchema {
                column_name: greptime_timestamp().into(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            }],
            rows: vec![Row {
                values: vec![Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(1)),
                }],
            }],
        };
        let mut omitted = row();
        validate_and_fill_rows(&mut omitted, &columns).unwrap();
        assert_eq!(omitted.schema[2].column_name, "host");
        assert_eq!(
            omitted.rows[0].values[2].value_data,
            Some(ValueData::StringValue("unknown".into()))
        );
        omitted.rows[0].values[2] = Value::default();
        assert!(validate_and_fill_rows(&mut omitted, &columns).is_err());
        columns[2].0 = DataColumn::new("host", ConcreteDataType::string_datatype(), false);
        assert!(validate_and_fill_rows(&mut row(), &columns).is_err());
        columns[2].0.set_nullable();
        let mut nullable = row();
        validate_and_fill_rows(&mut nullable, &columns).unwrap();
        assert_eq!(nullable.rows[0].values[2].value_data, None);
    }

    #[test]
    fn test_preflight_existing_columns_and_physical_definitions() {
        let columns = default_physical_columns();
        let mut rows = Rows {
            schema: vec![ColumnSchema {
                column_name: greptime_timestamp().into(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            }],
            rows: vec![Row {
                values: vec![Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(1)),
                }],
            }],
        };
        validate_and_fill_rows(&mut rows, &columns).unwrap();
        for case in 0..3 {
            let mut invalid = rows.clone();
            match case {
                0 => invalid.schema[0].semantic_type = SemanticType::Tag as i32,
                1 => invalid.schema[0].datatype = ColumnDataType::TimestampNanosecond as i32,
                _ => {
                    invalid.rows[0].values[0].value_data =
                        Some(ValueData::StringValue("bad".into()))
                }
            }
            assert!(validate_and_fill_rows(&mut invalid, &columns).is_err());
        }
        let columns = PhysicalColumns::new(columns);
        rows.schema[1].column_name = "new_field".into();
        assert!(validate_physical_columns(&rows, &columns, true).is_err());
        rows.schema[1].semantic_type = SemanticType::Tag as i32;
        rows.schema[1].datatype = ColumnDataType::String as i32;
        assert!(validate_physical_columns(&rows, &columns, false).is_err());
        assert!(validate_physical_columns(&rows, &columns, true).is_ok());
    }

    #[test]
    fn test_preflight_shared_physical_field_defaults() {
        let rows = Rows {
            schema: vec![api::v1::helper::time_index_column_schema(
                greptime_timestamp(),
                ColumnDataType::TimestampMillisecond,
            )],
            ..Default::default()
        };
        let required = DataColumn::new("required", ConcreteDataType::float64_datatype(), false);
        let defaulted = required
            .clone()
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(DataValue::Float64(
                1.0.into(),
            ))))
            .unwrap();
        let impure = DataColumn::new(
            "impure",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_default_constraint(Some(ColumnDefaultConstraint::Function(
            "current_timestamp()".into(),
        )))
        .unwrap();
        for (field, error) in [
            (required, Some("missing required physical field `required`")),
            (defaulted, None),
            (impure, Some("impure default for physical field `impure`")),
        ] {
            let mut columns = default_physical_columns();
            columns.extend((0..1024).map(|index| {
                (
                    DataColumn::new(
                        format!("unrelated_tag_{index}"),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    SemanticType::Tag,
                )
            }));
            columns.push((field, SemanticType::Field));
            let columns = PhysicalColumns::new(columns);
            for auto_create in [false, true] {
                let result = validate_physical_columns(&rows, &columns, auto_create);
                if let Some(error) = error {
                    assert!(result.unwrap_err().to_string().contains(error));
                } else {
                    result.unwrap();
                }
            }
        }
    }
}
