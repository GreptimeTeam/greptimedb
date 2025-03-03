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

use std::collections::HashSet;

use api::v1::column_data_type_extension::TypeExt;
use api::v1::column_def::{contains_fulltext, contains_skipping};
use api::v1::{
    AddColumn, AddColumns, Column, ColumnDataType, ColumnDataTypeExtension, ColumnDef,
    ColumnOptions, ColumnSchema, CreateTableExpr, JsonTypeExtension, SemanticType,
};
use datatypes::schema::Schema;
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableId;
use table::table_reference::TableReference;

use crate::error::{
    self, DuplicatedColumnNameSnafu, DuplicatedTimestampColumnSnafu,
    InvalidStringIndexColumnTypeSnafu, MissingTimestampColumnSnafu, Result,
    UnknownColumnDataTypeSnafu,
};
pub struct ColumnExpr<'a> {
    pub column_name: &'a str,
    pub datatype: i32,
    pub semantic_type: i32,
    pub datatype_extension: &'a Option<ColumnDataTypeExtension>,
    pub options: &'a Option<ColumnOptions>,
}

impl<'a> ColumnExpr<'a> {
    #[inline]
    pub fn from_columns(columns: &'a [Column]) -> Vec<Self> {
        columns.iter().map(Self::from).collect()
    }

    #[inline]
    pub fn from_column_schemas(schemas: &'a [ColumnSchema]) -> Vec<Self> {
        schemas.iter().map(Self::from).collect()
    }
}

impl<'a> From<&'a Column> for ColumnExpr<'a> {
    fn from(column: &'a Column) -> Self {
        Self {
            column_name: &column.column_name,
            datatype: column.datatype,
            semantic_type: column.semantic_type,
            datatype_extension: &column.datatype_extension,
            options: &column.options,
        }
    }
}

impl<'a> From<&'a ColumnSchema> for ColumnExpr<'a> {
    fn from(schema: &'a ColumnSchema) -> Self {
        Self {
            column_name: &schema.column_name,
            datatype: schema.datatype,
            semantic_type: schema.semantic_type,
            datatype_extension: &schema.datatype_extension,
            options: &schema.options,
        }
    }
}

fn infer_column_datatype(
    datatype: i32,
    datatype_extension: &Option<ColumnDataTypeExtension>,
) -> Result<ColumnDataType> {
    let column_type =
        ColumnDataType::try_from(datatype).context(UnknownColumnDataTypeSnafu { datatype })?;

    if matches!(&column_type, ColumnDataType::Binary) {
        if let Some(ext) = datatype_extension {
            let type_ext = ext
                .type_ext
                .as_ref()
                .context(error::MissingFieldSnafu { field: "type_ext" })?;
            if *type_ext == TypeExt::JsonType(JsonTypeExtension::JsonBinary.into()) {
                return Ok(ColumnDataType::Json);
            }
        }
    }

    Ok(column_type)
}

pub fn build_create_table_expr(
    table_id: Option<TableId>,
    table_name: &TableReference<'_>,
    column_exprs: Vec<ColumnExpr>,
    engine: &str,
    desc: &str,
) -> Result<CreateTableExpr> {
    // Check for duplicate names. If found, raise an error.
    //
    // The introduction of hashset incurs additional memory overhead
    // but achieves a time complexity of O(1).
    //
    // The separate iteration over `column_exprs` is because the CPU prefers
    // smaller loops, and avoid cloning String.
    let mut distinct_names = HashSet::with_capacity(column_exprs.len());
    for ColumnExpr { column_name, .. } in &column_exprs {
        ensure!(
            distinct_names.insert(*column_name),
            DuplicatedColumnNameSnafu { name: *column_name }
        );
    }

    let mut column_defs = Vec::with_capacity(column_exprs.len());
    let mut primary_keys = Vec::with_capacity(column_exprs.len());
    let mut time_index = None;

    for expr in column_exprs {
        let ColumnExpr {
            column_name,
            datatype,
            semantic_type,
            datatype_extension,
            options,
        } = expr;

        let mut is_nullable = true;
        match semantic_type {
            v if v == SemanticType::Tag as i32 => primary_keys.push(column_name.to_owned()),
            v if v == SemanticType::Timestamp as i32 => {
                ensure!(
                    time_index.is_none(),
                    DuplicatedTimestampColumnSnafu {
                        exists: time_index.as_ref().unwrap(),
                        duplicated: column_name,
                    }
                );
                time_index = Some(column_name.to_owned());
                // Timestamp column must not be null.
                is_nullable = false;
            }
            _ => {}
        }

        let column_type = infer_column_datatype(datatype, datatype_extension)?;

        ensure!(
            (!contains_fulltext(options) && !contains_skipping(options))
                || column_type == ColumnDataType::String,
            InvalidStringIndexColumnTypeSnafu {
                column_name,
                column_type,
            }
        );

        column_defs.push(ColumnDef {
            name: column_name.to_owned(),
            data_type: datatype,
            is_nullable,
            default_constraint: vec![],
            semantic_type,
            comment: String::new(),
            datatype_extension: *datatype_extension,
            options: options.clone(),
        });
    }

    let time_index = time_index.context(MissingTimestampColumnSnafu {
        msg: format!("table is {}", table_name.table),
    })?;

    Ok(CreateTableExpr {
        catalog_name: table_name.catalog.to_string(),
        schema_name: table_name.schema.to_string(),
        table_name: table_name.table.to_string(),
        desc: desc.to_string(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: table_id.map(|id| api::v1::TableId { id }),
        engine: engine.to_string(),
    })
}

/// Find columns that are not present in the schema and return them as `AddColumns`
/// for adding columns automatically.
/// It always sets `add_if_not_exists` to `true` for now.
pub fn extract_new_columns(
    schema: &Schema,
    column_exprs: Vec<ColumnExpr>,
) -> Result<Option<AddColumns>> {
    let columns_to_add = column_exprs
        .into_iter()
        .filter(|expr| schema.column_schema_by_name(expr.column_name).is_none())
        .map(|expr| {
            let column_def = Some(ColumnDef {
                name: expr.column_name.to_string(),
                data_type: expr.datatype,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: expr.semantic_type,
                comment: String::new(),
                datatype_extension: *expr.datatype_extension,
                options: expr.options.clone(),
            });
            AddColumn {
                column_def,
                location: None,
                add_if_not_exists: true,
            }
        })
        .collect::<Vec<_>>();

    if columns_to_add.is_empty() {
        Ok(None)
    } else {
        let mut distinct_names = HashSet::with_capacity(columns_to_add.len());
        for add_column in &columns_to_add {
            let name = add_column.column_def.as_ref().unwrap().name.as_str();
            ensure!(
                distinct_names.insert(name),
                DuplicatedColumnNameSnafu { name }
            );
        }

        Ok(Some(AddColumns {
            add_columns: columns_to_add,
        }))
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::{assert_eq, vec};

    use api::helper::ColumnDataTypeWrapper;
    use api::v1::column::Values;
    use api::v1::column_data_type_extension::TypeExt;
    use api::v1::{
        Column, ColumnDataType, ColumnDataTypeExtension, Decimal128, DecimalTypeExtension,
        IntervalMonthDayNano, SemanticType,
    };
    use common_catalog::consts::MITO_ENGINE;
    use common_time::interval::IntervalUnit;
    use common_time::timestamp::TimeUnit;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use snafu::ResultExt;

    use super::*;
    use crate::error;
    use crate::error::ColumnDataTypeSnafu;

    #[inline]
    fn build_column_schema(
        column_name: &str,
        datatype: i32,
        nullable: bool,
    ) -> error::Result<ColumnSchema> {
        let datatype_wrapper =
            ColumnDataTypeWrapper::try_new(datatype, None).context(ColumnDataTypeSnafu)?;

        Ok(ColumnSchema::new(
            column_name,
            datatype_wrapper.into(),
            nullable,
        ))
    }

    fn build_create_expr_from_insertion(
        catalog_name: &str,
        schema_name: &str,
        table_id: Option<TableId>,
        table_name: &str,
        columns: &[Column],
        engine: &str,
    ) -> Result<CreateTableExpr> {
        let table_name = TableReference::full(catalog_name, schema_name, table_name);
        let column_exprs = ColumnExpr::from_columns(columns);
        build_create_table_expr(
            table_id,
            &table_name,
            column_exprs,
            engine,
            "Created on insertion",
        )
    }

    #[test]
    fn test_build_create_table_request() {
        let table_id = Some(10);
        let table_name = "test_metric";

        assert!(
            build_create_expr_from_insertion("", "", table_id, table_name, &[], MITO_ENGINE)
                .is_err()
        );

        let insert_batch = mock_insert_batch();

        let create_expr = build_create_expr_from_insertion(
            "",
            "",
            table_id,
            table_name,
            &insert_batch.0,
            MITO_ENGINE,
        )
        .unwrap();

        assert_eq!(table_id, create_expr.table_id.map(|x| x.id));
        assert_eq!(table_name, create_expr.table_name);
        assert_eq!("Created on insertion".to_string(), create_expr.desc);
        assert_eq!(
            vec![create_expr.column_defs[0].name.clone()],
            create_expr.primary_keys
        );

        let column_defs = create_expr.column_defs;
        assert_eq!(column_defs[5].name, create_expr.time_index);
        assert_eq!(7, column_defs.len());

        assert_eq!(
            ConcreteDataType::string_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "host")
                        .unwrap()
                        .data_type,
                    None
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "cpu")
                        .unwrap()
                        .data_type,
                    None
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "memory")
                        .unwrap()
                        .data_type,
                    None
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::time_datatype(TimeUnit::Millisecond),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "time")
                        .unwrap()
                        .data_type,
                    None
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "interval")
                        .unwrap()
                        .data_type,
                    None
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "ts")
                        .unwrap()
                        .data_type,
                    None
                )
                .unwrap()
            )
        );

        let decimal_column = column_defs.iter().find(|c| c.name == "decimals").unwrap();
        assert_eq!(
            ConcreteDataType::decimal128_datatype(38, 10),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    decimal_column.data_type,
                    decimal_column.datatype_extension,
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn test_find_new_columns() {
        let mut columns = Vec::with_capacity(1);
        let cpu_column = build_column_schema("cpu", 10, true).unwrap();
        let ts_column = build_column_schema("ts", 15, false)
            .unwrap()
            .with_time_index(true);
        columns.push(cpu_column);
        columns.push(ts_column);

        let schema = Arc::new(SchemaBuilder::try_from(columns).unwrap().build().unwrap());

        assert!(extract_new_columns(&schema, ColumnExpr::from_columns(&[]))
            .unwrap()
            .is_none());

        let insert_batch = mock_insert_batch();

        let add_columns = extract_new_columns(&schema, ColumnExpr::from_columns(&insert_batch.0))
            .unwrap()
            .unwrap();

        assert_eq!(5, add_columns.add_columns.len());
        let host_column = &add_columns.add_columns[0];
        assert_eq!(
            ConcreteDataType::string_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    host_column.column_def.as_ref().unwrap().data_type,
                    None
                )
                .unwrap()
            )
        );
        assert!(host_column.add_if_not_exists);

        let memory_column = &add_columns.add_columns[1];
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    memory_column.column_def.as_ref().unwrap().data_type,
                    None
                )
                .unwrap()
            )
        );
        assert!(host_column.add_if_not_exists);

        let time_column = &add_columns.add_columns[2];
        assert_eq!(
            ConcreteDataType::time_datatype(TimeUnit::Millisecond),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    time_column.column_def.as_ref().unwrap().data_type,
                    None
                )
                .unwrap()
            )
        );
        assert!(host_column.add_if_not_exists);

        let interval_column = &add_columns.add_columns[3];
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    interval_column.column_def.as_ref().unwrap().data_type,
                    None
                )
                .unwrap()
            )
        );
        assert!(host_column.add_if_not_exists);

        let decimal_column = &add_columns.add_columns[4];
        assert_eq!(
            ConcreteDataType::decimal128_datatype(38, 10),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    decimal_column.column_def.as_ref().unwrap().data_type,
                    decimal_column
                        .column_def
                        .as_ref()
                        .unwrap()
                        .datatype_extension
                )
                .unwrap()
            )
        );
        assert!(host_column.add_if_not_exists);
    }

    fn mock_insert_batch() -> (Vec<Column>, u32) {
        let row_count = 2;

        let host_vals = Values {
            string_values: vec!["host1".to_string(), "host2".to_string()],
            ..Default::default()
        };
        let host_column = Column {
            column_name: "host".to_string(),
            semantic_type: SemanticType::Tag as i32,
            values: Some(host_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::String as i32,
            ..Default::default()
        };

        let cpu_vals = Values {
            f64_values: vec![0.31],
            ..Default::default()
        };
        let cpu_column = Column {
            column_name: "cpu".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(cpu_vals),
            null_mask: vec![2],
            datatype: ColumnDataType::Float64 as i32,
            ..Default::default()
        };

        let mem_vals = Values {
            f64_values: vec![0.1],
            ..Default::default()
        };
        let mem_column = Column {
            column_name: "memory".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(mem_vals),
            null_mask: vec![1],
            datatype: ColumnDataType::Float64 as i32,
            ..Default::default()
        };

        let time_vals = Values {
            time_millisecond_values: vec![100, 101],
            ..Default::default()
        };
        let time_column = Column {
            column_name: "time".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(time_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::TimeMillisecond as i32,
            ..Default::default()
        };

        let interval1 = IntervalMonthDayNano {
            months: 1,
            days: 2,
            nanoseconds: 3,
        };
        let interval2 = IntervalMonthDayNano {
            months: 4,
            days: 5,
            nanoseconds: 6,
        };
        let interval_vals = Values {
            interval_month_day_nano_values: vec![interval1, interval2],
            ..Default::default()
        };
        let interval_column = Column {
            column_name: "interval".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(interval_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::IntervalMonthDayNano as i32,
            ..Default::default()
        };

        let ts_vals = Values {
            timestamp_millisecond_values: vec![100, 101],
            ..Default::default()
        };
        let ts_column = Column {
            column_name: "ts".to_string(),
            semantic_type: SemanticType::Timestamp as i32,
            values: Some(ts_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::TimestampMillisecond as i32,
            ..Default::default()
        };
        let decimal_vals = Values {
            decimal128_values: vec![Decimal128 { hi: 0, lo: 123 }, Decimal128 { hi: 0, lo: 456 }],
            ..Default::default()
        };
        let decimal_column = Column {
            column_name: "decimals".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(decimal_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::Decimal128 as i32,
            datatype_extension: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::DecimalType(DecimalTypeExtension {
                    precision: 38,
                    scale: 10,
                })),
            }),
            options: None,
        };

        (
            vec![
                host_column,
                cpu_column,
                mem_column,
                time_column,
                interval_column,
                ts_column,
                decimal_column,
            ],
            row_count,
        )
    }
}
