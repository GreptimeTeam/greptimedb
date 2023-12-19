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

use api::helper;
use api::v1::column::Values;
use api::v1::{AddColumns, Column, CreateTableExpr};
use common_base::BitVec;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::prelude::VectorRef;
use datatypes::schema::SchemaRef;
use snafu::{ensure, ResultExt};
use table::engine::TableReference;
use table::metadata::TableId;

use crate::error::{CreateVectorSnafu, Result, UnexpectedValuesLengthSnafu};
use crate::util;
use crate::util::ColumnExpr;

pub fn find_new_columns(schema: &SchemaRef, columns: &[Column]) -> Result<Option<AddColumns>> {
    let column_exprs = ColumnExpr::from_columns(columns);
    util::extract_new_columns(schema, column_exprs)
}

/// Try to build create table request from insert data.
pub fn build_create_expr_from_insertion(
    catalog_name: &str,
    schema_name: &str,
    table_id: Option<TableId>,
    table_name: &str,
    columns: &[Column],
    engine: &str,
) -> Result<CreateTableExpr> {
    let table_name = TableReference::full(catalog_name, schema_name, table_name);
    let column_exprs = ColumnExpr::from_columns(columns);
    util::build_create_table_expr(
        table_id,
        &table_name,
        column_exprs,
        engine,
        "Created on insertion",
    )
}

pub(crate) fn add_values_to_builder(
    data_type: ConcreteDataType,
    values: Values,
    row_count: usize,
    null_mask: Vec<u8>,
) -> Result<VectorRef> {
    if null_mask.is_empty() {
        Ok(helper::pb_values_to_vector_ref(&data_type, values))
    } else {
        let builder = &mut data_type.create_mutable_vector(row_count);
        let values = helper::pb_values_to_values(&data_type, values);
        let null_mask = BitVec::from_vec(null_mask);
        ensure!(
            null_mask.count_ones() + values.len() == row_count,
            UnexpectedValuesLengthSnafu {
                reason: "If null_mask is not empty, the sum of the number of nulls and the length of values must be equal to row_count."
            }
        );

        let mut idx_of_values = 0;
        for idx in 0..row_count {
            match is_null(&null_mask, idx) {
                Some(true) => builder.push_null(),
                _ => {
                    builder
                        .try_push_value_ref(values[idx_of_values].as_value_ref())
                        .context(CreateVectorSnafu)?;
                    idx_of_values += 1
                }
            }
        }
        Ok(builder.to_vector())
    }
}

fn is_null(null_mask: &BitVec, idx: usize) -> Option<bool> {
    null_mask.get(idx).as_deref().copied()
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
    use common_base::BitVec;
    use common_catalog::consts::MITO_ENGINE;
    use common_time::interval::IntervalUnit;
    use common_time::timestamp::TimeUnit;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use snafu::ResultExt;

    use super::*;
    use crate::error;
    use crate::error::ColumnDataTypeSnafu;
    use crate::insert::find_new_columns;

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
        assert_eq!(column_defs[6].name, create_expr.time_index);
        assert_eq!(8, column_defs.len());

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
            ConcreteDataType::duration_millisecond_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "duration")
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
                    decimal_column.datatype_extension.clone(),
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

        assert!(find_new_columns(&schema, &[]).unwrap().is_none());

        let insert_batch = mock_insert_batch();

        let add_columns = find_new_columns(&schema, &insert_batch.0).unwrap().unwrap();

        assert_eq!(6, add_columns.add_columns.len());
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

        let duration_column = &add_columns.add_columns[4];

        assert_eq!(
            ConcreteDataType::duration_millisecond_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    duration_column.column_def.as_ref().unwrap().data_type,
                    None
                )
                .unwrap()
            )
        );

        let decimal_column = &add_columns.add_columns[5];
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
                        .clone()
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn test_is_null() {
        let null_mask = BitVec::from_slice(&[0b0000_0001, 0b0000_1000]);

        assert_eq!(Some(true), is_null(&null_mask, 0));
        assert_eq!(Some(false), is_null(&null_mask, 1));
        assert_eq!(Some(false), is_null(&null_mask, 10));
        assert_eq!(Some(true), is_null(&null_mask, 11));
        assert_eq!(Some(false), is_null(&null_mask, 12));

        assert_eq!(None, is_null(&null_mask, 16));
        assert_eq!(None, is_null(&null_mask, 99));
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

        let duration_vals = Values {
            duration_millisecond_values: vec![100, 101],
            ..Default::default()
        };
        let duration_column = Column {
            column_name: "duration".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(duration_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::DurationMillisecond as i32,
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
        };

        (
            vec![
                host_column,
                cpu_column,
                mem_column,
                time_column,
                interval_column,
                duration_column,
                ts_column,
                decimal_column,
            ],
            row_count,
        )
    }
}
