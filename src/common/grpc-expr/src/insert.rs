// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::codec::InsertBatch;
use api::v1::column::{SemanticType, Values};
use api::v1::{AddColumn, AddColumns, Column, ColumnDataType, ColumnDef, CreateExpr};
use common_base::BitVec;
use common_time::timestamp::Timestamp;
use common_time::{Date, DateTime};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::{ValueRef, VectorRef};
use datatypes::schema::SchemaRef;
use datatypes::value::Value;
use datatypes::vectors::VectorBuilder;
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableId;
use table::requests::{AddColumnRequest, AlterKind, AlterTableRequest, InsertRequest};
use table::Table;

use crate::error::{
    ColumnDataTypeSnafu, ColumnNotFoundSnafu, CreateVectorSnafu, DecodeInsertSnafu,
    DuplicatedTimestampColumnSnafu, IllegalInsertDataSnafu, InvalidColumnProtoSnafu,
    MissingTimestampColumnSnafu, Result,
};
const TAG_SEMANTIC_TYPE: i32 = SemanticType::Tag as i32;
const TIMESTAMP_SEMANTIC_TYPE: i32 = SemanticType::Timestamp as i32;

#[inline]
fn build_column_def(column_name: &str, datatype: i32, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: column_name.to_string(),
        datatype,
        is_nullable: nullable,
        default_constraint: None,
    }
}

pub fn find_new_columns(
    schema: &SchemaRef,
    insert_batches: &[InsertBatch],
) -> Result<Option<AddColumns>> {
    let mut columns_to_add = Vec::default();
    let mut new_columns: HashSet<String> = HashSet::default();

    for InsertBatch { columns, row_count } in insert_batches {
        if *row_count == 0 || columns.is_empty() {
            continue;
        }

        for Column {
            column_name,
            semantic_type,
            datatype,
            ..
        } in columns
        {
            if schema.column_schema_by_name(column_name).is_none()
                && !new_columns.contains(column_name)
            {
                let column_def = Some(build_column_def(column_name, *datatype, true));
                columns_to_add.push(AddColumn {
                    column_def,
                    is_key: *semantic_type == TAG_SEMANTIC_TYPE,
                });
                new_columns.insert(column_name.to_string());
            }
        }
    }

    if columns_to_add.is_empty() {
        Ok(None)
    } else {
        Ok(Some(AddColumns {
            add_columns: columns_to_add,
        }))
    }
}

/// Build a alter table rqeusts that adding new columns.
#[inline]
pub fn build_alter_table_request(
    table_name: &str,
    columns: Vec<AddColumnRequest>,
) -> AlterTableRequest {
    AlterTableRequest {
        catalog_name: None,
        schema_name: None,
        table_name: table_name.to_string(),
        alter_kind: AlterKind::AddColumns { columns },
    }
}

pub fn column_to_vector(column: &Column, rows: u32) -> Result<VectorRef> {
    let wrapper = ColumnDataTypeWrapper::try_new(column.datatype).context(ColumnDataTypeSnafu)?;
    let column_datatype = wrapper.datatype();

    let rows = rows as usize;
    let mut vector = VectorBuilder::with_capacity(wrapper.into(), rows);

    if let Some(values) = &column.values {
        let values = collect_column_values(column_datatype, values);
        let mut values_iter = values.into_iter();

        let null_mask = BitVec::from_slice(&column.null_mask);
        let mut nulls_iter = null_mask.iter().by_vals().fuse();

        for i in 0..rows {
            if let Some(true) = nulls_iter.next() {
                vector.push_null();
            } else {
                let value_ref = values_iter.next().context(InvalidColumnProtoSnafu {
                    err_msg: format!(
                        "value not found at position {} of column {}",
                        i, &column.column_name
                    ),
                })?;
                vector.try_push_ref(value_ref).context(CreateVectorSnafu)?;
            }
        }
    } else {
        (0..rows).for_each(|_| vector.push_null());
    }
    Ok(vector.finish())
}

fn collect_column_values(column_datatype: ColumnDataType, values: &Values) -> Vec<ValueRef> {
    macro_rules! collect_values {
        ($value: expr, $mapper: expr) => {
            $value.iter().map($mapper).collect::<Vec<ValueRef>>()
        };
    }

    match column_datatype {
        ColumnDataType::Boolean => collect_values!(values.bool_values, |v| ValueRef::from(*v)),
        ColumnDataType::Int8 => collect_values!(values.i8_values, |v| ValueRef::from(*v as i8)),
        ColumnDataType::Int16 => {
            collect_values!(values.i16_values, |v| ValueRef::from(*v as i16))
        }
        ColumnDataType::Int32 => {
            collect_values!(values.i32_values, |v| ValueRef::from(*v))
        }
        ColumnDataType::Int64 => {
            collect_values!(values.i64_values, |v| ValueRef::from(*v as i64))
        }
        ColumnDataType::Uint8 => {
            collect_values!(values.u8_values, |v| ValueRef::from(*v as u8))
        }
        ColumnDataType::Uint16 => {
            collect_values!(values.u16_values, |v| ValueRef::from(*v as u16))
        }
        ColumnDataType::Uint32 => {
            collect_values!(values.u32_values, |v| ValueRef::from(*v))
        }
        ColumnDataType::Uint64 => {
            collect_values!(values.u64_values, |v| ValueRef::from(*v as u64))
        }
        ColumnDataType::Float32 => collect_values!(values.f32_values, |v| ValueRef::from(*v)),
        ColumnDataType::Float64 => collect_values!(values.f64_values, |v| ValueRef::from(*v)),
        ColumnDataType::Binary => {
            collect_values!(values.binary_values, |v| ValueRef::from(v.as_slice()))
        }
        ColumnDataType::String => {
            collect_values!(values.string_values, |v| ValueRef::from(v.as_str()))
        }
        ColumnDataType::Date => {
            collect_values!(values.date_values, |v| ValueRef::Date(Date::new(*v)))
        }
        ColumnDataType::Datetime => {
            collect_values!(values.datetime_values, |v| ValueRef::DateTime(
                DateTime::new(*v)
            ))
        }
        ColumnDataType::Timestamp => {
            collect_values!(values.ts_millis_values, |v| ValueRef::Timestamp(
                Timestamp::from_millis(*v)
            ))
        }
    }
}

/// Try to build create table request from insert data.
pub fn build_create_expr_from_insertion(
    catalog_name: &str,
    schema_name: &str,
    table_id: Option<TableId>,
    table_name: &str,
    insert_batches: &[InsertBatch],
) -> Result<CreateExpr> {
    let mut new_columns: HashSet<String> = HashSet::default();
    let mut column_defs = Vec::default();
    let mut primary_key_indices = Vec::default();
    let mut timestamp_index = usize::MAX;

    for InsertBatch { columns, row_count } in insert_batches {
        if *row_count == 0 || columns.is_empty() {
            continue;
        }

        for Column {
            column_name,
            semantic_type,
            datatype,
            ..
        } in columns
        {
            if !new_columns.contains(column_name) {
                let mut is_nullable = true;
                match *semantic_type {
                    TAG_SEMANTIC_TYPE => primary_key_indices.push(column_defs.len()),
                    TIMESTAMP_SEMANTIC_TYPE => {
                        ensure!(
                            timestamp_index == usize::MAX,
                            DuplicatedTimestampColumnSnafu {
                                exists: &columns[timestamp_index].column_name,
                                duplicated: column_name,
                            }
                        );
                        timestamp_index = column_defs.len();
                        // Timestamp column must not be null.
                        is_nullable = false;
                    }
                    _ => {}
                }

                let column_def = build_column_def(column_name, *datatype, is_nullable);
                column_defs.push(column_def);
                new_columns.insert(column_name.to_string());
            }
        }

        ensure!(
            timestamp_index != usize::MAX,
            MissingTimestampColumnSnafu { msg: table_name }
        );
        let timestamp_field_name = columns[timestamp_index].column_name.clone();

        let primary_keys = primary_key_indices
            .iter()
            .map(|idx| columns[*idx].column_name.clone())
            .collect::<Vec<_>>();

        let expr = CreateExpr {
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            table_name: table_name.to_string(),
            desc: Some("Created on insertion".to_string()),
            column_defs,
            time_index: timestamp_field_name,
            primary_keys,
            create_if_not_exists: true,
            table_options: Default::default(),
            table_id,
            region_ids: vec![0], // TODO:(hl): region id should be allocated by frontend
        };

        return Ok(expr);
    }

    IllegalInsertDataSnafu.fail()
}

pub fn insertion_expr_to_request(
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    insert_batches: Vec<InsertBatch>,
    table: Arc<dyn Table>,
) -> Result<InsertRequest> {
    let schema = table.schema();
    let mut columns_builders = HashMap::with_capacity(schema.column_schemas().len());

    for InsertBatch { columns, row_count } in insert_batches {
        for Column {
            column_name,
            values,
            null_mask,
            ..
        } in columns
        {
            let values = match values {
                Some(vals) => vals,
                None => continue,
            };

            let column = column_name.clone();
            let vector_builder = match columns_builders.entry(column) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let column_schema = schema.column_schema_by_name(&column_name).context(
                        ColumnNotFoundSnafu {
                            column_name: &column_name,
                            table_name,
                        },
                    )?;
                    let data_type = &column_schema.data_type;
                    entry.insert(VectorBuilder::with_capacity(
                        data_type.clone(),
                        row_count as usize,
                    ))
                }
            };
            add_values_to_builder(vector_builder, values, row_count as usize, null_mask)?;
        }
    }
    let columns_values = columns_builders
        .into_iter()
        .map(|(column_name, mut vector_builder)| (column_name, vector_builder.finish()))
        .collect();

    Ok(InsertRequest {
        catalog_name: catalog_name.to_string(),
        schema_name: schema_name.to_string(),
        table_name: table_name.to_string(),
        columns_values,
    })
}

#[inline]
pub fn insert_batches(bytes_vec: &[Vec<u8>]) -> Result<Vec<InsertBatch>> {
    bytes_vec
        .iter()
        .map(|bytes| bytes.deref().try_into().context(DecodeInsertSnafu))
        .collect()
}

fn add_values_to_builder(
    builder: &mut VectorBuilder,
    values: Values,
    row_count: usize,
    null_mask: Vec<u8>,
) -> Result<()> {
    let data_type = builder.data_type();
    let values = convert_values(&data_type, values);

    if null_mask.is_empty() {
        ensure!(values.len() == row_count, IllegalInsertDataSnafu);

        values.iter().for_each(|value| {
            builder.push(value);
        });
    } else {
        let null_mask = BitVec::from_vec(null_mask);
        ensure!(
            null_mask.count_ones() + values.len() == row_count,
            IllegalInsertDataSnafu
        );

        let mut idx_of_values = 0;
        for idx in 0..row_count {
            match is_null(&null_mask, idx) {
                Some(true) => builder.push(&Value::Null),
                _ => {
                    builder.push(&values[idx_of_values]);
                    idx_of_values += 1
                }
            }
        }
    }
    Ok(())
}

fn convert_values(data_type: &ConcreteDataType, values: Values) -> Vec<Value> {
    // TODO(fys): use macros to optimize code
    match data_type {
        ConcreteDataType::Int64(_) => values
            .i64_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Float64(_) => values
            .f64_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::String(_) => values
            .string_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Boolean(_) => values
            .bool_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Int8(_) => values.i8_values.into_iter().map(|val| val.into()).collect(),
        ConcreteDataType::Int16(_) => values
            .i16_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Int32(_) => values
            .i32_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::UInt8(_) => values.u8_values.into_iter().map(|val| val.into()).collect(),
        ConcreteDataType::UInt16(_) => values
            .u16_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::UInt32(_) => values
            .u32_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::UInt64(_) => values
            .u64_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Float32(_) => values
            .f32_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Binary(_) => values
            .binary_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::DateTime(_) => values
            .i64_values
            .into_iter()
            .map(|v| Value::DateTime(v.into()))
            .collect(),
        ConcreteDataType::Date(_) => values
            .i32_values
            .into_iter()
            .map(|v| Value::Date(v.into()))
            .collect(),
        ConcreteDataType::Timestamp(_) => values
            .ts_millis_values
            .into_iter()
            .map(|v| Value::Timestamp(Timestamp::from_millis(v)))
            .collect(),
        ConcreteDataType::Null(_) => unreachable!(),
        ConcreteDataType::List(_) => unreachable!(),
    }
}

fn is_null(null_mask: &BitVec, idx: usize) -> Option<bool> {
    null_mask.get(idx).as_deref().copied()
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use api::helper::ColumnDataTypeWrapper;
    use api::v1::codec::InsertBatch;
    use api::v1::column::{self, SemanticType, Values};
    use api::v1::{insert_expr, Column, ColumnDataType};
    use common_base::BitVec;
    use common_query::physical_plan::PhysicalPlanRef;
    use common_query::prelude::Expr;
    use common_time::timestamp::Timestamp;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
    use datatypes::value::Value;
    use snafu::ResultExt;
    use table::error::Result as TableResult;
    use table::metadata::TableInfoRef;
    use table::Table;

    use super::{
        build_create_expr_from_insertion, convert_values, find_new_columns, insert_batches,
        insertion_expr_to_request, is_null, TAG_SEMANTIC_TYPE, TIMESTAMP_SEMANTIC_TYPE,
    };
    use crate::error;
    use crate::error::ColumnDataTypeSnafu;

    #[inline]
    fn build_column_schema(
        column_name: &str,
        datatype: i32,
        nullable: bool,
    ) -> error::Result<ColumnSchema> {
        let datatype_wrapper =
            ColumnDataTypeWrapper::try_new(datatype).context(ColumnDataTypeSnafu)?;

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

        assert!(build_create_expr_from_insertion("", "", table_id, table_name, &[]).is_err());

        let mock_batch_bytes = mock_insert_batches();
        let insert_batches = insert_batches(&mock_batch_bytes).unwrap();

        let create_expr =
            build_create_expr_from_insertion("", "", table_id, table_name, &insert_batches)
                .unwrap();

        assert_eq!(table_id, create_expr.table_id);
        assert_eq!(table_name, create_expr.table_name);
        assert_eq!(Some("Created on insertion".to_string()), create_expr.desc);
        assert_eq!(
            vec![create_expr.column_defs[0].name.clone()],
            create_expr.primary_keys
        );

        let column_defs = create_expr.column_defs;
        assert_eq!(column_defs[3].name, create_expr.time_index);
        assert_eq!(4, column_defs.len());

        assert_eq!(
            ConcreteDataType::string_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "host")
                        .unwrap()
                        .datatype
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
                        .datatype
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
                        .datatype
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::timestamp_millis_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "ts")
                        .unwrap()
                        .datatype
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

        let mock_insert_bytes = mock_insert_batches();
        let insert_batches = insert_batches(&mock_insert_bytes).unwrap();
        let add_columns = find_new_columns(&schema, &insert_batches).unwrap().unwrap();

        assert_eq!(2, add_columns.add_columns.len());
        let host_column = &add_columns.add_columns[0];
        assert!(host_column.is_key);

        assert_eq!(
            ConcreteDataType::string_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(host_column.column_def.as_ref().unwrap().datatype)
                    .unwrap()
            )
        );

        let memory_column = &add_columns.add_columns[1];
        assert!(!memory_column.is_key);

        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(memory_column.column_def.as_ref().unwrap().datatype)
                    .unwrap()
            )
        );
    }

    #[test]
    fn test_insertion_expr_to_request() {
        let table: Arc<dyn Table> = Arc::new(DemoTable {});

        let values = insert_expr::Values {
            values: mock_insert_batches(),
        };
        let insert_batches = insert_batches(&values.values).unwrap();
        let insert_req =
            insertion_expr_to_request("greptime", "public", "demo", insert_batches, table).unwrap();

        assert_eq!("greptime", insert_req.catalog_name);
        assert_eq!("public", insert_req.schema_name);
        assert_eq!("demo", insert_req.table_name);

        let host = insert_req.columns_values.get("host").unwrap();
        assert_eq!(Value::String("host1".into()), host.get(0));
        assert_eq!(Value::String("host2".into()), host.get(1));

        let cpu = insert_req.columns_values.get("cpu").unwrap();
        assert_eq!(Value::Float64(0.31.into()), cpu.get(0));
        assert_eq!(Value::Null, cpu.get(1));

        let memory = insert_req.columns_values.get("memory").unwrap();
        assert_eq!(Value::Null, memory.get(0));
        assert_eq!(Value::Float64(0.1.into()), memory.get(1));

        let ts = insert_req.columns_values.get("ts").unwrap();
        assert_eq!(Value::Timestamp(Timestamp::from_millis(100)), ts.get(0));
        assert_eq!(Value::Timestamp(Timestamp::from_millis(101)), ts.get(1));
    }

    #[test]
    fn test_convert_values() {
        let data_type = ConcreteDataType::float64_datatype();
        let values = Values {
            f64_values: vec![0.1, 0.2, 0.3],
            ..Default::default()
        };

        let result = convert_values(&data_type, values);

        assert_eq!(
            vec![
                Value::Float64(0.1.into()),
                Value::Float64(0.2.into()),
                Value::Float64(0.3.into())
            ],
            result
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

    struct DemoTable;

    #[async_trait::async_trait]
    impl Table for DemoTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            let column_schemas = vec![
                ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
                ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
                ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
                ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), true)
                    .with_time_index(true),
            ];

            Arc::new(
                SchemaBuilder::try_from(column_schemas)
                    .unwrap()
                    .build()
                    .unwrap(),
            )
        }

        fn table_info(&self) -> TableInfoRef {
            unimplemented!()
        }

        async fn scan(
            &self,
            _projection: &Option<Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> TableResult<PhysicalPlanRef> {
            unimplemented!();
        }
    }

    fn mock_insert_batches() -> Vec<Vec<u8>> {
        let row_count = 2;

        let host_vals = column::Values {
            string_values: vec!["host1".to_string(), "host2".to_string()],
            ..Default::default()
        };
        let host_column = Column {
            column_name: "host".to_string(),
            semantic_type: TAG_SEMANTIC_TYPE,
            values: Some(host_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::String as i32,
        };

        let cpu_vals = column::Values {
            f64_values: vec![0.31],
            ..Default::default()
        };
        let cpu_column = Column {
            column_name: "cpu".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(cpu_vals),
            null_mask: vec![2],
            datatype: ColumnDataType::Float64 as i32,
        };

        let mem_vals = column::Values {
            f64_values: vec![0.1],
            ..Default::default()
        };
        let mem_column = Column {
            column_name: "memory".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(mem_vals),
            null_mask: vec![1],
            datatype: ColumnDataType::Float64 as i32,
        };

        let ts_vals = column::Values {
            ts_millis_values: vec![100, 101],
            ..Default::default()
        };
        let ts_column = Column {
            column_name: "ts".to_string(),
            semantic_type: TIMESTAMP_SEMANTIC_TYPE,
            values: Some(ts_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::Timestamp as i32,
        };

        let insert_batch = InsertBatch {
            columns: vec![host_column, cpu_column, mem_column, ts_column],
            row_count,
        };
        vec![insert_batch.into()]
    }
}
