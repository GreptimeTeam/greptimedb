use std::collections::HashSet;
use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Deref,
    sync::Arc,
};

use api::{
    helper::ColumnDataTypeWrapper,
    v1::{codec::InsertBatch, column::Values, Column},
};
use common_base::BitVec;
use common_time::timestamp::Timestamp;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use datatypes::{data_type::ConcreteDataType, value::Value, vectors::VectorBuilder};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableId;
use table::{
    requests::{AddColumnRequest, AlterKind, AlterTableRequest, CreateTableRequest, InsertRequest},
    Table,
};

use crate::error::{self, ColumnNotFoundSnafu, DecodeInsertSnafu, IllegalInsertDataSnafu, Result};

pub fn find_new_columns(
    schema: &SchemaRef,
    insert_batches: &[InsertBatch],
) -> Result<Option<Vec<AddColumnRequest>>> {
    let mut requests = Vec::default();
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
                let datatype_wrapper = ColumnDataTypeWrapper::try_new(*datatype)
                    .context(error::ColumnDataTypeSnafu)?;
                let column_schema = ColumnSchema::new(column_name, datatype_wrapper.into(), true);

                requests.push(AddColumnRequest {
                    column_schema,
                    is_key: *semantic_type == 0, // It's tag
                });
                new_columns.insert(column_name.to_string());
            }
        }
    }

    if requests.is_empty() {
        Ok(None)
    } else {
        Ok(Some(requests))
    }
}

pub fn build_alter_table_request(
    table_name: &str,
    columns: Vec<AddColumnRequest>,
) -> Result<AlterTableRequest> {
    Ok(AlterTableRequest {
        catalog_name: None,
        schema_name: None,
        table_name: table_name.to_string(),
        alter_kind: AlterKind::AddColumns { columns },
    })
}

/// Try to build create table request from insert data.
pub fn build_create_table_request(
    table_id: TableId,
    table_name: &str,
    insert_batches: &[InsertBatch],
) -> Result<CreateTableRequest> {
    let mut new_columns: HashSet<String> = HashSet::default();
    let mut column_schemas = Vec::default();
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
                let datatype_wrapper = ColumnDataTypeWrapper::try_new(*datatype)
                    .context(error::ColumnDataTypeSnafu)?;
                let column_schema = ColumnSchema::new(column_name, datatype_wrapper.into(), true);
                column_schemas.push(column_schema);
                new_columns.insert(column_name.to_string());

                match *semantic_type {
                    // Tag
                    0 => primary_key_indices.push(column_schemas.len() - 1),
                    // Timestamp
                    2 => {
                        ensure!(
                            timestamp_index == usize::MAX,
                            error::DuplicatedTimestampColumnSnafu {
                                exists: &columns[timestamp_index].column_name,
                                duplicated: column_name,
                            }
                        );
                        timestamp_index = column_schemas.len() - 1;
                    }
                    _ => continue,
                }
            }
        }

        ensure!(
            timestamp_index != usize::MAX,
            error::MissingTimestampColumnSnafu
        );

        let schema = Arc::new(
            SchemaBuilder::try_from(column_schemas)
                .unwrap()
                .timestamp_index(timestamp_index)
                .build()
                .context(error::CreateSchemaSnafu)?,
        );

        return Ok(CreateTableRequest {
            id: table_id,
            catalog_name: None,
            schema_name: None,
            table_name: table_name.to_string(),
            desc: None,
            schema,
            create_if_not_exists: true,
            primary_key_indices,
            table_options: HashMap::new(),
        });
    }

    error::IllegalInsertDataSnafu.fail()
}

pub fn insertion_expr_to_request(
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
        table_name: table_name.to_string(),
        columns_values,
    })
}

#[inline]
pub fn insert_batches(bytes_vec: Vec<Vec<u8>>) -> Result<Vec<InsertBatch>> {
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
    use std::{any::Any, sync::Arc};

    use api::v1::{
        codec::InsertBatch,
        column::{self, Values},
        insert_expr, Column,
    };
    use common_base::BitVec;
    use common_query::prelude::Expr;
    use common_recordbatch::SendableRecordBatchStream;
    use common_time::timestamp::Timestamp;
    use datatypes::{
        data_type::ConcreteDataType,
        schema::{ColumnSchema, SchemaBuilder, SchemaRef},
        value::Value,
    };
    use table::error::Result as TableResult;
    use table::Table;

    use super::{convert_values, insert_batches, insertion_expr_to_request, is_null};

    #[test]
    fn test_insertion_expr_to_request() {
        let table: Arc<dyn Table> = Arc::new(DemoTable {});

        let values = insert_expr::Values {
            values: mock_insert_batches(),
        };
        let insert_batches = insert_batches(values.values).unwrap();
        let insert_req = insertion_expr_to_request("demo", insert_batches, table).unwrap();

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
                ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), true),
            ];

            Arc::new(
                SchemaBuilder::try_from(column_schemas)
                    .unwrap()
                    .timestamp_index(3)
                    .build()
                    .unwrap(),
            )
        }
        async fn scan(
            &self,
            _projection: &Option<Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> TableResult<SendableRecordBatchStream> {
            unimplemented!();
        }
    }

    fn mock_insert_batches() -> Vec<Vec<u8>> {
        const SEMANTIC_TAG: i32 = 0;
        const SEMANTIC_FIELD: i32 = 1;
        const SEMANTIC_TS: i32 = 2;

        let row_count = 2;

        let host_vals = column::Values {
            string_values: vec!["host1".to_string(), "host2".to_string()],
            ..Default::default()
        };
        let host_column = Column {
            column_name: "host".to_string(),
            semantic_type: SEMANTIC_TAG,
            values: Some(host_vals),
            null_mask: vec![0],
            ..Default::default()
        };

        let cpu_vals = column::Values {
            f64_values: vec![0.31],
            ..Default::default()
        };
        let cpu_column = Column {
            column_name: "cpu".to_string(),
            semantic_type: SEMANTIC_FIELD,
            values: Some(cpu_vals),
            null_mask: vec![2],
            ..Default::default()
        };

        let mem_vals = column::Values {
            f64_values: vec![0.1],
            ..Default::default()
        };
        let mem_column = Column {
            column_name: "memory".to_string(),
            semantic_type: SEMANTIC_FIELD,
            values: Some(mem_vals),
            null_mask: vec![1],
            ..Default::default()
        };

        let ts_vals = column::Values {
            ts_millis_values: vec![100, 101],
            ..Default::default()
        };
        let ts_column = Column {
            column_name: "ts".to_string(),
            semantic_type: SEMANTIC_TS,
            values: Some(ts_vals),
            null_mask: vec![0],
            datatype: 15,
        };

        let insert_batch = InsertBatch {
            columns: vec![host_column, cpu_column, mem_column, ts_column],
            row_count,
        };
        vec![insert_batch.into()]
    }
}
