use std::{collections::HashMap, sync::Arc};

use api::v1::{column::Values, Column, InsertBatch, InsertExpr};
use datatypes::{data_type::ConcreteDataType, value::Value, vectors::VectorBuilder};
use snafu::{ensure, OptionExt, ResultExt};
use table::{requests::InsertRequest, Table};

use crate::error::{ColumnNotFoundSnafu, DecodeInsertSnafu, IllegalInsertDataSnafu, Result};

pub fn insertion_expr_to_request(
    insert: InsertExpr,
    table: Arc<dyn Table>,
) -> Result<InsertRequest> {
    let schema = table.schema();
    let table_name = &insert.table_name;
    let mut columns_builders = HashMap::with_capacity(schema.column_schemas().len());
    let insert_batches = insert_batches(insert.values)?;

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

            let column_schema =
                schema
                    .column_schema_by_name(&column_name)
                    .context(ColumnNotFoundSnafu {
                        column_name: &column_name,
                        table_name,
                    })?;
            let data_type = &column_schema.data_type;

            let vector_builder = columns_builders.entry(column_name).or_insert_with(|| {
                VectorBuilder::with_capacity(data_type.clone(), row_count as usize)
            });

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

fn insert_batches(bytes_vec: Vec<Vec<u8>>) -> Result<Vec<InsertBatch>> {
    let mut insert_batches = Vec::with_capacity(bytes_vec.len());

    for bytes in bytes_vec {
        insert_batches.push(bytes.try_into().context(DecodeInsertSnafu)?);
    }
    Ok(insert_batches)
}

fn add_values_to_builder(
    builder: &mut VectorBuilder,
    values: Values,
    row_count: usize,
    null_mask: impl Into<BitSet>,
) -> Result<()> {
    let data_type = builder.data_type();
    let null_mask = null_mask.into();
    let values = convert_values(&data_type, values);

    if null_mask.len() == 0 {
        ensure!(values.len() == row_count, IllegalInsertDataSnafu);

        values.iter().for_each(|value| {
            builder.push(value);
        });
    } else {
        ensure!(
            null_mask.set_count() + values.len() == row_count,
            IllegalInsertDataSnafu
        );

        let mut idx_of_values = 0;
        for idx in 0..row_count {
            if is_null(&null_mask, idx) {
                builder.push(&Value::Null);
            } else {
                builder.push(&values[idx_of_values]);
                idx_of_values += 1;
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
        _ => unimplemented!(),
    }
}

fn is_null(null_mask: &BitSet, idx: usize) -> bool {
    debug_assert!(idx < null_mask.len, "idx should be less than null_mask.len");

    matches!(null_mask.get_bit(idx), Some(true))
}

// TOOD(fys): move BitSet to better location
struct BitSet {
    buffer: Vec<u8>,
    len: usize,
}

impl BitSet {
    fn len(&self) -> usize {
        self.len
    }

    fn set_count(&self) -> usize {
        (0..self.len)
            .into_iter()
            .filter(|&i| matches!(self.get_bit(i), Some(true)))
            .count()
    }

    fn get_bit(&self, idx: usize) -> Option<bool> {
        if idx >= self.len {
            return None;
        }

        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        Some((self.buffer[byte_idx] >> bit_idx) & 1 != 0)
    }
}

impl From<Vec<u8>> for BitSet {
    fn from(data: Vec<u8>) -> Self {
        BitSet {
            len: data.len() << 3,
            buffer: data,
        }
    }
}

impl From<&[u8]> for BitSet {
    fn from(data: &[u8]) -> Self {
        BitSet {
            buffer: data.into(),
            len: data.len() << 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{any::Any, sync::Arc};

    use api::v1::{
        column::{self, Values},
        Column, InsertBatch, InsertExpr,
    };
    use common_query::prelude::Expr;
    use common_recordbatch::SendableRecordBatchStream;
    use datatypes::{
        data_type::ConcreteDataType,
        schema::{ColumnSchema, Schema, SchemaRef},
        value::Value,
    };
    use table::error::Result as TableResult;
    use table::Table;

    use crate::server::grpc::insert::{convert_values, insertion_expr_to_request, is_null, BitSet};

    #[test]
    fn test_insertion_expr_to_request() {
        let insert_expr = InsertExpr {
            table_name: "demo".to_string(),
            values: mock_insert_batches(),
        };
        let table: Arc<dyn Table> = Arc::new(DemoTable {});

        let insert_req = insertion_expr_to_request(insert_expr, table).unwrap();

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
        assert_eq!(Value::Int64(100), ts.get(0));
        assert_eq!(Value::Int64(101), ts.get(1));
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
        let null_mask: BitSet = vec![0b0000_0001, 0b0000_1000].into();

        assert!(is_null(&null_mask, 0));
        assert!(!is_null(&null_mask, 1));
        assert!(!is_null(&null_mask, 10));
        assert!(is_null(&null_mask, 11));
        assert!(!is_null(&null_mask, 12));
    }

    #[test]
    fn test_bit_set() {
        let bit_set: BitSet = vec![0b0000_0001, 0b0000_1000].into();

        assert!(bit_set.get_bit(0).unwrap());
        assert!(!bit_set.get_bit(1).unwrap());
        assert!(!bit_set.get_bit(10).unwrap());
        assert!(bit_set.get_bit(11).unwrap());
        assert!(!bit_set.get_bit(12).unwrap());

        assert!(bit_set.get_bit(16).is_none());

        assert_eq!(2, bit_set.set_count());
        assert_eq!(16, bit_set.len());

        let bit_set: BitSet = vec![0b0000_0000, 0b0000_0000].into();
        assert_eq!(0, bit_set.set_count());
        assert_eq!(16, bit_set.len());

        let bit_set: BitSet = vec![0b1111_1111, 0b1111_1111].into();
        assert_eq!(16, bit_set.set_count());

        let bit_set: BitSet = vec![].into();
        assert_eq!(0, bit_set.len());
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
                ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), true),
            ];

            Arc::new(Schema::with_timestamp_index(column_schemas, 3).unwrap())
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
        const SEMANTIC_FEILD: i32 = 1;
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
        };

        let cpu_vals = column::Values {
            f64_values: vec![0.31],
            ..Default::default()
        };
        let cpu_column = Column {
            column_name: "cpu".to_string(),
            semantic_type: SEMANTIC_FEILD,
            values: Some(cpu_vals),
            null_mask: vec![2],
        };

        let mem_vals = column::Values {
            f64_values: vec![0.1],
            ..Default::default()
        };
        let mem_column = Column {
            column_name: "memory".to_string(),
            semantic_type: SEMANTIC_FEILD,
            values: Some(mem_vals),
            null_mask: vec![1],
        };

        let ts_vals = column::Values {
            i64_values: vec![100, 101],
            ..Default::default()
        };
        let ts_column = Column {
            column_name: "ts".to_string(),
            semantic_type: SEMANTIC_TS,
            values: Some(ts_vals),
            null_mask: vec![0],
        };

        let insert_batch = InsertBatch {
            columns: vec![host_column, cpu_column, mem_column, ts_column],
            row_count,
        };
        vec![insert_batch.into()]
    }
}
