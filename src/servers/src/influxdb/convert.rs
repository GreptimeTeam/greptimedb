use std::collections::HashMap;

use api::v1::{
    column::{SemanticType, Values},
    Column, ColumnDataType,
};
use common_base::BitVec;
use snafu::{ensure, ResultExt};

use crate::error::{InfluxdbLineProtocolSnafu, Result, TypeMismatchSnafu};
use crate::influxdb::line_protocol::{parse_lines, FieldValue};

pub const INFLUXDB_TIMESTAMP_COLUMN_NAME: &str = "ts";

type TableName = String;
type ColumnName = String;

pub struct InsertBatches {
    pub data: Vec<(TableName, api::v1::codec::InsertBatch)>,
}

impl TryFrom<&str> for InsertBatches {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let mut writers: HashMap<TableName, Writer> = HashMap::new();

        for line in parse_lines(value) {
            let line = line.context(InfluxdbLineProtocolSnafu)?;

            let table_name = line.series.measurement;
            let writer = writers
                .entry(table_name.to_string())
                .or_insert_with(Writer::new);

            let tags = line.series.tag_set;
            if let Some(tags) = tags {
                for (k, v) in tags {
                    writer.write_tag(k.as_str(), v.as_str())?;
                }
            }

            let fields = line.field_set;
            for (k, v) in fields {
                let column_name = k.as_str();
                match v {
                    FieldValue::I64(value) => {
                        writer.write_i64(column_name, value)?;
                    }
                    FieldValue::U64(value) => {
                        writer.write_u64(column_name, value)?;
                    }
                    FieldValue::F64(value) => {
                        writer.write_f64(column_name, value)?;
                    }
                    FieldValue::String(value) => {
                        writer.write_string(column_name, value.as_str())?;
                    }
                    FieldValue::Boolean(value) => {
                        writer.write_bool(column_name, value)?;
                    }
                }
            }

            if let Some(timestamp) = line.timestamp {
                writer.write_time(INFLUXDB_TIMESTAMP_COLUMN_NAME, timestamp)?;
            }

            writer.commit();
        }

        Ok(InsertBatches {
            data: writers
                .into_iter()
                .map(|(table_name, writer)| (table_name, writer.finish()))
                .collect(),
        })
    }
}

pub struct Writer {
    inner: InsertBatch,
}

#[derive(Default)]
pub struct InsertBatch {
    column_name_index: HashMap<ColumnName, usize>,
    null_mask: Vec<BitVec>,
    batch: api::v1::codec::InsertBatch,
}

impl Writer {
    fn new() -> Self {
        Self {
            inner: InsertBatch::default(),
        }
    }

    fn write_time(&mut self, column_name: &str, value: i64) -> Result<()> {
        let (idx, column) = self.mut_column(
            column_name,
            ColumnDataType::Timestamp,
            SemanticType::Timestamp,
        );
        ensure!(
            column.datatype == Some(ColumnDataType::Timestamp.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        // Convert nanoseconds to milliseconds
        values.ts_millis_values.push(value / 1000000);
        self.inner.null_mask[idx].push(false);
        Ok(())
    }

    fn write_tag(&mut self, column_name: &str, value: &str) -> Result<()> {
        let (idx, column) = self.mut_column(column_name, ColumnDataType::String, SemanticType::Tag);
        ensure!(
            column.datatype == Some(ColumnDataType::String.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.string_values.push(value.to_string());
        self.inner.null_mask[idx].push(false);
        Ok(())
    }

    fn write_u64(&mut self, column_name: &str, value: u64) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::Uint64, SemanticType::Field);
        ensure!(
            column.datatype == Some(ColumnDataType::Uint64.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.u64_values.push(value);
        self.inner.null_mask[idx].push(false);
        Ok(())
    }

    fn write_i64(&mut self, column_name: &str, value: i64) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::Int64, SemanticType::Field);
        ensure!(
            column.datatype == Some(ColumnDataType::Int64.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.i64_values.push(value);
        self.inner.null_mask[idx].push(false);
        Ok(())
    }

    fn write_f64(&mut self, column_name: &str, value: f64) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::Float64, SemanticType::Field);
        ensure!(
            column.datatype == Some(ColumnDataType::Float64.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.f64_values.push(value);
        self.inner.null_mask[idx].push(false);
        Ok(())
    }

    fn write_string(&mut self, column_name: &str, value: &str) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::String, SemanticType::Field);
        ensure!(
            column.datatype == Some(ColumnDataType::String.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unsafe here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.string_values.push(value.to_string());
        self.inner.null_mask[idx].push(false);
        Ok(())
    }

    fn write_bool(&mut self, column_name: &str, value: bool) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::Boolean, SemanticType::Field);
        ensure!(
            column.datatype == Some(api::v1::ColumnDataType::Boolean.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unsafe here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.bool_values.push(value);
        self.inner.null_mask[idx].push(false);
        Ok(())
    }

    fn commit(&mut self) {
        let batch = &mut self.inner.batch;
        batch.row_count += 1;

        for i in 0..batch.columns.len() {
            let null_mask = &mut self.inner.null_mask[i];
            if batch.row_count as usize > null_mask.len() {
                null_mask.push(true);
            }
        }
    }

    fn finish(mut self) -> api::v1::codec::InsertBatch {
        let null_masks = self.inner.null_mask;
        for (i, null_mask) in null_masks.into_iter().enumerate() {
            let columns = &mut self.inner.batch.columns;
            columns[i].null_mask = null_mask.into_vec();
        }
        self.inner.batch
    }

    fn mut_column(
        &mut self,
        column_name: &str,
        datatype: ColumnDataType,
        semantic_type: SemanticType,
    ) -> (usize, &mut Column) {
        let column_names = &mut self.inner.column_name_index;
        let column_idx = match column_names.get(column_name) {
            Some(i) => *i,
            None => {
                let new_idx = column_names.len();
                let batch = &mut self.inner.batch;
                self.inner
                    .null_mask
                    .push(BitVec::repeat(true, batch.row_count as usize));
                batch.columns.push(Column {
                    column_name: column_name.to_string(),
                    semantic_type: semantic_type.into(),
                    values: Some(Values::default()),
                    datatype: Some(datatype.into()),
                    null_mask: Vec::default(),
                });
                column_names.insert(column_name.to_string(), new_idx);
                new_idx
            }
        };
        (column_idx, &mut self.inner.batch.columns[column_idx])
    }
}

#[cfg(test)]
mod tests {
    use api::v1::{column::SemanticType, ColumnDataType};
    use common_base::BitVec;

    use super::Writer;

    #[test]
    fn test_writer1() {
        let mut writer = Writer::new();

        writer.write_tag("host", "host1").unwrap();
        writer.write_f64("cpu", 0.5).unwrap();
        writer.write_f64("memory", 0.4).unwrap();
        writer.write_time("ts", 101011000).unwrap();
        writer.commit();

        writer.write_tag("host", "host2").unwrap();
        writer.write_time("ts", 102011001).unwrap();
        writer.commit();

        writer.write_tag("host", "host3").unwrap();
        writer.write_f64("cpu", 0.4).unwrap();
        writer.write_u64("cpu_core_num", 16).unwrap();
        writer.write_time("ts", 103011002).unwrap();
        writer.commit();

        let insert_batch = writer.finish();
        assert_eq!(3, insert_batch.row_count);

        let columns = insert_batch.columns;
        assert_eq!(5, columns.len());

        let column = &columns[0];
        assert_eq!("host", columns[0].column_name);
        assert_eq!(Some(ColumnDataType::String as i32), column.datatype);
        assert_eq!(SemanticType::Tag as i32, column.semantic_type);
        assert_eq!(
            vec!["host1", "host2", "host3"],
            column.values.as_ref().unwrap().string_values
        );
        verify_null_mask(&column.null_mask, vec![false, false, false]);

        let column = &columns[1];
        assert_eq!("cpu", column.column_name);
        assert_eq!(Some(ColumnDataType::Float64 as i32), column.datatype);
        assert_eq!(SemanticType::Field as i32, column.semantic_type);
        assert_eq!(vec![0.5, 0.4], column.values.as_ref().unwrap().f64_values);
        verify_null_mask(&column.null_mask, vec![false, true, false]);

        let column = &columns[2];
        assert_eq!("memory", column.column_name);
        assert_eq!(Some(ColumnDataType::Float64 as i32), column.datatype);
        assert_eq!(SemanticType::Field as i32, column.semantic_type);
        assert_eq!(vec![0.4], column.values.as_ref().unwrap().f64_values);
        verify_null_mask(&column.null_mask, vec![false, true, true]);

        let column = &columns[3];
        assert_eq!("ts", column.column_name);
        assert_eq!(Some(ColumnDataType::Timestamp as i32), column.datatype);
        assert_eq!(SemanticType::Timestamp as i32, column.semantic_type);
        assert_eq!(
            vec![101, 102, 103],
            column.values.as_ref().unwrap().ts_millis_values
        );
        verify_null_mask(&column.null_mask, vec![false, false, false]);

        let column = &columns[4];
        assert_eq!("cpu_core_num", column.column_name);
        assert_eq!(Some(ColumnDataType::Uint64 as i32), column.datatype);
        assert_eq!(SemanticType::Field as i32, column.semantic_type);
        assert_eq!(vec![16], column.values.as_ref().unwrap().u64_values);
        verify_null_mask(&column.null_mask, vec![true, true, false]);
    }

    fn verify_null_mask(data: &[u8], expected: Vec<bool>) {
        let bitvec = BitVec::from_slice(data);
        for (idx, b) in expected.iter().enumerate() {
            assert_eq!(b, bitvec.get(idx).unwrap())
        }
    }
}
