use std::collections::HashMap;

use api::v1::{
    column::{SemanticType, Values},
    Column, ColumnDataType,
};
use common_base::BitVec;
use snafu::ensure;

use crate::error::{Result, TypeMismatchSnafu};

pub struct LinesWriter {
    inner: Inner,
}

type ColumnName = String;

#[derive(Default)]
struct Inner {
    column_name_index: HashMap<ColumnName, usize>,
    null_masks: Vec<BitVec>,
    batch: api::v1::codec::InsertBatch,
    to_insert: usize,
}

impl LinesWriter {
    pub fn with_capacity(to_insert: usize) -> Self {
        Self {
            inner: Inner {
                to_insert,
                ..Default::default()
            },
        }
    }

    pub fn write_time(&mut self, column_name: &str, value: i64) -> Result<()> {
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
        self.inner.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_tag(&mut self, column_name: &str, value: &str) -> Result<()> {
        let (idx, column) = self.mut_column(column_name, ColumnDataType::String, SemanticType::Tag);
        ensure!(
            column.datatype == Some(ColumnDataType::String.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.string_values.push(value.to_string());
        self.inner.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_u64(&mut self, column_name: &str, value: u64) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::Uint64, SemanticType::Field);
        ensure!(
            column.datatype == Some(ColumnDataType::Uint64.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.u64_values.push(value);
        self.inner.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_i64(&mut self, column_name: &str, value: i64) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::Int64, SemanticType::Field);
        ensure!(
            column.datatype == Some(ColumnDataType::Int64.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.i64_values.push(value);
        self.inner.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_f64(&mut self, column_name: &str, value: f64) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::Float64, SemanticType::Field);
        ensure!(
            column.datatype == Some(ColumnDataType::Float64.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.f64_values.push(value);
        self.inner.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_string(&mut self, column_name: &str, value: &str) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::String, SemanticType::Field);
        ensure!(
            column.datatype == Some(ColumnDataType::String.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.string_values.push(value.to_string());
        self.inner.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_bool(&mut self, column_name: &str, value: bool) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::Boolean, SemanticType::Field);
        ensure!(
            column.datatype == Some(api::v1::ColumnDataType::Boolean.into()),
            TypeMismatchSnafu { column_name }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.bool_values.push(value);
        self.inner.null_masks[idx].push(false);
        Ok(())
    }

    pub fn commit(&mut self) {
        let batch = &mut self.inner.batch;
        batch.row_count += 1;

        for i in 0..batch.columns.len() {
            let null_mask = &mut self.inner.null_masks[i];
            if batch.row_count as usize > null_mask.len() {
                null_mask.push(true);
            }
        }
    }

    pub fn finish(mut self) -> api::v1::codec::InsertBatch {
        let null_masks = self.inner.null_masks;
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
                let to_insert = self.inner.to_insert;
                let mut null_mask = BitVec::with_capacity(to_insert);
                null_mask.extend(BitVec::repeat(true, batch.row_count as usize));
                self.inner.null_masks.push(null_mask);
                batch.columns.push(Column {
                    column_name: column_name.to_string(),
                    semantic_type: semantic_type.into(),
                    values: Some(Values::with_capacity(datatype, to_insert)),
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

    use super::LinesWriter;

    #[test]
    fn test_lines_writer() {
        let mut writer = LinesWriter::with_capacity(3);

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
