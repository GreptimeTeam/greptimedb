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

use std::collections::HashMap;
use std::fmt::Display;

use api::helper::values_with_capacity;
use api::v1::{Column, ColumnDataType, ColumnDataTypeExtension, SemanticType};
use common_base::BitVec;
use common_time::timestamp::TimeUnit;
use snafu::ensure;

use crate::error::{Result, TypeMismatchSnafu};
use crate::Error;

type ColumnName = String;

type RowCount = u32;

// TODO(fys): will remove in the future.
#[derive(Default)]
pub struct LinesWriter {
    column_name_index: HashMap<ColumnName, usize>,
    null_masks: Vec<BitVec>,
    batch: (Vec<Column>, RowCount),
    lines: usize,
}

impl LinesWriter {
    pub fn with_lines(lines: usize) -> Self {
        Self {
            lines,
            ..Default::default()
        }
    }

    pub fn write_ts(&mut self, column_name: &str, value: (i64, Precision)) -> Result<()> {
        let (idx, column) = self.mut_column(
            column_name,
            ColumnDataType::TimestampMillisecond,
            SemanticType::Timestamp,
            None,
        );
        ensure!(
            column.datatype == ColumnDataType::TimestampMillisecond as i32,
            TypeMismatchSnafu {
                column_name,
                expected: "timestamp",
                actual: format!("{:?}", column.datatype)
            }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values
            .timestamp_millisecond_values
            .push(to_ms_ts(value.1, value.0));
        self.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_tag(&mut self, column_name: &str, value: &str) -> Result<()> {
        let (idx, column) =
            self.mut_column(column_name, ColumnDataType::String, SemanticType::Tag, None);
        ensure!(
            column.datatype == ColumnDataType::String as i32,
            TypeMismatchSnafu {
                column_name,
                expected: "string",
                actual: format!("{:?}", column.datatype)
            }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.string_values.push(value.to_string());
        self.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_u64(&mut self, column_name: &str, value: u64) -> Result<()> {
        let (idx, column) = self.mut_column(
            column_name,
            ColumnDataType::Uint64,
            SemanticType::Field,
            None,
        );
        ensure!(
            column.datatype == ColumnDataType::Uint64 as i32,
            TypeMismatchSnafu {
                column_name,
                expected: "u64",
                actual: format!("{:?}", column.datatype)
            }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.u64_values.push(value);
        self.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_i64(&mut self, column_name: &str, value: i64) -> Result<()> {
        let (idx, column) = self.mut_column(
            column_name,
            ColumnDataType::Int64,
            SemanticType::Field,
            None,
        );
        ensure!(
            column.datatype == ColumnDataType::Int64 as i32,
            TypeMismatchSnafu {
                column_name,
                expected: "i64",
                actual: format!("{:?}", column.datatype)
            }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.i64_values.push(value);
        self.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_f64(&mut self, column_name: &str, value: f64) -> Result<()> {
        let (idx, column) = self.mut_column(
            column_name,
            ColumnDataType::Float64,
            SemanticType::Field,
            None,
        );
        ensure!(
            column.datatype == ColumnDataType::Float64 as i32,
            TypeMismatchSnafu {
                column_name,
                expected: "f64",
                actual: format!("{:?}", column.datatype)
            }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.f64_values.push(value);
        self.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_string(&mut self, column_name: &str, value: &str) -> Result<()> {
        let (idx, column) = self.mut_column(
            column_name,
            ColumnDataType::String,
            SemanticType::Field,
            None,
        );
        ensure!(
            column.datatype == ColumnDataType::String as i32,
            TypeMismatchSnafu {
                column_name,
                expected: "string",
                actual: format!("{:?}", column.datatype)
            }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.string_values.push(value.to_string());
        self.null_masks[idx].push(false);
        Ok(())
    }

    pub fn write_bool(&mut self, column_name: &str, value: bool) -> Result<()> {
        let (idx, column) = self.mut_column(
            column_name,
            ColumnDataType::Boolean,
            SemanticType::Field,
            None,
        );
        ensure!(
            column.datatype == ColumnDataType::Boolean as i32,
            TypeMismatchSnafu {
                column_name,
                expected: "boolean",
                actual: format!("{:?}", column.datatype)
            }
        );
        // It is safe to use unwrap here, because values has been initialized in mut_column()
        let values = column.values.as_mut().unwrap();
        values.bool_values.push(value);
        self.null_masks[idx].push(false);
        Ok(())
    }

    pub fn commit(&mut self) {
        let batch = &mut self.batch;
        batch.1 += 1;

        for i in 0..batch.0.len() {
            let null_mask = &mut self.null_masks[i];
            if batch.1 as usize > null_mask.len() {
                null_mask.push(true);
            }
        }
    }

    pub fn finish(mut self) -> (Vec<Column>, RowCount) {
        let null_masks = self.null_masks;
        for (i, null_mask) in null_masks.into_iter().enumerate() {
            let columns = &mut self.batch.0;
            columns[i].null_mask = null_mask.into_vec();
        }
        self.batch
    }

    fn mut_column(
        &mut self,
        column_name: &str,
        datatype: ColumnDataType,
        semantic_type: SemanticType,
        datatype_extension: Option<ColumnDataTypeExtension>,
    ) -> (usize, &mut Column) {
        let column_names = &mut self.column_name_index;
        let column_idx = match column_names.get(column_name) {
            Some(i) => *i,
            None => {
                let new_idx = column_names.len();
                let batch = &mut self.batch;
                let to_insert = self.lines;
                let mut null_mask = BitVec::with_capacity(to_insert);
                null_mask.extend(BitVec::repeat(true, batch.1 as usize));
                self.null_masks.push(null_mask);
                batch.0.push(Column {
                    column_name: column_name.to_string(),
                    semantic_type: semantic_type.into(),
                    values: Some(values_with_capacity(datatype, to_insert)),
                    datatype: datatype as i32,
                    null_mask: Vec::default(),
                    datatype_extension,
                });
                let _ = column_names.insert(column_name.to_string(), new_idx);
                new_idx
            }
        };
        (column_idx, &mut self.batch.0[column_idx])
    }
}

pub fn to_ms_ts(p: Precision, ts: i64) -> i64 {
    match p {
        Precision::Nanosecond => ts / 1_000_000,
        Precision::Microsecond => ts / 1000,
        Precision::Millisecond => ts,
        Precision::Second => ts * 1000,
        Precision::Minute => ts * 1000 * 60,
        Precision::Hour => ts * 1000 * 60 * 60,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precision {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
}

impl Display for Precision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Precision::Nanosecond => write!(f, "Precision::Nanosecond"),
            Precision::Microsecond => write!(f, "Precision::Microsecond"),
            Precision::Millisecond => write!(f, "Precision::Millisecond"),
            Precision::Second => write!(f, "Precision::Second"),
            Precision::Minute => write!(f, "Precision::Minute"),
            Precision::Hour => write!(f, "Precision::Hour"),
        }
    }
}

impl TryFrom<Precision> for TimeUnit {
    type Error = Error;

    fn try_from(precision: Precision) -> std::result::Result<Self, Self::Error> {
        Ok(match precision {
            Precision::Second => TimeUnit::Second,
            Precision::Millisecond => TimeUnit::Millisecond,
            Precision::Microsecond => TimeUnit::Microsecond,
            Precision::Nanosecond => TimeUnit::Nanosecond,
            _ => {
                return Err(Error::NotSupported {
                    feat: format!("convert {precision} into TimeUnit"),
                })
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use api::v1::{ColumnDataType, SemanticType};
    use common_base::BitVec;

    use super::LinesWriter;
    use crate::writer::{to_ms_ts, Precision};

    #[test]
    fn test_lines_writer() {
        let mut writer = LinesWriter::with_lines(3);

        writer.write_tag("host", "host1").unwrap();
        writer.write_f64("cpu", 0.5).unwrap();
        writer.write_f64("memory", 0.4).unwrap();
        writer.write_string("name", "name1").unwrap();
        writer
            .write_ts("ts", (101011000, Precision::Millisecond))
            .unwrap();
        writer.commit();

        writer.write_tag("host", "host2").unwrap();
        writer
            .write_ts("ts", (102011001, Precision::Millisecond))
            .unwrap();
        writer.write_bool("enable_reboot", true).unwrap();
        writer.write_u64("year_of_service", 2).unwrap();
        writer.write_i64("temperature", 4).unwrap();
        writer.commit();

        writer.write_tag("host", "host3").unwrap();
        writer.write_f64("cpu", 0.4).unwrap();
        writer.write_u64("cpu_core_num", 16).unwrap();
        writer
            .write_ts("ts", (103011002, Precision::Millisecond))
            .unwrap();
        writer.commit();

        let insert_batch = writer.finish();
        assert_eq!(3, insert_batch.1);

        let columns = insert_batch.0;
        assert_eq!(9, columns.len());

        let column = &columns[0];
        assert_eq!("host", columns[0].column_name);
        assert_eq!(ColumnDataType::String as i32, column.datatype);
        assert_eq!(SemanticType::Tag as i32, column.semantic_type);
        assert_eq!(
            vec!["host1", "host2", "host3"],
            column.values.as_ref().unwrap().string_values
        );
        verify_null_mask(&column.null_mask, vec![false, false, false]);

        let column = &columns[1];
        assert_eq!("cpu", column.column_name);
        assert_eq!(ColumnDataType::Float64 as i32, column.datatype);
        assert_eq!(SemanticType::Field as i32, column.semantic_type);
        assert_eq!(vec![0.5, 0.4], column.values.as_ref().unwrap().f64_values);
        verify_null_mask(&column.null_mask, vec![false, true, false]);

        let column = &columns[2];
        assert_eq!("memory", column.column_name);
        assert_eq!(ColumnDataType::Float64 as i32, column.datatype);
        assert_eq!(SemanticType::Field as i32, column.semantic_type);
        assert_eq!(vec![0.4], column.values.as_ref().unwrap().f64_values);
        verify_null_mask(&column.null_mask, vec![false, true, true]);

        let column = &columns[3];
        assert_eq!("name", column.column_name);
        assert_eq!(ColumnDataType::String as i32, column.datatype);
        assert_eq!(SemanticType::Field as i32, column.semantic_type);
        assert_eq!(vec!["name1"], column.values.as_ref().unwrap().string_values);
        verify_null_mask(&column.null_mask, vec![false, true, true]);

        let column = &columns[4];
        assert_eq!("ts", column.column_name);
        assert_eq!(ColumnDataType::TimestampMillisecond as i32, column.datatype);
        assert_eq!(SemanticType::Timestamp as i32, column.semantic_type);
        assert_eq!(
            vec![101011000, 102011001, 103011002],
            column.values.as_ref().unwrap().timestamp_millisecond_values
        );
        verify_null_mask(&column.null_mask, vec![false, false, false]);

        let column = &columns[5];
        assert_eq!("enable_reboot", column.column_name);
        assert_eq!(ColumnDataType::Boolean as i32, column.datatype);
        assert_eq!(SemanticType::Field as i32, column.semantic_type);
        assert_eq!(vec![true], column.values.as_ref().unwrap().bool_values);
        verify_null_mask(&column.null_mask, vec![true, false, true]);

        let column = &columns[6];
        assert_eq!("year_of_service", column.column_name);
        assert_eq!(ColumnDataType::Uint64 as i32, column.datatype);
        assert_eq!(SemanticType::Field as i32, column.semantic_type);
        assert_eq!(vec![2], column.values.as_ref().unwrap().u64_values);
        verify_null_mask(&column.null_mask, vec![true, false, true]);

        let column = &columns[7];
        assert_eq!("temperature", column.column_name);
        assert_eq!(ColumnDataType::Int64 as i32, column.datatype);
        assert_eq!(SemanticType::Field as i32, column.semantic_type);
        assert_eq!(vec![4], column.values.as_ref().unwrap().i64_values);
        verify_null_mask(&column.null_mask, vec![true, false, true]);

        let column = &columns[8];
        assert_eq!("cpu_core_num", column.column_name);
        assert_eq!(ColumnDataType::Uint64 as i32, column.datatype);
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

    #[test]
    fn test_to_ms() {
        assert_eq!(100, to_ms_ts(Precision::Nanosecond, 100110000));
        assert_eq!(100110, to_ms_ts(Precision::Microsecond, 100110000));
        assert_eq!(100110000, to_ms_ts(Precision::Millisecond, 100110000));
        assert_eq!(
            100110000 * 1000 * 60,
            to_ms_ts(Precision::Minute, 100110000)
        );
        assert_eq!(
            100110000 * 1000 * 60 * 60,
            to_ms_ts(Precision::Hour, 100110000)
        );
    }
}
