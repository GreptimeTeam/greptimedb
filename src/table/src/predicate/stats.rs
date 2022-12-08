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

use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::parquet::file::statistics::Statistics as ParquetStats;
use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::datatypes::DataType;
use datatypes::prelude::Vector;
pub use datatypes::vectors::UInt64Vector;

pub struct RowGroupPruningStatistics<'a> {
    pub meta_data: &'a [RowGroupMetaData],
    pub schema: &'a datatypes::schema::SchemaRef,
}

impl<'a> RowGroupPruningStatistics<'a> {
    pub fn new(
        meta_data: &'a [RowGroupMetaData],
        schema: &'a datatypes::schema::SchemaRef,
    ) -> Self {
        Self { meta_data, schema }
    }

    fn field_by_name(&self, name: &str) -> Option<(usize, &DataType)> {
        let idx = self.schema.column_index_by_name(name)?;
        let data_type = &self.schema.arrow_schema().fields.get(idx)?.data_type();
        Some((idx, data_type))
    }
}

impl<'a> PruningStatistics for RowGroupPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let arrow_schema = self.schema.arrow_schema().clone();
        let (column_index, field) =
            if let Some((v, f)) = arrow_schema.column_with_name(&column.name) {
                (v, f)
            } else {
                return None;
            };
        let data_type = field.data_type();
        let null_scalar: ScalarValue = if let Ok(v) = data_type.try_into() {
            v
        } else {
            return None;
        };

        let scalar_values: Vec<ScalarValue> = self
            .meta_data
            .iter()
            .flat_map(|meta| meta.column(column_index).statistics())
            .map(|stats| {
                if !stats.has_min_max_set() {
                    return None;
                }
                match stats {
                    ParquetStats::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.min()))),
                    ParquetStats::Int32(s) => Some(ScalarValue::Int32(Some(*s.min()))),
                    ParquetStats::Int64(s) => Some(ScalarValue::Int64(Some(*s.min()))),

                    ParquetStats::Int96(_) => None,
                    ParquetStats::Float(s) => Some(ScalarValue::Float32(Some(*s.min()))),
                    ParquetStats::Double(s) => Some(ScalarValue::Float64(Some(*s.min()))),
                    ParquetStats::ByteArray(s) => {
                        let s = std::str::from_utf8(s.min_bytes())
                            .map(|s| s.to_string())
                            .ok();
                        Some(ScalarValue::Utf8(s))
                    }

                    ParquetStats::FixedLenByteArray(_) => None,
                }
            })
            .map(|maybe_scalar| maybe_scalar.unwrap_or_else(|| null_scalar.clone()))
            .collect();
        ScalarValue::iter_to_array(scalar_values).ok()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let arrow_schema = self.schema.arrow_schema().clone();
        let (column_index, field) =
            if let Some((v, f)) = arrow_schema.column_with_name(&column.name) {
                (v, f)
            } else {
                return None;
            };

        let data_type = field.data_type();
        let null_scalar: ScalarValue = if let Ok(v) = data_type.try_into() {
            v
        } else {
            return None;
        };

        let scalar_values: Vec<ScalarValue> = self
            .meta_data
            .iter()
            .flat_map(|meta| meta.column(column_index).statistics())
            .map(|stats| {
                if !stats.has_min_max_set() {
                    return None;
                }
                match stats {
                    ParquetStats::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.max()))),
                    ParquetStats::Int32(s) => Some(ScalarValue::Int32(Some(*s.max()))),
                    ParquetStats::Int64(s) => Some(ScalarValue::Int64(Some(*s.max()))),

                    ParquetStats::Int96(_) => None,
                    ParquetStats::Float(s) => Some(ScalarValue::Float32(Some(*s.max()))),
                    ParquetStats::Double(s) => Some(ScalarValue::Float64(Some(*s.max()))),
                    ParquetStats::ByteArray(s) => {
                        let s = std::str::from_utf8(s.max_bytes())
                            .map(|s| s.to_string())
                            .ok();
                        Some(ScalarValue::Utf8(s))
                    }

                    ParquetStats::FixedLenByteArray(_) => None,
                }
            })
            .map(|maybe_scalar| maybe_scalar.unwrap_or_else(|| null_scalar.clone()))
            .collect();

        ScalarValue::iter_to_array(scalar_values).ok()
    }

    fn num_containers(&self) -> usize {
        self.meta_data.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let (idx, _) = self.field_by_name(&column.name)?;
        let mut values: Vec<Option<u64>> = Vec::with_capacity(self.meta_data.len());
        for m in self.meta_data {
            let col = m.column(idx);
            let stat = col.statistics()?;
            let bs = stat.null_count();
            values.push(Some(bs));
        }

        Some(UInt64Vector::from(values).to_arrow_array())
    }
}
