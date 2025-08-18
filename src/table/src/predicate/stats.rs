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
use std::sync::Arc;

use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::parquet::file::statistics::Statistics as ParquetStats;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datatypes::arrow::datatypes::DataType;
use paste::paste;

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

macro_rules! impl_min_max_values {
    ($self:ident, $col:ident, $min_max: ident) => {{
        let arrow_schema = $self.schema.arrow_schema().clone();
        let (column_index, field) = if let Some((v, f)) = arrow_schema.column_with_name(&$col.name)
        {
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

        let scalar_values = $self
            .meta_data
            .iter()
            .map(|meta| {
                let stats = meta.column(column_index).statistics()?;
                paste! {
                    match stats {
                        ParquetStats::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.[<$min_max _opt>]()?))),
                        ParquetStats::Int32(s) => Some(ScalarValue::Int32(Some(*s.[<$min_max _opt>]()?))),
                        ParquetStats::Int64(s) => Some(ScalarValue::Int64(Some(*s.[<$min_max _opt>]()?))),

                        ParquetStats::Int96(_) => None,
                        ParquetStats::Float(s) => Some(ScalarValue::Float32(Some(*s.[<$min_max _opt>]()?))),
                        ParquetStats::Double(s) => Some(ScalarValue::Float64(Some(*s.[<$min_max _opt>]()?))),
                        ParquetStats::ByteArray(s) => {
                            let s = String::from_utf8(s.[<$min_max _bytes_opt>]()?.to_owned()).ok();
                            Some(ScalarValue::Utf8(s))
                        }
                        ParquetStats::FixedLenByteArray(_) => None,
                    }
                }
            })
            .map(|maybe_scalar| maybe_scalar.unwrap_or_else(|| null_scalar.clone()))
            .collect::<Vec<ScalarValue>>();
        debug_assert_eq!(scalar_values.len(), $self.meta_data.len());
        ScalarValue::iter_to_array(scalar_values).ok()
    }};
}

impl PruningStatistics for RowGroupPruningStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        impl_min_max_values!(self, column, min)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        impl_min_max_values!(self, column, max)
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
            let bs = stat.null_count_opt()?;
            values.push(Some(bs));
        }
        Some(Arc::new(UInt64Array::from(values)))
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        // TODO(LFC): Impl it.
        None
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        // TODO(LFC): Impl it.
        None
    }
}
