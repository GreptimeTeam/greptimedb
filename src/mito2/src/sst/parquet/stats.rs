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

//! Statistics of parquet SSTs.

use std::borrow::Borrow;
use std::collections::HashSet;
use std::sync::Arc;

use api::v1::SemanticType;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datatypes::data_type::DataType;
use parquet::file::metadata::RowGroupMetaData;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::sst::parquet::flat_format::FlatReadFormat;
use crate::sst::parquet::format::StatValues;

/// Statistics for pruning row groups.
pub(crate) struct RowGroupPruningStats<'a, T> {
    /// Metadata of SST row groups.
    row_groups: &'a [T],
    /// Helper to read the SST.
    read_format: &'a FlatReadFormat,
    /// The metadata of the region.
    /// It contains the schema a query expects to read. If it is not None, we use it instead
    /// of the metadata in the SST to get the column id of a column as the SST may have
    /// different columns.
    expected_metadata: Option<RegionMetadataRef>,
    /// If true, skip columns with Field semantic type during pruning.
    skip_fields: bool,
}

impl<'a, T> RowGroupPruningStats<'a, T> {
    /// Creates a new statistics to prune specific `row_groups`.
    pub(crate) fn new(
        row_groups: &'a [T],
        read_format: &'a FlatReadFormat,
        expected_metadata: Option<RegionMetadataRef>,
        skip_fields: bool,
    ) -> Self {
        Self {
            row_groups,
            read_format,
            expected_metadata,
            skip_fields,
        }
    }

    /// Returns the column id of specific column name if we need to read it.
    /// Prefers the column id in the expected metadata if it exists.
    /// Returns None if skip_fields is true and the column is a Field.
    fn column_id_to_prune(&self, name: &str) -> Option<ColumnId> {
        let metadata = self
            .expected_metadata
            .as_ref()
            .unwrap_or_else(|| self.read_format.metadata());
        let col = metadata.column_by_name(name)?;

        // Skip field columns when skip_fields is enabled
        if self.skip_fields && col.semantic_type == SemanticType::Field {
            return None;
        }

        Some(col.column_id)
    }

    /// Casts stats values to the expected data type when the SST stores the
    /// column with a different type (e.g. after `MODIFY COLUMN` changed the
    /// column type, including widening the time index unit). The pruning
    /// predicate is built against the expected schema, so raw file-typed
    /// stats would prune wrongly.
    ///
    /// Timestamp stats are raw integers: first reinterpret them in the file's
    /// type, then convert to the expected type. A single-step cast would
    /// reinterpret (not rescale) the value. Returns `None` on cast failure so
    /// the row group is kept (conservative).
    fn cast_stats_to_expected(&self, column_id: ColumnId, values: ArrayRef) -> Option<ArrayRef> {
        // Without expected metadata the file metadata is the expected one,
        // so there is nothing to cast toward.
        let Some(expected_metadata) = self.expected_metadata.as_ref() else {
            return Some(values);
        };
        let expected_col = expected_metadata.column_by_id(column_id)?;
        let file_col = self.read_format.metadata().column_by_id(column_id)?;
        if expected_col.column_schema.data_type == file_col.column_schema.data_type {
            return Some(values);
        }
        let file_arrow_type = file_col.column_schema.data_type.as_arrow_type();
        let expected_arrow_type = expected_col.column_schema.data_type.as_arrow_type();
        let values = if values.data_type() == &file_arrow_type {
            values
        } else {
            datatypes::arrow::compute::cast(&values, &file_arrow_type).ok()?
        };
        datatypes::arrow::compute::cast(&values, &expected_arrow_type).ok()
    }

    /// Returns the default value of all row groups for `column` according to the metadata.
    fn compat_default_value(&self, column: &str) -> Option<ArrayRef> {
        let metadata = self.expected_metadata.as_ref()?;
        let col_metadata = metadata.column_by_name(column)?;
        col_metadata
            .column_schema
            .create_default_vector(self.row_groups.len())
            .unwrap_or(None)
            .map(|vector| vector.to_arrow_array())
    }
}

impl<T: Borrow<RowGroupMetaData>> RowGroupPruningStats<'_, T> {
    /// Returns the null count of all row groups for `column` according to the metadata.
    fn compat_null_count(&self, column: &str) -> Option<ArrayRef> {
        let metadata = self.expected_metadata.as_ref()?;
        let col_metadata = metadata.column_by_name(column)?;
        let value = col_metadata
            .column_schema
            .create_default()
            .unwrap_or(None)?;
        let values = self.row_groups.iter().map(|meta| {
            if value.is_null() {
                u64::try_from(meta.borrow().num_rows()).ok()
            } else {
                Some(0)
            }
        });
        Some(Arc::new(UInt64Array::from_iter(values)))
    }
}

impl<T: Borrow<RowGroupMetaData>> PruningStatistics for RowGroupPruningStats<'_, T> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        match self.read_format.min_values(self.row_groups, column_id) {
            StatValues::Values(values) => self.cast_stats_to_expected(column_id, values),
            StatValues::NoColumn => self.compat_default_value(&column.name),
            StatValues::NoStats => None,
        }
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        match self.read_format.max_values(self.row_groups, column_id) {
            StatValues::Values(values) => self.cast_stats_to_expected(column_id, values),
            StatValues::NoColumn => self.compat_default_value(&column.name),
            StatValues::NoStats => None,
        }
    }

    fn num_containers(&self) -> usize {
        self.row_groups.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        match self.read_format.null_counts(self.row_groups, column_id) {
            StatValues::Values(values) => Some(values),
            StatValues::NoColumn => self.compat_null_count(&column.name),
            StatValues::NoStats => None,
        }
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        // TODO(LFC): Impl it.
        None
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        // TODO(LFC): Impl it.
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::Column;
    use datatypes::arrow::array::{Array, Int64Array, TimestampMicrosecondArray};
    use datatypes::arrow::datatypes::TimeUnit;
    use datatypes::prelude::ConcreteDataType;
    use parquet::basic::Type as PhysicalType;
    use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
    use parquet::file::statistics::Statistics;
    use parquet::schema::types::{SchemaDescriptor, Type};
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::RegionMetadataRef;

    use super::*;
    use crate::read::read_columns::ReadColumns;
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;

    /// Builds one row group whose `ts` column (the time index of the
    /// `sst_region_metadata_with_encoding` fixture) carries raw Int64
    /// statistics `ts_min..=ts_max`, like a real parquet file does.
    fn row_group_with_ts_stats(
        read_format: &FlatReadFormat,
        ts_min: i64,
        ts_max: i64,
    ) -> RowGroupMetaData {
        let ts_idx = read_format.arrow_schema().index_of("ts").unwrap();
        let fields: Vec<Arc<Type>> = read_format
            .arrow_schema()
            .fields()
            .iter()
            .map(|field| {
                Arc::new(
                    Type::primitive_type_builder(field.name(), PhysicalType::INT64)
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        let schema_descr = Arc::new(SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema")
                .with_fields(fields)
                .build()
                .unwrap(),
        )));
        let chunks: Vec<_> = (0..schema_descr.num_columns())
            .map(|i| {
                let mut builder = ColumnChunkMetaData::builder(schema_descr.column(i));
                if i == ts_idx {
                    builder = builder.set_statistics(Statistics::int64(
                        Some(ts_min),
                        Some(ts_max),
                        None,
                        Some(0),
                        true,
                    ));
                }
                builder.build().unwrap()
            })
            .collect();
        RowGroupMetaData::builder(schema_descr)
            .set_num_rows(10)
            .set_total_byte_size(0)
            .set_column_metadata(chunks)
            .build()
            .unwrap()
    }

    /// The same region metadata but with the time index unit widened to
    /// microsecond, as the region looks after a widening alter.
    fn expected_metadata_us(file_metadata: &RegionMetadataRef) -> RegionMetadataRef {
        let mut expected = (**file_metadata).clone();
        for column in expected.column_metadatas.iter_mut() {
            if column.column_schema.name == "ts" {
                column.column_schema.data_type = ConcreteDataType::timestamp_microsecond_datatype();
            }
        }
        Arc::new(expected)
    }

    fn read_format_for(file_metadata: &RegionMetadataRef) -> FlatReadFormat {
        FlatReadFormat::new(
            file_metadata.clone(),
            ReadColumns::new([0, 1, 2, 3]),
            None,
            "test",
            false,
        )
        .unwrap()
    }

    /// Timestamp stats are raw Int64 in the file's unit: when the expected
    /// type differs, they must be *rescaled* (1000ms -> 1_000_000us), not
    /// reinterpreted (1000us).
    #[test]
    fn test_row_group_stats_cast_to_expected_unit() {
        let file_metadata: RegionMetadataRef =
            Arc::new(sst_region_metadata_with_encoding(PrimaryKeyEncoding::Dense));
        let read_format = read_format_for(&file_metadata);
        let row_group = row_group_with_ts_stats(&read_format, 1_000, 9_000);
        let column = Column::new_unqualified("ts");

        // No expected metadata: raw file stats pass through as Int64.
        let groups = [&row_group];
        let stats = RowGroupPruningStats::new(&groups, &read_format, None, false);
        let min = stats.min_values(&column).unwrap();
        let min = min.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(1_000, min.value(0));

        // Same type: passthrough, no cast needed.
        let groups = [&row_group];
        let stats =
            RowGroupPruningStats::new(&groups, &read_format, Some(file_metadata.clone()), false);
        let min = stats.min_values(&column).unwrap();
        let min = min.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(1_000, min.value(0));

        // Widened expected unit: rescaled, not reinterpreted.
        let expected = expected_metadata_us(&file_metadata);
        let groups = [&row_group];
        let stats = RowGroupPruningStats::new(&groups, &read_format, Some(expected), false);
        let min = stats.min_values(&column).unwrap();
        assert_eq!(
            datatypes::arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None),
            min.data_type().clone()
        );
        let min = min
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(1_000_000, min.value(0));
        let max = stats.max_values(&column).unwrap();
        let max = max
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(9_000_000, max.value(0));
    }
}
