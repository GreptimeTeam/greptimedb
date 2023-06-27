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

//! Utilities for resolving schema compatibility problems.

use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::SchemaRef;
use datatypes::vectors::{Helper, VectorRef};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::metadata::ColumnMetadata;
use crate::read::Batch;
use crate::schema::{ProjectedSchemaRef, StoreSchemaRef};

/// Make schema compatible to write to target with another schema.
pub trait CompatWrite {
    /// Makes the schema of `self` compatible with `dest_schema`.
    ///
    /// For column in `dest_schema` but not in `self`, this method would insert a
    /// vector with default value.
    ///
    /// If there are columns not in `dest_schema`, an error would be returned.
    fn compat_write(&mut self, dest_schema: &SchemaRef) -> Result<()>;
}

/// Checks whether column with `source_column` could be read as a column with `dest_column`.
///
/// Returns
/// - `Ok(true)` if `source_column` is compatible to read using `dest_column` as schema.
/// - `Ok(false)` if they are considered different columns.
/// - `Err` if there is incompatible issue that could not be resolved.
fn is_source_column_compatible(
    source_column: &ColumnMetadata,
    dest_column: &ColumnMetadata,
) -> Result<bool> {
    ensure!(
        source_column.name() == dest_column.name(),
        error::CompatReadSnafu {
            reason: format!(
                "try to use column in {} for column {}",
                source_column.name(),
                dest_column.name()
            ),
        }
    );

    if source_column.id() != dest_column.id() {
        return Ok(false);
    }

    ensure!(
        source_column.desc.data_type == dest_column.desc.data_type,
        error::CompatReadSnafu {
            reason: format!(
                "could not read column {} from {:?} type as {:?} type",
                dest_column.name(),
                source_column.desc.data_type,
                dest_column.desc.data_type
            ),
        }
    );

    ensure!(
        dest_column.desc.is_nullable() || !source_column.desc.is_nullable(),
        error::CompatReadSnafu {
            reason: format!(
                "unable to read nullable data for non null column {}",
                dest_column.name()
            ),
        }
    );

    Ok(true)
}

/// Adapter to help reading data with source schema as data with dest schema.
#[derive(Debug)]
pub struct ReadAdapter {
    /// Schema of data source.
    source_schema: StoreSchemaRef,
    /// Schema user expects to read.
    dest_schema: ProjectedSchemaRef,
    /// For each column in dest schema, stores the index in read result for
    /// this column, or None if the column is not in result.
    ///
    /// This vec would be left empty if `source_version == dest_version`.
    indices_in_result: Vec<Option<usize>>,
    /// For each column in source schema, stores whether we need to read that column. All
    /// columns are needed by default.
    is_source_needed: Vec<bool>,
}

impl ReadAdapter {
    /// Creates a new [ReadAdapter] that could convert data with `source_schema` into data
    /// with `dest_schema`.
    pub fn new(
        source_schema: StoreSchemaRef,
        dest_schema: ProjectedSchemaRef,
    ) -> Result<ReadAdapter> {
        if source_schema.version() == dest_schema.schema_to_read().version() {
            ReadAdapter::from_same_version(source_schema, dest_schema)
        } else {
            ReadAdapter::from_different_version(source_schema, dest_schema)
        }
    }

    fn from_same_version(
        source_schema: StoreSchemaRef,
        dest_schema: ProjectedSchemaRef,
    ) -> Result<ReadAdapter> {
        let mut is_source_needed = vec![true; source_schema.num_columns()];
        if source_schema.num_columns() != dest_schema.schema_to_read().num_columns() {
            // `dest_schema` might be projected, so we need to find out value columns that not be read
            // by the `dest_schema`.

            for (offset, field_column) in source_schema.field_columns().iter().enumerate() {
                // Iterate value columns in source and mark those not in destination as unneeded.
                if !dest_schema.is_needed(field_column.id()) {
                    is_source_needed[source_schema.field_column_index_by_offset(offset)] = false;
                }
            }
        }

        Ok(ReadAdapter {
            source_schema,
            dest_schema,
            indices_in_result: Vec::new(),
            is_source_needed,
        })
    }

    fn from_different_version(
        source_schema: StoreSchemaRef,
        dest_schema: ProjectedSchemaRef,
    ) -> Result<ReadAdapter> {
        let schema_to_read = dest_schema.schema_to_read();
        let mut indices_in_result = vec![None; schema_to_read.num_columns()];
        let mut is_source_needed = vec![true; source_schema.num_columns()];
        // Number of columns in result from source data.
        let mut num_columns_in_result = 0;

        for (idx, source_column) in source_schema.columns().iter().enumerate() {
            // For each column in source schema, check whether we need to read it.
            if let Some(dest_idx) = schema_to_read
                .schema()
                .column_index_by_name(source_column.name())
            {
                let dest_column = &schema_to_read.columns()[dest_idx];
                // Check whether we could read this column.
                if is_source_column_compatible(source_column, dest_column)? {
                    // Mark that this column could be read from source data, since some
                    // columns in source schema would be skipped, we should not use
                    // the source column's index directly.
                    indices_in_result[dest_idx] = Some(num_columns_in_result);
                    num_columns_in_result += 1;
                } else {
                    // This column is not the same column in dest schema, should be fill by default value
                    // instead of reading from source data.
                    is_source_needed[idx] = false;
                }
            } else {
                // The column is not in `dest_schema`, we don't need to read it.
                is_source_needed[idx] = false;
            }
        }

        Ok(ReadAdapter {
            source_schema,
            dest_schema,
            indices_in_result,
            is_source_needed,
        })
    }

    /// Returns a bool slice to denote which key column in source is needed.
    #[inline]
    pub fn source_key_needed(&self) -> &[bool] {
        &self.is_source_needed[..self.source_schema.row_key_end()]
    }

    /// Returns a bool slice to denote which value column in source is needed.
    #[inline]
    pub fn source_value_needed(&self) -> &[bool] {
        &self.is_source_needed
            [self.source_schema.row_key_end()..self.source_schema.user_column_end()]
    }

    /// Construct a new [Batch] from row key, value, sequence and op_type.
    ///
    /// # Panics
    /// Panics if input `VectorRef` is empty.
    pub fn batch_from_parts(
        &self,
        row_key_columns: Vec<VectorRef>,
        mut field_columns: Vec<VectorRef>,
        sequences: VectorRef,
        op_types: VectorRef,
    ) -> Result<Batch> {
        // Each vector should has same length, so here we just use the length of `sequence`.
        let num_rows = sequences.len();

        let mut source = row_key_columns;
        // Reserve space for value, sequence and op_type
        source.reserve(field_columns.len() + 2);
        source.append(&mut field_columns);
        // Internal columns are push in sequence, op_type order.
        source.push(sequences);
        source.push(op_types);

        if !self.need_compat() {
            return Ok(Batch::new(source));
        }

        self.source_columns_to_batch(source, num_rows)
    }

    /// Returns list of fields indices need to read from the parquet file.
    pub fn fields_to_read(&self) -> Vec<usize> {
        self.is_source_needed
            .iter()
            .enumerate()
            .filter_map(|(idx, needed)| if *needed { Some(idx) } else { None })
            .collect::<Vec<_>>()
    }

    /// Convert [RecordBatch] read from the parquet file into [Batch].
    ///
    /// The [RecordBatch] should have the same schema as [`ReadAdapter::fields_to_read()`].
    pub fn arrow_record_batch_to_batch(&self, record_batch: &RecordBatch) -> Result<Batch> {
        let names = self
            .source_schema
            .schema()
            .column_schemas()
            .iter()
            .zip(self.is_source_needed.iter())
            .filter_map(|(column_schema, is_needed)| {
                if *is_needed {
                    Some(&column_schema.name)
                } else {
                    None
                }
            });
        let source = record_batch
            .columns()
            .iter()
            .zip(names)
            .map(|(column, name)| {
                Helper::try_into_vector(column.clone()).context(error::ConvertChunkSnafu { name })
            })
            .collect::<Result<_>>()?;

        if !self.need_compat() || record_batch.num_rows() == 0 {
            return Ok(Batch::new(source));
        }

        let num_rows = record_batch.num_rows();
        self.source_columns_to_batch(source, num_rows)
    }

    #[inline]
    fn need_compat(&self) -> bool {
        self.source_schema.version() != self.dest_schema.schema_to_read().version()
    }

    fn source_columns_to_batch(&self, source: Vec<VectorRef>, num_rows: usize) -> Result<Batch> {
        let column_schemas = self.dest_schema.schema_to_read().schema().column_schemas();
        let columns = self
            .indices_in_result
            .iter()
            .zip(column_schemas)
            .map(|(index_opt, column_schema)| {
                if let Some(idx) = index_opt {
                    Ok(source[*idx].clone())
                } else {
                    let vector = column_schema
                        .create_default_vector(num_rows)
                        .context(error::CreateDefaultToReadSnafu {
                            column: &column_schema.name,
                        })?
                        .context(error::NoDefaultToReadSnafu {
                            column: &column_schema.name,
                        })?;
                    Ok(vector)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Batch::new(columns))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::Schema;
    use store_api::storage::ColumnDescriptorBuilder;

    use super::*;
    use crate::error::Error;
    use crate::metadata::RegionMetadata;
    use crate::schema::{tests, ProjectedSchema, RegionSchema};
    use crate::test_util::{descriptor_util, schema_util};

    fn call_batch_from_parts(
        adapter: &ReadAdapter,
        batch: &Batch,
        num_field_columns: usize,
    ) -> Batch {
        let key = batch.columns()[0..2].to_vec();
        let value = batch.columns()[2..2 + num_field_columns].to_vec();
        let sequence = batch.column(2 + num_field_columns).clone();
        let op_type = batch.column(2 + num_field_columns + 1).clone();

        adapter
            .batch_from_parts(key, value, sequence, op_type)
            .unwrap()
    }

    fn check_batch_from_parts_without_padding(
        adapter: &ReadAdapter,
        batch: &Batch,
        num_field_columns: usize,
    ) {
        let new_batch = call_batch_from_parts(adapter, batch, num_field_columns);
        assert_eq!(*batch, new_batch);
    }

    fn call_arrow_chunk_to_batch(adapter: &ReadAdapter, batch: &Batch) -> Batch {
        let columns_schema = adapter
            .source_schema
            .columns()
            .iter()
            .zip(adapter.is_source_needed.iter())
            .filter_map(|(field, is_needed)| {
                if *is_needed {
                    Some(field.to_column_schema().unwrap())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let arrow_schema = Schema::try_new(columns_schema)
            .unwrap()
            .arrow_schema()
            .clone();
        let arrays = batch.columns().iter().map(|v| v.to_arrow_array()).collect();
        let chunk = RecordBatch::try_new(arrow_schema, arrays).unwrap();
        adapter.arrow_record_batch_to_batch(&chunk).unwrap()
    }

    fn check_arrow_chunk_to_batch_without_padding(adapter: &ReadAdapter, batch: &Batch) {
        let new_batch = call_arrow_chunk_to_batch(adapter, batch);
        assert_eq!(*batch, new_batch);
    }

    fn check_batch_with_null_padding(batch: &Batch, new_batch: &Batch, null_columns: &[usize]) {
        assert_eq!(
            batch.num_columns() + null_columns.len(),
            new_batch.num_columns()
        );

        let columns_from_source = new_batch
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(i, v)| {
                if null_columns.contains(&i) {
                    None
                } else {
                    Some(v.clone())
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(batch.columns(), &columns_from_source);

        for idx in null_columns {
            assert!(new_batch.column(*idx).only_null());
        }
    }

    #[test]
    fn test_compat_same_schema() {
        // (k0, timestamp, v0, v1) with version 0.
        let region_schema = Arc::new(schema_util::new_region_schema(0, 2));
        let projected_schema = Arc::new(ProjectedSchema::no_projection(region_schema.clone()));
        let source_schema = region_schema.store_schema().clone();
        let adapter = ReadAdapter::new(source_schema, projected_schema).unwrap();

        assert_eq!(&[true, true], adapter.source_key_needed());
        assert_eq!(&[true, true], adapter.source_value_needed());

        let batch = tests::new_batch_with_num_values(2);
        check_batch_from_parts_without_padding(&adapter, &batch, 2);

        assert_eq!(&adapter.fields_to_read(), &[0, 1, 2, 3, 4, 5],);

        check_arrow_chunk_to_batch_without_padding(&adapter, &batch);
    }

    #[test]
    fn test_compat_same_version_with_projection() {
        // (k0, timestamp, v0, v1) with version 0.
        let region_schema = Arc::new(schema_util::new_region_schema(0, 2));
        // Just read v0, k0.
        let projected_schema =
            Arc::new(ProjectedSchema::new(region_schema.clone(), Some(vec![2, 0])).unwrap());

        let source_schema = region_schema.store_schema().clone();
        let adapter = ReadAdapter::new(source_schema, projected_schema).unwrap();

        assert_eq!(&[true, true], adapter.source_key_needed());
        assert_eq!(&[true, false], adapter.source_value_needed());

        // One value column has been filtered out, so the result batch should only contains one value column.
        let batch = tests::new_batch_with_num_values(1);
        check_batch_from_parts_without_padding(&adapter, &batch, 1);

        assert_eq!(&adapter.fields_to_read(), &[0, 1, 2, 4, 5]);

        check_arrow_chunk_to_batch_without_padding(&adapter, &batch);
    }

    #[test]
    fn test_compat_old_column() {
        // (k0, timestamp, v0) with version 0.
        let region_schema_old = Arc::new(schema_util::new_region_schema(0, 1));
        // (k0, timestamp, v0, v1) with version 1.
        let region_schema_new = Arc::new(schema_util::new_region_schema(1, 1));

        // Just read v0, k0
        let projected_schema =
            Arc::new(ProjectedSchema::new(region_schema_new, Some(vec![2, 0])).unwrap());

        let source_schema = region_schema_old.store_schema().clone();
        let adapter = ReadAdapter::new(source_schema, projected_schema).unwrap();

        assert_eq!(&[true, true], adapter.source_key_needed());
        assert_eq!(&[true], adapter.source_value_needed());

        let batch = tests::new_batch_with_num_values(1);
        check_batch_from_parts_without_padding(&adapter, &batch, 1);

        assert_eq!(&adapter.fields_to_read(), &[0, 1, 2, 3, 4],);

        check_arrow_chunk_to_batch_without_padding(&adapter, &batch);
    }

    #[test]
    fn test_compat_new_column() {
        // (k0, timestamp, v0, v1) with version 0.
        let region_schema_old = Arc::new(schema_util::new_region_schema(0, 2));
        // (k0, timestamp, v0, v1, v2) with version 1.
        let region_schema_new = Arc::new(schema_util::new_region_schema(1, 3));

        // Just read v2, v0, k0
        let projected_schema =
            Arc::new(ProjectedSchema::new(region_schema_new, Some(vec![4, 2, 0])).unwrap());

        let source_schema = region_schema_old.store_schema().clone();
        let adapter = ReadAdapter::new(source_schema, projected_schema).unwrap();

        assert_eq!(&[true, true], adapter.source_key_needed());
        assert_eq!(&[true, false], adapter.source_value_needed());

        // Only read one value column from source.
        let batch = tests::new_batch_with_num_values(1);
        // New batch should contains k0, timestamp, v0, sequence, op_type.
        let new_batch = call_batch_from_parts(&adapter, &batch, 1);
        // v2 is filled by null.
        check_batch_with_null_padding(&batch, &new_batch, &[3]);

        assert_eq!(&adapter.fields_to_read(), &[0, 1, 2, 4, 5],);

        let new_batch = call_arrow_chunk_to_batch(&adapter, &batch);
        check_batch_with_null_padding(&batch, &new_batch, &[3]);
    }

    #[test]
    fn test_compat_different_column() {
        // (k0, timestamp, v0, v1) with version 0.
        let region_schema_old = Arc::new(schema_util::new_region_schema(0, 2));

        let mut descriptor = descriptor_util::desc_with_field_columns(tests::REGION_NAME, 2);
        // Assign a much larger column id to v0.
        descriptor.default_cf.columns[0].id = descriptor.default_cf.columns.last().unwrap().id + 10;
        let metadata: RegionMetadata = descriptor.try_into().unwrap();
        let columns = metadata.columns;
        // (k0, timestamp, v0, v1) with version 2, and v0 has different column id.
        let region_schema_new = Arc::new(RegionSchema::new(columns, 2).unwrap());

        let projected_schema = Arc::new(ProjectedSchema::no_projection(region_schema_new));
        let source_schema = region_schema_old.store_schema().clone();
        let adapter = ReadAdapter::new(source_schema, projected_schema).unwrap();

        assert_eq!(&[true, true], adapter.source_key_needed());
        // v0 is discarded as it has different column id than new schema's.
        assert_eq!(&[false, true], adapter.source_value_needed());

        // New batch should contains k0, timestamp, v1, sequence, op_type, so we need to remove v0
        // from the created batch.
        let batch = tests::new_batch_with_num_values(2);
        let mut columns = batch.columns().to_vec();
        // Remove v0.
        let _ = columns.remove(2);
        let batch = Batch::new(columns);

        let new_batch = call_batch_from_parts(&adapter, &batch, 1);
        // v0 is filled by null.
        check_batch_with_null_padding(&batch, &new_batch, &[2]);

        assert_eq!(&adapter.fields_to_read(), &[0, 1, 3, 4, 5],);

        let new_batch = call_arrow_chunk_to_batch(&adapter, &batch);
        check_batch_with_null_padding(&batch, &new_batch, &[2]);
    }

    #[inline]
    fn new_column_desc_builder() -> ColumnDescriptorBuilder {
        ColumnDescriptorBuilder::new(10, "test", ConcreteDataType::int32_datatype())
    }

    #[test]
    fn test_is_source_column_compatible() {
        let desc = new_column_desc_builder().build().unwrap();
        let source = ColumnMetadata { cf_id: 1, desc };

        // Same column is always compatible, also tests read nullable column
        // as a nullable column.
        assert!(is_source_column_compatible(&source, &source).unwrap());

        // Different id.
        let desc = new_column_desc_builder()
            .id(source.desc.id + 1)
            .build()
            .unwrap();
        let dest = ColumnMetadata { cf_id: 1, desc };
        assert!(!is_source_column_compatible(&source, &dest).unwrap());
    }

    #[test]
    fn test_nullable_column_read_by_not_null() {
        let desc = new_column_desc_builder().build().unwrap();
        assert!(desc.is_nullable());
        let source = ColumnMetadata { cf_id: 1, desc };

        let desc = new_column_desc_builder()
            .is_nullable(false)
            .build()
            .unwrap();
        let dest = ColumnMetadata { cf_id: 1, desc };

        let err = is_source_column_compatible(&source, &dest).unwrap_err();
        assert!(
            matches!(err, Error::CompatRead { .. }),
            "{err:?} is not CompatRead",
        );
    }

    #[test]
    fn test_read_not_null_column() {
        let desc = new_column_desc_builder()
            .is_nullable(false)
            .build()
            .unwrap();
        let source = ColumnMetadata { cf_id: 1, desc };

        let desc = new_column_desc_builder()
            .is_nullable(false)
            .build()
            .unwrap();
        let not_null_dest = ColumnMetadata { cf_id: 1, desc };
        assert!(is_source_column_compatible(&source, &not_null_dest).unwrap());

        let desc = new_column_desc_builder().build().unwrap();
        let null_dest = ColumnMetadata { cf_id: 1, desc };
        assert!(is_source_column_compatible(&source, &null_dest).unwrap());
    }

    #[test]
    fn test_read_column_with_different_name() {
        let desc = new_column_desc_builder().build().unwrap();
        let source = ColumnMetadata { cf_id: 1, desc };

        let desc = new_column_desc_builder()
            .name(format!("{}_other", source.desc.name))
            .build()
            .unwrap();
        let dest = ColumnMetadata { cf_id: 1, desc };

        let err = is_source_column_compatible(&source, &dest).unwrap_err();
        assert!(
            matches!(err, Error::CompatRead { .. }),
            "{err:?} is not CompatRead",
        );
    }
}
