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

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use common_base::BitVec;
use datatypes::prelude::ScalarVector;
use datatypes::schema::{SchemaBuilder, SchemaRef};
use datatypes::vectors::{BooleanVector, UInt8Vector};
use snafu::{ensure, ResultExt};
use store_api::storage::{Chunk, ColumnId, OpType};

use crate::error;
use crate::metadata::{self, Result};
use crate::read::{Batch, BatchOp};
use crate::schema::{RegionSchema, RegionSchemaRef, StoreSchema, StoreSchemaRef};

/// Metadata about projection.
#[derive(Debug, Default)]
struct Projection {
    /// Column indices of projection.
    projected_columns: Vec<usize>,
    /// Sorted and deduplicated indices of columns to read, includes all row key columns
    /// and internal columns.
    ///
    /// We use these indices to read from data sources.
    columns_to_read: Vec<usize>,
    /// Maps column id to its index in `columns_to_read`.
    ///
    /// Used to ask whether the column with given column id is needed in projection.
    id_to_read_idx: HashMap<ColumnId, usize>,
    /// Maps index of `projected_columns` to index of the column in `columns_to_read`.
    ///
    /// Invariant:
    /// - `projected_idx_to_read_idx.len() == projected_columns.len()`
    projected_idx_to_read_idx: Vec<usize>,
    /// Number of user columns to read.
    num_user_columns: usize,
}

impl Projection {
    fn new(region_schema: &RegionSchema, projected_columns: Vec<usize>) -> Projection {
        // Get a sorted list of column indices to read.
        let mut column_indices: BTreeSet<_> = projected_columns.iter().cloned().collect();
        column_indices.extend(region_schema.row_key_indices());
        let num_user_columns = column_indices.len();
        // Now insert internal columns.
        column_indices.extend([
            region_schema.sequence_index(),
            region_schema.op_type_index(),
        ]);
        let columns_to_read: Vec<_> = column_indices.into_iter().collect();

        // The region schema ensure that last two column must be internal columns.
        assert_eq!(
            region_schema.sequence_index(),
            columns_to_read[num_user_columns]
        );
        assert_eq!(
            region_schema.op_type_index(),
            columns_to_read[num_user_columns + 1]
        );

        // Mapping: <column id> => <index in `columns_to_read`>
        let id_to_read_idx: HashMap<_, _> = columns_to_read
            .iter()
            .enumerate()
            .map(|(idx, col_idx)| (region_schema.column_metadata(*col_idx).id(), idx))
            .collect();
        // Use column id to find index in `columns_to_read` of a column in `projected_columns`.
        let projected_idx_to_read_idx = projected_columns
            .iter()
            .map(|col_idx| {
                let column_id = region_schema.column_metadata(*col_idx).id();
                // This unwrap() should be safe since `columns_to_read` must contains all columns in `projected_columns`.
                let read_idx = id_to_read_idx.get(&column_id).unwrap();
                *read_idx
            })
            .collect();

        Projection {
            projected_columns,
            columns_to_read,
            id_to_read_idx,
            projected_idx_to_read_idx,
            num_user_columns,
        }
    }
}

/// Schema with projection info.
#[derive(Debug)]
pub struct ProjectedSchema {
    /// Projection info, `None` means don't need to do projection.
    projection: Option<Projection>,
    /// Schema used to read from data sources.
    schema_to_read: StoreSchemaRef,
    /// User schema after projection.
    projected_user_schema: SchemaRef,
}

pub type ProjectedSchemaRef = Arc<ProjectedSchema>;

impl ProjectedSchema {
    /// Create a new `ProjectedSchema` with given `projected_columns`.
    ///
    /// If `projected_columns` is None, then all columns would be read. If `projected_columns` is
    /// `Some`, then the `Vec` in it contains the indices of columns need to be read.
    ///
    /// If the `Vec` is empty or contains invalid index, `Err` would be returned.
    pub fn new(
        region_schema: RegionSchemaRef,
        projected_columns: Option<Vec<usize>>,
    ) -> Result<ProjectedSchema> {
        match projected_columns {
            Some(indices) => {
                Self::validate_projection(&region_schema, &indices)?;

                let projection = Projection::new(&region_schema, indices);

                let schema_to_read = Self::build_schema_to_read(&region_schema, &projection)?;
                let projected_user_schema =
                    Self::build_projected_user_schema(&region_schema, &projection)?;

                Ok(ProjectedSchema {
                    projection: Some(projection),
                    schema_to_read,
                    projected_user_schema,
                })
            }
            None => Ok(ProjectedSchema::no_projection(region_schema)),
        }
    }

    /// Create a `ProjectedSchema` that read all columns.
    pub fn no_projection(region_schema: RegionSchemaRef) -> ProjectedSchema {
        // We could just reuse the StoreSchema and user schema.
        ProjectedSchema {
            projection: None,
            schema_to_read: region_schema.store_schema().clone(),
            projected_user_schema: region_schema.user_schema().clone(),
        }
    }

    #[inline]
    pub fn projected_user_schema(&self) -> &SchemaRef {
        &self.projected_user_schema
    }

    #[inline]
    pub fn schema_to_read(&self) -> &StoreSchemaRef {
        &self.schema_to_read
    }

    /// Convert [Batch] into [Chunk].
    ///
    /// This will remove all internal columns. The input `batch` should has the
    /// same schema as [`self.schema_to_read()`](ProjectedSchema::schema_to_read).
    /// The output [Chunk] has the same schema as
    /// [`self.projected_user_schema()`](ProjectedSchema::projected_user_schema).
    pub fn batch_to_chunk(&self, batch: &Batch) -> Chunk {
        let columns = match &self.projection {
            Some(projection) => projection
                .projected_idx_to_read_idx
                .iter()
                .map(|col_idx| batch.column(*col_idx))
                .cloned()
                .collect(),
            None => {
                let num_user_columns = self.projected_user_schema.num_columns();
                batch
                    .columns()
                    .iter()
                    .take(num_user_columns)
                    .cloned()
                    .collect()
            }
        };
        Chunk::new(columns)
    }

    /// Returns true if column with given `column_id` is needed (in projection).
    pub fn is_needed(&self, column_id: ColumnId) -> bool {
        self.projection
            .as_ref()
            .map(|p| p.id_to_read_idx.contains_key(&column_id))
            .unwrap_or(true)
    }

    fn build_schema_to_read(
        region_schema: &RegionSchema,
        projection: &Projection,
    ) -> Result<StoreSchemaRef> {
        // Reorder columns according to the projection.
        let columns: Vec<_> = projection
            .columns_to_read
            .iter()
            .map(|col_idx| region_schema.column_metadata(*col_idx))
            .cloned()
            .collect();
        // All row key columns are reserved in this schema, so we can use the row_key_end
        // and timestamp_key_index from region schema.
        let store_schema = StoreSchema::new(
            columns,
            region_schema.version(),
            region_schema.row_key_end(),
            projection.num_user_columns,
        )?;

        Ok(Arc::new(store_schema))
    }

    fn build_projected_user_schema(
        region_schema: &RegionSchema,
        projection: &Projection,
    ) -> Result<SchemaRef> {
        let column_schemas: Vec<_> = projection
            .projected_columns
            .iter()
            .map(|col_idx| {
                region_schema
                    .column_metadata(*col_idx)
                    .desc
                    .to_column_schema()
            })
            .collect();

        let schema = SchemaBuilder::try_from(column_schemas)
            .context(metadata::ConvertSchemaSnafu)?
            .version(region_schema.version())
            .build()
            .context(metadata::InvalidSchemaSnafu)?;

        Ok(Arc::new(schema))
    }

    fn validate_projection(region_schema: &RegionSchema, indices: &[usize]) -> Result<()> {
        // The projection indices should not be empty, at least the timestamp column
        // should be always read, and the `StoreSchema` also requires the timestamp column.
        ensure!(
            !indices.is_empty(),
            metadata::InvalidProjectionSnafu {
                msg: "at least one column should be read",
            }
        );

        // Now only allowed to read user columns.
        let user_schema = region_schema.user_schema();
        for i in indices {
            ensure!(
                *i < user_schema.num_columns(),
                metadata::InvalidProjectionSnafu {
                    msg: format!(
                        "index {} out of bound, only contains {} columns",
                        i,
                        user_schema.num_columns()
                    ),
                }
            );
        }

        Ok(())
    }
}

impl BatchOp for ProjectedSchema {
    fn compare_row(&self, left: &Batch, i: usize, right: &Batch, j: usize) -> Ordering {
        // Ordered by (row_key asc, sequence desc, op_type desc).
        let indices = self.schema_to_read.row_key_indices();
        for idx in indices {
            let (left_col, right_col) = (left.column(idx), right.column(idx));
            // Comparison of vector is done by virtual method calls currently. Consider using
            // enum dispatch if this becomes bottleneck.
            let order = left_col.get_ref(i).cmp(&right_col.get_ref(j));
            if order != Ordering::Equal {
                return order;
            }
        }
        let (sequence_index, op_type_index) = (
            self.schema_to_read.sequence_index(),
            self.schema_to_read.op_type_index(),
        );
        right
            .column(sequence_index)
            .get_ref(j)
            .cmp(&left.column(sequence_index).get_ref(i))
            .then_with(|| {
                right
                    .column(op_type_index)
                    .get_ref(j)
                    .cmp(&left.column(op_type_index).get_ref(i))
            })
    }

    fn find_unique(&self, batch: &Batch, selected: &mut BitVec, prev: Option<&Batch>) {
        if let Some(prev) = prev {
            assert_eq!(batch.num_columns(), prev.num_columns());
        }
        let indices = self.schema_to_read.row_key_indices();
        for idx in indices {
            let (current, prev_col) = (
                batch.column(idx),
                prev.map(|prev| prev.column(idx).as_ref()),
            );
            current.find_unique(selected, prev_col);
        }
    }

    fn filter(&self, batch: &Batch, filter: &BooleanVector) -> error::Result<Batch> {
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(i, v)| {
                v.filter(filter).context(error::FilterColumnSnafu {
                    name: self.schema_to_read.column_name(i),
                })
            })
            .collect::<error::Result<Vec<_>>>()?;

        Ok(Batch::new(columns))
    }

    fn unselect_deleted(&self, batch: &Batch, selected: &mut BitVec) {
        let op_types = batch.column(self.schema_to_read.op_type_index());
        // Safety: We expect the batch has the same schema as `self.schema_to_read`. The
        // read procedure should guarantee this, otherwise this is a critical bug and it
        // should be fine to panic.
        let op_types = op_types
            .as_any()
            .downcast_ref::<UInt8Vector>()
            .unwrap_or_else(|| {
                panic!(
                    "Expect op_type (UInt8) column at index {}, given {:?}",
                    self.schema_to_read.op_type_index(),
                    op_types.data_type()
                );
            });

        for (i, op_type) in op_types.iter_data().enumerate() {
            if op_type == Some(OpType::Delete.as_u8()) {
                selected.set(i, false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::ScalarVector;
    use datatypes::type_id::LogicalTypeId;
    use datatypes::vectors::{TimestampMillisecondVector, VectorRef};
    use store_api::storage::OpType;

    use super::*;
    use crate::metadata::Error;
    use crate::schema::tests;
    use crate::test_util::{read_util, schema_util};

    #[test]
    fn test_projection() {
        // Build a region schema with 2 value columns. So the final user schema is
        // (k0, timestamp, v0, v1)
        let region_schema = schema_util::new_region_schema(0, 2);

        // Projection, but still keep column order.
        // After projection: (timestamp, v0)
        let projected_columns = vec![1, 2];
        let projection = Projection::new(&region_schema, projected_columns.clone());
        assert_eq!(projected_columns, projection.projected_columns);
        // Need to read (k0, timestamp, v0, sequence, op_type)
        assert_eq!(&[0, 1, 2, 4, 5], &projection.columns_to_read[..]);
        assert_eq!(5, projection.id_to_read_idx.len());
        // Index of timestamp, v0 in `columns_to_read`
        assert_eq!(&[1, 2], &projection.projected_idx_to_read_idx[..]);
        // 3 columns: k0, timestamp, v0
        assert_eq!(3, projection.num_user_columns);

        // Projection, unordered.
        // After projection: (timestamp, v1, k0)
        let projected_columns = vec![1, 3, 0];
        let projection = Projection::new(&region_schema, projected_columns.clone());
        assert_eq!(projected_columns, projection.projected_columns);
        // Need to read (k0, timestamp, v1, sequence, op_type)
        assert_eq!(&[0, 1, 3, 4, 5], &projection.columns_to_read[..]);
        assert_eq!(5, projection.id_to_read_idx.len());
        // Index of timestamp, v1, k0 in `columns_to_read`
        assert_eq!(&[1, 2, 0], &projection.projected_idx_to_read_idx[..]);
        // 3 columns: k0, timestamp, v1
        assert_eq!(3, projection.num_user_columns);

        // Empty projection.
        let projection = Projection::new(&region_schema, Vec::new());
        assert!(projection.projected_columns.is_empty());
        // Still need to read row keys.
        assert_eq!(&[0, 1, 4, 5], &projection.columns_to_read[..]);
        assert_eq!(4, projection.id_to_read_idx.len());
        assert!(projection.projected_idx_to_read_idx.is_empty());
        assert_eq!(2, projection.num_user_columns);
    }

    #[test]
    fn test_projected_schema_with_projection() {
        // (k0, timestamp, v0, v1, v2)
        let region_schema = Arc::new(schema_util::new_region_schema(123, 3));

        // After projection: (v1, timestamp)
        let projected_schema =
            ProjectedSchema::new(region_schema.clone(), Some(vec![3, 1])).unwrap();
        let expect_user = schema_util::new_schema_with_version(
            &[
                ("v1", LogicalTypeId::Int64, true),
                ("timestamp", LogicalTypeId::TimestampMillisecond, false),
            ],
            Some(1),
            123,
        );
        assert_eq!(expect_user, **projected_schema.projected_user_schema());

        // Test is_needed
        let needed: Vec<_> = region_schema
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(idx, column_meta)| {
                if projected_schema.is_needed(column_meta.id()) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();
        // (k0, timestamp, v1, sequence, op_type)
        assert_eq!(&[0, 1, 3, 5, 6], &needed[..]);

        // Use another projection.
        // After projection: (v0, timestamp)
        let projected_schema = ProjectedSchema::new(region_schema, Some(vec![2, 1])).unwrap();

        // The schema to read should be same as region schema with (k0, timestamp, v0).
        // We can't use `new_schema_with_version()` because the StoreSchema also store other
        // metadata that `new_schema_with_version()` can't store.
        let expect_schema = schema_util::new_region_schema(123, 1);
        assert_eq!(
            expect_schema.store_schema(),
            projected_schema.schema_to_read()
        );

        // (k0, timestamp, v0, sequence, op_type)
        let batch = tests::new_batch();
        // Test Batch to our Chunk.
        // (v0, timestamp)
        let chunk = projected_schema.batch_to_chunk(&batch);
        assert_eq!(2, chunk.columns.len());
        assert_eq!(&chunk.columns[0], batch.column(2));
        assert_eq!(&chunk.columns[1], batch.column(1));
    }

    #[test]
    fn test_projected_schema_no_projection() {
        // (k0, timestamp, v0)
        let region_schema = Arc::new(schema_util::new_region_schema(123, 1));

        let projected_schema = ProjectedSchema::no_projection(region_schema.clone());

        assert_eq!(
            region_schema.user_schema(),
            projected_schema.projected_user_schema()
        );
        assert_eq!(
            region_schema.store_schema(),
            projected_schema.schema_to_read()
        );

        for column in region_schema.columns() {
            assert!(projected_schema.is_needed(column.id()));
        }

        // (k0, timestamp, v0, sequence, op_type)
        let batch = tests::new_batch();
        // Test Batch to our Chunk.
        // (k0, timestamp, v0)
        let chunk = projected_schema.batch_to_chunk(&batch);
        assert_eq!(3, chunk.columns.len());
    }

    #[test]
    fn test_projected_schema_empty_projection() {
        // (k0, timestamp, v0)
        let region_schema = Arc::new(schema_util::new_region_schema(123, 1));

        let err = ProjectedSchema::new(region_schema, Some(Vec::new()))
            .err()
            .unwrap();
        assert!(matches!(err, Error::InvalidProjection { .. }));
    }

    #[test]
    fn test_compare_batch() {
        let schema = read_util::new_projected_schema();
        let left = read_util::new_full_kv_batch(&[(1000, 1, 1000, OpType::Put)]);
        let right = read_util::new_full_kv_batch(&[
            (999, 1, 1000, OpType::Put),
            (1000, 1, 999, OpType::Put),
            (1000, 1, 1000, OpType::Put),
        ]);

        assert_eq!(Ordering::Greater, schema.compare_row(&left, 0, &right, 0));
        assert_eq!(Ordering::Less, schema.compare_row(&left, 0, &right, 1));
        assert_eq!(Ordering::Equal, schema.compare_row(&left, 0, &right, 2));
    }

    #[test]
    fn test_batch_find_unique() {
        let schema = read_util::new_projected_schema();
        let batch = read_util::new_kv_batch(&[(1000, Some(1)), (2000, Some(2)), (2000, Some(2))]);

        let mut selected = BitVec::repeat(false, 3);
        schema.find_unique(&batch, &mut selected, None);
        assert!(selected[0]);
        assert!(selected[1]);
        assert!(!selected[2]);

        let mut selected = BitVec::repeat(false, 3);
        let prev = read_util::new_kv_batch(&[(1000, Some(1))]);
        schema.find_unique(&batch, &mut selected, Some(&prev));
        assert!(!selected[0]);
        assert!(selected[1]);
        assert!(!selected[2]);
    }

    #[test]
    fn test_find_unique_with_op() {
        let schema = read_util::new_projected_schema();
        let mut selected = BitVec::repeat(false, 3);
        let batch = read_util::new_full_kv_batch(&[
            (1001, 1, 3, OpType::Put),
            (1000, 1, 2, OpType::Delete),
            (1000, 1, 1, OpType::Put),
        ]);
        schema.find_unique(&batch, &mut selected, None);
        assert!(selected[0]);
        assert!(selected[1]);
        assert!(!selected[2]);
    }

    #[test]
    fn test_filter_batch() {
        let schema = read_util::new_projected_schema();
        let batch = read_util::new_kv_batch(&[(1000, Some(1)), (2000, Some(2)), (3000, Some(3))]);
        let filter = BooleanVector::from_slice(&[true, false, true]);

        let res = schema.filter(&batch, &filter).unwrap();
        let expect: VectorRef = Arc::new(TimestampMillisecondVector::from_values([1000, 3000]));
        assert_eq!(expect, *res.column(0));
    }

    #[test]
    fn test_unselect_deleted() {
        let schema = read_util::new_projected_schema();
        let batch = read_util::new_full_kv_batch(&[
            (100, 1, 1000, OpType::Put),
            (101, 1, 999, OpType::Delete),
            (102, 1, 1000, OpType::Put),
            (103, 1, 999, OpType::Put),
            (104, 1, 1000, OpType::Delete),
        ]);

        let mut selected = BitVec::repeat(true, batch.num_rows());
        schema.unselect_deleted(&batch, &mut selected);
        assert_eq!(
            BitVec::from_iter([true, false, true, true, false]),
            selected
        );
    }
}
