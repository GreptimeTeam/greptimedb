use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use common_error::prelude::*;
use datatypes::arrow::bitmap::MutableBitmap;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use datatypes::vectors::{BooleanVector, VectorRef};
use store_api::storage::{Chunk, ColumnId};

use crate::error;
use crate::read::{Batch, BatchOp};
use crate::schema::{self, RegionSchema, RegionSchemaRef, Result, StoreSchema};

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
    schema_to_read: StoreSchema,
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
    pub fn schema_to_read(&self) -> &StoreSchema {
        &self.schema_to_read
    }

    /// Convert [Batch] into [Chunk].
    ///
    /// This will remove all internal columns. The input `batch` should has the
    /// same schema as `self.schema_to_read()`.
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

    /// Construct a new [Batch] from row key, value, sequence and op_type.
    ///
    /// # Panics
    /// Panics if number of columns are not the same as this schema.
    pub fn batch_from_parts(
        &self,
        row_key_columns: Vec<VectorRef>,
        mut value_columns: Vec<VectorRef>,
        sequences: VectorRef,
        op_types: VectorRef,
    ) -> Batch {
        // sequence and op_type
        let num_internal_columns = 2;

        assert_eq!(
            self.schema_to_read.num_columns(),
            row_key_columns.len() + value_columns.len() + num_internal_columns
        );

        let mut columns = row_key_columns;
        // Reserve space for value, sequence and op_type
        columns.reserve(value_columns.len() + num_internal_columns);
        columns.append(&mut value_columns);
        // Internal columns are push in sequence, op_type order.
        columns.push(sequences);
        columns.push(op_types);

        Batch::new(columns)
    }

    fn build_schema_to_read(
        region_schema: &RegionSchema,
        projection: &Projection,
    ) -> Result<StoreSchema> {
        let column_schemas: Vec<_> = projection
            .columns_to_read
            .iter()
            .map(|col_idx| ColumnSchema::from(&region_schema.column_metadata(*col_idx).desc))
            .collect();
        // All row key columns are reserved in this schema, so we can use the row_key_end
        // and timestamp_key_index from region schema.
        StoreSchema::new(
            column_schemas,
            region_schema.version(),
            region_schema.timestamp_key_index(),
            region_schema.row_key_end(),
            projection.num_user_columns,
        )
    }

    fn build_projected_user_schema(
        region_schema: &RegionSchema,
        projection: &Projection,
    ) -> Result<SchemaRef> {
        let timestamp_index =
            projection
                .projected_columns
                .iter()
                .enumerate()
                .find_map(|(idx, col_idx)| {
                    if *col_idx == region_schema.timestamp_key_index() {
                        Some(idx)
                    } else {
                        None
                    }
                });
        let column_schemas: Vec<_> = projection
            .projected_columns
            .iter()
            .map(|col_idx| ColumnSchema::from(&region_schema.column_metadata(*col_idx).desc))
            .collect();

        let mut builder = SchemaBuilder::try_from(column_schemas)
            .context(schema::ConvertSchemaSnafu)?
            .version(region_schema.version());
        if let Some(timestamp_index) = timestamp_index {
            builder = builder.timestamp_index(timestamp_index);
        }

        let schema = builder.build().context(schema::BuildSchemaSnafu)?;

        Ok(Arc::new(schema))
    }

    fn validate_projection(region_schema: &RegionSchema, indices: &[usize]) -> Result<()> {
        // The projection indices should not be empty, at least the timestamp column
        // should be always read, and the `StoreSchema` also requires the timestamp column.
        ensure!(
            !indices.is_empty(),
            schema::InvalidProjectionSnafu {
                msg: "at least one column should be read",
            }
        );

        // Now only allowed to read user columns.
        let user_schema = region_schema.user_schema();
        for i in indices {
            ensure!(
                *i < user_schema.num_columns(),
                schema::InvalidProjectionSnafu {
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
            // Comparision of vector is done by virtual method calls currently. Consider using
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

    fn dedup(&self, batch: &Batch, selected: &mut MutableBitmap, prev: Option<&Batch>) {
        if let Some(prev) = prev {
            assert_eq!(batch.num_columns(), prev.num_columns());
        }
        let indices = self.schema_to_read.row_key_indices();
        for idx in indices {
            let (current, prev_col) = (
                batch.column(idx),
                prev.map(|prev| prev.column(idx).as_ref()),
            );
            current.dedup(selected, prev_col);
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
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::ScalarVector;
    use datatypes::type_id::LogicalTypeId;
    use datatypes::vectors::TimestampVector;
    use store_api::storage::OpType;

    use super::*;
    use crate::schema::{tests, Error};
    use crate::test_util::{read_util, schema_util};

    #[test]
    fn test_projection() {
        // Build a region schema with 2 value columns. So the final user schema is
        // (k0, timestamp, v0, v1)
        let region_schema = tests::new_region_schema(0, 2);

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
        let region_schema = Arc::new(tests::new_region_schema(123, 3));

        // After projection: (v1, timestamp)
        let projected_schema =
            ProjectedSchema::new(region_schema.clone(), Some(vec![3, 1])).unwrap();
        let expect_user = schema_util::new_schema_with_version(
            &[
                ("v1", LogicalTypeId::Int64, true),
                ("timestamp", LogicalTypeId::Timestamp, false),
            ],
            Some(1),
            123,
        );
        assert_eq!(expect_user, **projected_schema.projected_user_schema());

        // Test is_needed
        let needed: Vec<_> = region_schema
            .all_columns()
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
        let expect_schema = tests::new_region_schema(123, 1);
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

        // Test batch_from_parts
        let keys = batch.columns()[0..2].to_vec();
        let values = batch.columns()[2..3].to_vec();
        let created = projected_schema.batch_from_parts(
            keys,
            values,
            batch.column(3).clone(),
            batch.column(4).clone(),
        );
        assert_eq!(batch, created);
    }

    #[test]
    fn test_projected_schema_no_projection() {
        // (k0, timestamp, v0)
        let region_schema = Arc::new(tests::new_region_schema(123, 1));

        let projected_schema = ProjectedSchema::no_projection(region_schema.clone());

        assert_eq!(
            region_schema.user_schema(),
            projected_schema.projected_user_schema()
        );
        assert_eq!(
            region_schema.store_schema(),
            projected_schema.schema_to_read()
        );

        for column in region_schema.all_columns() {
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
        let region_schema = Arc::new(tests::new_region_schema(123, 1));

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
    fn test_dedup_batch() {
        let schema = read_util::new_projected_schema();
        let batch = read_util::new_kv_batch(&[(1000, Some(1)), (2000, Some(2)), (2000, Some(2))]);
        let mut selected = MutableBitmap::from_len_zeroed(3);

        schema.dedup(&batch, &mut selected, None);
        assert!(selected.get(0));
        assert!(selected.get(1));
        assert!(!selected.get(2));

        let prev = read_util::new_kv_batch(&[(1000, Some(1))]);
        schema.dedup(&batch, &mut selected, Some(&prev));
        assert!(!selected.get(0));
        assert!(selected.get(1));
        assert!(!selected.get(2));
    }

    #[test]
    fn test_filter_batch() {
        let schema = read_util::new_projected_schema();
        let batch = read_util::new_kv_batch(&[(1000, Some(1)), (2000, Some(2)), (3000, Some(3))]);
        let filter = BooleanVector::from_slice(&[true, false, true]);

        let res = schema.filter(&batch, &filter).unwrap();
        let expect: VectorRef = Arc::new(TimestampVector::from_values([1000, 3000]));
        assert_eq!(expect, *res.column(0));
    }
}
