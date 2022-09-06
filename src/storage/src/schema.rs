use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use common_error::prelude::*;
use datatypes::arrow::array::Array;
use datatypes::arrow::chunk::Chunk as ArrowChunk;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::schema::Metadata;
use datatypes::vectors::{Helper, VectorRef};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use store_api::storage::{consts, Chunk, ColumnId, ColumnSchema, Schema, SchemaBuilder, SchemaRef};

use crate::metadata::{ColumnMetadata, ColumnsMetadata, ColumnsMetadataRef};
use crate::read::Batch;

const ROW_KEY_END_KEY: &str = "greptime:storage:row_key_end";
const USER_COLUMN_END_KEY: &str = "greptime:storage:user_column_end";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build schema, source: {}", source))]
    BuildSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to convert from arrow schema, source: {}", source))]
    ConvertArrowSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid internal column index in arrow schema"))]
    InvalidIndex { backtrace: Backtrace },

    #[snafu(display("Missing metadata {} in arrow schema", key))]
    MissingMeta { key: String, backtrace: Backtrace },

    #[snafu(display("Missing column {} in arrow schema", column))]
    MissingColumn {
        column: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to parse index in schema meta, value: {}, source: {}",
        value,
        source
    ))]
    ParseIndex {
        value: String,
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to convert arrow chunk to batch, name: {}, source: {}",
        name,
        source
    ))]
    ConvertChunk {
        name: String,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid projection, {}", msg))]
    InvalidProjection { msg: String, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Schema of region.
///
/// The `RegionSchema` has the knowledge of reserved and internal columns.
/// Reserved columns are columns that their names, ids are reserved by the storage
/// engine, and could not be used by the user. Reserved columns usually have
/// special usage. Reserved columns expect the version columns are also
/// called internal columns (though the version could also be thought as a
/// special kind of internal column), are not visible to user, such as our
/// internal sequence, op_type columns.
///
/// The user schema is the schema that only contains columns that user could visit,
/// as well as what the schema user created.
#[derive(Debug, PartialEq)]
pub struct RegionSchema {
    /// Schema that only contains columns that user defined, excluding internal columns
    /// that are reserved and used by the storage engine.
    ///
    /// Holding a [SchemaRef] to allow converting into `SchemaRef`/`arrow::SchemaRef`
    /// conveniently. The fields order in `SchemaRef` **must** be consistent with
    /// columns order in [ColumnsMetadata] to ensure the projection index of a field
    /// is correct.
    user_schema: SchemaRef,
    /// store schema contains all columns of the region, including all internal columns.
    store_schema: StoreSchema,
    /// Metadata of columns.
    columns: ColumnsMetadataRef,
}

impl RegionSchema {
    pub fn new(columns: ColumnsMetadataRef, version: u32) -> Result<RegionSchema> {
        let user_schema = Arc::new(build_user_schema(&columns, version)?);
        let store_schema = StoreSchema::from_columns_metadata(&columns, version)?;

        debug_assert_eq!(user_schema.version(), store_schema.version());
        debug_assert_eq!(version, user_schema.version());

        Ok(RegionSchema {
            user_schema,
            store_schema,
            columns,
        })
    }

    /// Returns the schema of the region, excluding internal columns that used by
    /// the storage engine.
    #[inline]
    pub fn user_schema(&self) -> &SchemaRef {
        &self.user_schema
    }

    /// Returns the schema actually stores, which would also contains all internal columns.
    #[inline]
    pub fn store_schema(&self) -> &StoreSchema {
        &self.store_schema
    }

    #[inline]
    pub fn row_key_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter_row_key_columns()
    }

    #[inline]
    pub fn value_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter_value_columns()
    }

    #[inline]
    pub fn num_row_key_columns(&self) -> usize {
        self.columns.num_row_key_columns()
    }

    #[inline]
    pub fn num_value_columns(&self) -> usize {
        self.columns.num_value_columns()
    }

    #[inline]
    pub fn version(&self) -> u32 {
        self.user_schema.version()
    }

    #[inline]
    fn sequence_index(&self) -> usize {
        self.store_schema.sequence_index()
    }

    #[inline]
    fn op_type_index(&self) -> usize {
        self.store_schema.op_type_index()
    }

    #[inline]
    fn row_key_indices(&self) -> impl Iterator<Item = usize> {
        self.store_schema.row_key_indices()
    }

    #[inline]
    fn column_metadata(&self, idx: usize) -> &ColumnMetadata {
        self.columns.column_metadata(idx)
    }

    #[inline]
    fn timestamp_key_index(&self) -> usize {
        self.columns.timestamp_key_index()
    }
}

pub type RegionSchemaRef = Arc<RegionSchema>;

/// Schema for storage engine.
///
/// Used internally, contains all row key columns, internal columns and parts of value columns.
///
/// Only contains a reference to schema and some indices, so it should be cheap to clone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreSchema {
    schema: SchemaRef,
    row_key_end: usize,
    user_column_end: usize,
}

impl StoreSchema {
    #[inline]
    pub fn version(&self) -> u32 {
        self.schema.version()
    }

    #[inline]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[inline]
    pub fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        self.schema.arrow_schema()
    }

    pub fn batch_to_arrow_chunk(&self, batch: &Batch) -> ArrowChunk<Arc<dyn Array>> {
        assert_eq!(self.schema.num_columns(), batch.num_columns());

        ArrowChunk::new(batch.columns().iter().map(|v| v.to_arrow_array()).collect())
    }

    pub fn arrow_chunk_to_batch(&self, chunk: &ArrowChunk<Arc<dyn Array>>) -> Result<Batch> {
        assert_eq!(self.schema.num_columns(), chunk.columns().len());

        let columns = chunk
            .iter()
            .enumerate()
            .map(|(i, column)| {
                Helper::try_into_vector(column.clone()).context(ConvertChunkSnafu {
                    name: self.column_name(i),
                })
            })
            .collect::<Result<_>>()?;

        Ok(Batch::new(columns))
    }

    fn from_columns_metadata(columns: &ColumnsMetadata, version: u32) -> Result<StoreSchema> {
        let column_schemas: Vec<_> = columns
            .iter_all_columns()
            .map(|col| ColumnSchema::from(&col.desc))
            .collect();

        StoreSchema::new(
            column_schemas,
            version,
            columns.timestamp_key_index(),
            columns.row_key_end(),
            columns.user_column_end(),
        )
    }

    fn new(
        column_schemas: Vec<ColumnSchema>,
        version: u32,
        timestamp_key_index: usize,
        row_key_end: usize,
        user_column_end: usize,
    ) -> Result<StoreSchema> {
        let schema = SchemaBuilder::from(column_schemas)
            .timestamp_index(timestamp_key_index)
            .version(version)
            .add_metadata(ROW_KEY_END_KEY, row_key_end.to_string())
            .add_metadata(USER_COLUMN_END_KEY, user_column_end.to_string())
            .build()
            .context(BuildSchemaSnafu)?;

        assert_eq!(
            consts::SEQUENCE_COLUMN_NAME,
            schema.column_schemas()[user_column_end].name
        );
        assert_eq!(
            consts::OP_TYPE_COLUMN_NAME,
            schema.column_schemas()[user_column_end + 1].name
        );

        Ok(StoreSchema {
            schema: Arc::new(schema),
            row_key_end,
            user_column_end,
        })
    }

    #[inline]
    fn sequence_index(&self) -> usize {
        self.user_column_end
    }

    #[inline]
    fn op_type_index(&self) -> usize {
        self.user_column_end + 1
    }

    #[inline]
    fn row_key_indices(&self) -> impl Iterator<Item = usize> {
        0..self.row_key_end
    }

    #[inline]
    fn column_name(&self, idx: usize) -> &str {
        &self.schema.column_schemas()[idx].name
    }

    #[inline]
    fn num_columns(&self) -> usize {
        self.schema.num_columns()
    }
}

impl TryFrom<ArrowSchema> for StoreSchema {
    type Error = Error;

    fn try_from(arrow_schema: ArrowSchema) -> Result<StoreSchema> {
        let schema = Schema::try_from(arrow_schema).context(ConvertArrowSchemaSnafu)?;
        // Recover other metadata from schema.
        let row_key_end = parse_index_from_metadata(schema.metadata(), ROW_KEY_END_KEY)?;
        let user_column_end = parse_index_from_metadata(schema.metadata(), USER_COLUMN_END_KEY)?;

        // There should be sequence and op_type columns.
        ensure!(
            consts::SEQUENCE_COLUMN_NAME == schema.column_schemas()[user_column_end].name,
            InvalidIndexSnafu
        );
        ensure!(
            consts::OP_TYPE_COLUMN_NAME == schema.column_schemas()[user_column_end + 1].name,
            InvalidIndexSnafu
        );

        Ok(StoreSchema {
            schema: Arc::new(schema),
            row_key_end,
            user_column_end,
        })
    }
}

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
            region_schema.columns.row_key_end(),
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

        let mut builder = SchemaBuilder::from(column_schemas).version(region_schema.version());
        if let Some(timestamp_index) = timestamp_index {
            builder = builder.timestamp_index(timestamp_index);
        }

        let schema = builder.build().context(BuildSchemaSnafu)?;

        Ok(Arc::new(schema))
    }

    fn validate_projection(region_schema: &RegionSchema, indices: &[usize]) -> Result<()> {
        // The projection indices should not be empty, at least the timestamp column
        // should be always read, and the `StoreSchema` also requires the timestamp column.
        ensure!(
            !indices.is_empty(),
            InvalidProjectionSnafu {
                msg: "at least one column should be read",
            }
        );

        // Now only allowed to read user columns.
        let user_schema = region_schema.user_schema();
        for i in indices {
            ensure!(
                *i < user_schema.num_columns(),
                InvalidProjectionSnafu {
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

fn parse_index_from_metadata(metadata: &Metadata, key: &str) -> Result<usize> {
    let value = metadata.get(key).context(MissingMetaSnafu { key })?;
    value.parse().context(ParseIndexSnafu { value })
}

// Now user schema don't have extra metadata like store schema.
fn build_user_schema(columns: &ColumnsMetadata, version: u32) -> Result<Schema> {
    let column_schemas: Vec<_> = columns
        .iter_user_columns()
        .map(|col| ColumnSchema::from(&col.desc))
        .collect();

    SchemaBuilder::from(column_schemas)
        .timestamp_index(columns.timestamp_key_index())
        .version(version)
        .build()
        .context(BuildSchemaSnafu)
}

#[cfg(test)]
mod tests {
    use common_time::timestamp::TimeUnit;
    use datatypes::type_id::LogicalTypeId;
    use datatypes::vectors::{Int64Vector, UInt64Vector, UInt8Vector};

    use super::*;
    use crate::metadata::RegionMetadata;
    use crate::test_util::{descriptor_util, schema_util};

    fn new_batch() -> Batch {
        let k0 = Int64Vector::from_slice(&[1, 2, 3]);
        let timestamp = Int64Vector::from_slice(&[4, 5, 6]);
        let v0 = Int64Vector::from_slice(&[7, 8, 9]);
        let sequences = UInt64Vector::from_slice(&[100, 100, 100]);
        let op_types = UInt8Vector::from_slice(&[0, 0, 0]);

        Batch::new(vec![
            Arc::new(k0),
            Arc::new(timestamp),
            Arc::new(v0),
            Arc::new(sequences),
            Arc::new(op_types),
        ])
    }

    fn check_chunk_batch(chunk: &ArrowChunk<Arc<dyn Array>>, batch: &Batch) {
        assert_eq!(5, chunk.columns().len());
        assert_eq!(3, chunk.len());

        for i in 0..5 {
            assert_eq!(chunk[i], batch.column(i).to_arrow_array());
        }
    }

    fn new_region_schema(version: u32, num_value_columns: usize) -> RegionSchema {
        let metadata: RegionMetadata =
            descriptor_util::desc_with_value_columns("test", num_value_columns)
                .try_into()
                .unwrap();

        let columns = metadata.columns;
        RegionSchema::new(columns, version).unwrap()
    }

    #[test]
    fn test_region_schema() {
        let region_schema = Arc::new(new_region_schema(123, 1));

        let expect_schema = schema_util::new_schema_with_version(
            &[
                ("k0", LogicalTypeId::Int64, false),
                (
                    "timestamp",
                    LogicalTypeId::Timestamp(TimeUnit::Microsecond),
                    false,
                ),
                ("v0", LogicalTypeId::Int64, true),
            ],
            Some(1),
            123,
        );

        assert_eq!(expect_schema, **region_schema.user_schema());

        // Checks row key column.
        let mut row_keys = region_schema.row_key_columns();
        assert_eq!("k0", row_keys.next().unwrap().desc.name);
        assert_eq!("timestamp", row_keys.next().unwrap().desc.name);
        assert_eq!(None, row_keys.next());
        assert_eq!(2, region_schema.num_row_key_columns());

        // Checks value column.
        let mut values = region_schema.value_columns();
        assert_eq!("v0", values.next().unwrap().desc.name);
        assert_eq!(None, values.next());
        assert_eq!(1, region_schema.num_value_columns());

        // Checks version.
        assert_eq!(123, region_schema.version());
        assert_eq!(123, region_schema.store_schema().version());

        // Checks StoreSchema.
        let store_schema = region_schema.store_schema();
        let sst_arrow_schema = store_schema.arrow_schema();
        let converted_store_schema = StoreSchema::try_from((**sst_arrow_schema).clone()).unwrap();

        assert_eq!(*store_schema, converted_store_schema);

        let expect_schema = schema_util::new_schema_with_version(
            &[
                ("k0", LogicalTypeId::Int64, false),
                (
                    "timestamp",
                    LogicalTypeId::Timestamp(TimeUnit::Microsecond),
                    false,
                ),
                ("v0", LogicalTypeId::Int64, true),
                (consts::SEQUENCE_COLUMN_NAME, LogicalTypeId::UInt64, false),
                (consts::OP_TYPE_COLUMN_NAME, LogicalTypeId::UInt8, false),
            ],
            Some(1),
            123,
        );
        assert_eq!(
            expect_schema.column_schemas(),
            store_schema.schema().column_schemas()
        );
        assert_eq!(3, store_schema.sequence_index());
        assert_eq!(4, store_schema.op_type_index());
        let row_key_indices: Vec<_> = store_schema.row_key_indices().collect();
        assert_eq!([0, 1], &row_key_indices[..]);

        // Test batch and chunk conversion.
        let batch = new_batch();
        // Convert batch to chunk.
        let chunk = store_schema.batch_to_arrow_chunk(&batch);
        check_chunk_batch(&chunk, &batch);

        // Convert chunk to batch.
        let converted_batch = store_schema.arrow_chunk_to_batch(&chunk).unwrap();
        check_chunk_batch(&chunk, &converted_batch);
    }

    #[test]
    fn test_projection() {
        // Build a region schema with 2 value columns. So the final user schema is
        // (k0, timestamp, v0, v1)
        let region_schema = new_region_schema(0, 2);

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
        let region_schema = Arc::new(new_region_schema(123, 3));

        // After projection: (v1, timestamp)
        let projected_schema =
            ProjectedSchema::new(region_schema.clone(), Some(vec![3, 1])).unwrap();
        let expect_user = schema_util::new_schema_with_version(
            &[
                ("v1", LogicalTypeId::Int64, true),
                (
                    "timestamp",
                    LogicalTypeId::Timestamp(TimeUnit::Microsecond),
                    false,
                ),
            ],
            Some(1),
            123,
        );
        assert_eq!(expect_user, **projected_schema.projected_user_schema());

        // Test is_needed
        let needed: Vec<_> = region_schema
            .columns
            .iter_all_columns()
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
        let expect_schema = new_region_schema(123, 1);
        assert_eq!(
            expect_schema.store_schema(),
            projected_schema.schema_to_read()
        );

        // (k0, timestamp, v0, sequence, op_type)
        let batch = new_batch();
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
        let region_schema = Arc::new(new_region_schema(123, 1));

        let projected_schema = ProjectedSchema::no_projection(region_schema.clone());

        assert_eq!(
            region_schema.user_schema(),
            projected_schema.projected_user_schema()
        );
        assert_eq!(
            region_schema.store_schema(),
            projected_schema.schema_to_read()
        );

        for column in region_schema.columns.iter_all_columns() {
            assert!(projected_schema.is_needed(column.id()));
        }

        // (k0, timestamp, v0, sequence, op_type)
        let batch = new_batch();
        // Test Batch to our Chunk.
        // (k0, timestamp, v0)
        let chunk = projected_schema.batch_to_chunk(&batch);
        assert_eq!(3, chunk.columns.len());
    }

    #[test]
    fn test_projected_schema_empty_projection() {
        // (k0, timestamp, v0)
        let region_schema = Arc::new(new_region_schema(123, 1));

        let err = ProjectedSchema::new(region_schema, Some(Vec::new()))
            .err()
            .unwrap();
        assert!(matches!(err, Error::InvalidProjection { .. }));
    }
}
