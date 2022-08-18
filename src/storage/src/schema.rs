use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use common_error::prelude::*;
use datatypes::arrow::array::Array;
use datatypes::arrow::chunk::Chunk as ArrowChunk;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::prelude::Vector;
use datatypes::schema::Metadata;
use datatypes::vectors::{Helper, UInt64Vector, UInt8Vector};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use store_api::storage::{consts, ColumnSchema, Schema, SchemaBuilder, SchemaRef};

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
    /// SST schema contains all columns of the region, including all internal columns.
    sst_schema: SstSchema,
    /// Metadata of columns.
    columns: ColumnsMetadataRef,
}

impl RegionSchema {
    pub fn new(columns: ColumnsMetadataRef, version: u32) -> Result<RegionSchema> {
        let user_schema = Arc::new(build_user_schema(&columns, version)?);
        let sst_schema = SstSchema::from_columns_metadata(&columns, version)?;

        debug_assert_eq!(user_schema.version(), sst_schema.version());
        debug_assert_eq!(version, user_schema.version());

        Ok(RegionSchema {
            user_schema,
            sst_schema,
            columns,
        })
    }

    /// Returns the schema of the region, excluding internal columns that used by
    /// the storage engine.
    #[inline]
    pub fn user_schema(&self) -> &SchemaRef {
        &self.user_schema
    }

    /// Returns the schema for sst, which contains all columns used by the region.
    #[inline]
    pub fn sst_schema(&self) -> &SstSchema {
        &self.sst_schema
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
        self.sst_schema.sequence_index()
    }

    #[inline]
    fn op_type_index(&self) -> usize {
        self.sst_schema.op_type_index()
    }

    #[inline]
    fn row_key_indices(&self) -> impl Iterator<Item = usize> {
        self.sst_schema.row_key_indices()
    }

    #[inline]
    fn column_metadata(&self, idx: usize) -> &ColumnMetadata {
        self.columns.column_metadata(idx)
    }

    #[inline]
    fn user_column_indices(&self) -> impl Iterator<Item = usize> {
        self.columns.user_column_indices()
    }

    #[inline]
    fn timestamp_key_index(&self) -> usize {
        self.columns.timestamp_key_index()
    }
}

pub type RegionSchemaRef = Arc<RegionSchema>;

/// Schema of SST.
///
/// Only contains a reference to schema and some indices, so it should be cheap to clone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SstSchema {
    schema: SchemaRef,
    row_key_end: usize,
    user_column_end: usize,
}

impl SstSchema {
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

    // TODO(yingwen): [projection] Use the new batch struct.
    pub fn batch_to_arrow_chunk(&self, batch: &Batch) -> ArrowChunk<Arc<dyn Array>> {
        assert_eq!(
            self.schema.num_columns(),
            // key columns + value columns + sequence + op_type
            batch.keys.len() + batch.values.len() + 2
        );

        ArrowChunk::new(
            batch
                .keys
                .iter()
                .map(|v| v.to_arrow_array())
                .chain(batch.values.iter().map(|v| v.to_arrow_array()))
                .chain(std::iter::once(batch.sequences.to_arrow_array()))
                .chain(std::iter::once(batch.op_types.to_arrow_array()))
                .collect(),
        )
    }

    pub fn arrow_chunk_to_batch(&self, chunk: &ArrowChunk<Arc<dyn Array>>) -> Result<Batch> {
        let keys = self
            .row_key_indices()
            .map(|i| {
                Helper::try_into_vector(&chunk[i].clone()).context(ConvertChunkSnafu {
                    name: self.column_name(i),
                })
            })
            .collect::<Result<_>>()?;
        let sequences = UInt64Vector::try_from_arrow_array(&chunk[self.sequence_index()].clone())
            .context(ConvertChunkSnafu {
            name: consts::SEQUENCE_COLUMN_NAME,
        })?;
        let op_types = UInt8Vector::try_from_arrow_array(&chunk[self.op_type_index()].clone())
            .context(ConvertChunkSnafu {
                name: consts::OP_TYPE_COLUMN_NAME,
            })?;
        let values = self
            .value_indices()
            .map(|i| {
                Helper::try_into_vector(&chunk[i].clone()).context(ConvertChunkSnafu {
                    name: self.column_name(i),
                })
            })
            .collect::<Result<_>>()?;

        Ok(Batch {
            keys,
            sequences,
            op_types,
            values,
        })
    }

    fn from_columns_metadata(columns: &ColumnsMetadata, version: u32) -> Result<SstSchema> {
        let column_schemas: Vec<_> = columns
            .iter_all_columns()
            .map(|col| ColumnSchema::from(&col.desc))
            .collect();

        SstSchema::new(
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
    ) -> Result<SstSchema> {
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

        Ok(SstSchema {
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
    fn value_indices(&self) -> impl Iterator<Item = usize> {
        self.row_key_end..self.user_column_end
    }

    #[inline]
    fn column_name(&self, idx: usize) -> &str {
        &self.schema.column_schemas()[idx].name
    }
}

impl TryFrom<ArrowSchema> for SstSchema {
    type Error = Error;

    fn try_from(arrow_schema: ArrowSchema) -> Result<SstSchema> {
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

        Ok(SstSchema {
            schema: Arc::new(schema),
            row_key_end,
            user_column_end,
        })
    }
}

/// Metadata about projection.
#[derive(Debug)]
struct Projection {
    /// Column indices of projection.
    projected_columns: Vec<usize>,
    /// Sorted and deduplicated indices of columns to read, includes all row key columns
    /// and internal columns.
    ///
    /// We use these indices to read from data sources.
    columns_to_read: Vec<usize>,
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
            projected_idx_to_read_idx,
            num_user_columns,
        }
    }
}

/// Schema with projection info.
#[derive(Debug)]
pub struct ProjectedSchema {
    region_schema: RegionSchemaRef,
    projection: Projection,
    /// Schema used to read from data sources.
    schema_to_read: SstSchema,
    /// User schema after projection.
    projected_user_schema: SchemaRef,
}

pub type ProjectedSchemaRef = Arc<ProjectedSchema>;

impl ProjectedSchema {
    pub fn new(
        region_schema: RegionSchemaRef,
        projected_columns: Option<Vec<usize>>,
    ) -> Result<ProjectedSchema> {
        if let Some(indices) = &projected_columns {
            Self::validate_projection(&region_schema, &indices)?;
        }
        let indices = match projected_columns {
            Some(indices) => {
                Self::validate_projection(&region_schema, &indices)?;
                indices
            }
            None => {
                // If all user columns are needed, we convert `None` to full
                // list of column indices, so we don't need to handle this case
                // specially.
                region_schema.user_column_indices().collect()
            }
        };

        let projection = Projection::new(&region_schema, indices);

        let schema_to_read = Self::build_schema_to_read(&region_schema, &projection)?;
        let projected_user_schema = Self::build_projected_user_schema(&region_schema, &projection)?;

        Ok(ProjectedSchema {
            region_schema,
            projection,
            schema_to_read,
            projected_user_schema,
        })
    }

    // FIXME(yingwen): [projection] Replaced by projected user schema
    #[inline]
    pub fn user_schema(&self) -> &SchemaRef {
        self.region_schema.user_schema()
    }

    #[inline]
    pub fn schema_to_read(&self) -> &SstSchema {
        &self.schema_to_read
    }

    fn build_schema_to_read(
        region_schema: &RegionSchema,
        projection: &Projection,
    ) -> Result<SstSchema> {
        let column_schemas: Vec<_> = projection
            .columns_to_read
            .iter()
            .map(|col_idx| ColumnSchema::from(&region_schema.column_metadata(*col_idx).desc))
            .collect();
        // All row key columns are reserved in this schema, so we can use the row_key_end
        // and timestamp_key_index from region schema.
        SstSchema::new(
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

        let mut builder = SchemaBuilder::from(column_schemas);
        if let Some(timestamp_index) = timestamp_index {
            builder = builder.timestamp_index(timestamp_index);
        }

        let schema = builder.build().context(BuildSchemaSnafu)?;

        Ok(Arc::new(schema))
    }

    fn validate_projection(region_schema: &RegionSchema, indices: &[usize]) -> Result<()> {
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

// Now user schema don't have extra metadata like sst schema.
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
    use datatypes::type_id::LogicalTypeId;
    use datatypes::vectors::{Int64Vector, UInt64Vector, UInt8Vector};

    use super::*;
    use crate::metadata::RegionMetadata;
    use crate::test_util::{descriptor_util::RegionDescBuilder, schema_util};

    fn new_batch() -> Batch {
        let k1 = Int64Vector::from_slice(&[1, 2, 3]);
        let timestamp = Int64Vector::from_slice(&[4, 5, 6]);
        let v1 = Int64Vector::from_slice(&[7, 8, 9]);

        Batch {
            keys: vec![Arc::new(k1), Arc::new(timestamp)],
            values: vec![Arc::new(v1)],
            sequences: UInt64Vector::from_slice(&[100, 100, 100]),
            op_types: UInt8Vector::from_slice(&[0, 0, 0]),
        }
    }

    fn check_chunk_batch(chunk: &ArrowChunk<Arc<dyn Array>>, batch: &Batch) {
        assert_eq!(5, chunk.columns().len());
        assert_eq!(3, chunk.len());

        for i in 0..2 {
            assert_eq!(chunk[i], batch.keys[i].to_arrow_array());
        }
        assert_eq!(chunk[2], batch.values[0].to_arrow_array());
        assert_eq!(chunk[3], batch.sequences.to_arrow_array());
        assert_eq!(chunk[4], batch.op_types.to_arrow_array());
    }

    #[test]
    fn test_region_schema() {
        let desc = RegionDescBuilder::new("test")
            .push_key_column(("k1", LogicalTypeId::Int64, false))
            .push_value_column(("v1", LogicalTypeId::Int64, true))
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();

        let columns = metadata.columns;
        let region_schema = RegionSchema::new(columns.clone(), 0).unwrap();

        let expect_schema = schema_util::new_schema(
            &[
                ("k1", LogicalTypeId::Int64, false),
                ("timestamp", LogicalTypeId::Int64, false),
                ("v1", LogicalTypeId::Int64, true),
            ],
            Some(1),
        );

        assert_eq!(expect_schema, **region_schema.user_schema());

        let mut row_keys = region_schema.row_key_columns();
        assert_eq!("k1", row_keys.next().unwrap().desc.name);
        assert_eq!("timestamp", row_keys.next().unwrap().desc.name);
        assert_eq!(None, row_keys.next());
        assert_eq!(2, region_schema.num_row_key_columns());

        let mut values = region_schema.value_columns();
        assert_eq!("v1", values.next().unwrap().desc.name);
        assert_eq!(None, values.next());
        assert_eq!(1, region_schema.num_value_columns());

        assert_eq!(0, region_schema.version());
        {
            let region_schema = RegionSchema::new(columns, 1234).unwrap();
            assert_eq!(1234, region_schema.version());
            assert_eq!(1234, region_schema.sst_schema().version());
        }

        let sst_schema = region_schema.sst_schema();
        let sst_arrow_schema = sst_schema.arrow_schema();
        let converted_sst_schema = SstSchema::try_from((**sst_arrow_schema).clone()).unwrap();

        assert_eq!(*sst_schema, converted_sst_schema);

        let expect_schema = schema_util::new_schema(
            &[
                ("k1", LogicalTypeId::Int64, false),
                ("timestamp", LogicalTypeId::Int64, false),
                ("v1", LogicalTypeId::Int64, true),
                (consts::SEQUENCE_COLUMN_NAME, LogicalTypeId::UInt64, false),
                (consts::OP_TYPE_COLUMN_NAME, LogicalTypeId::UInt8, false),
            ],
            Some(1),
        );
        assert_eq!(
            expect_schema.column_schemas(),
            sst_schema.schema().column_schemas()
        );
        assert_eq!(3, sst_schema.sequence_index());
        assert_eq!(4, sst_schema.op_type_index());
        let row_key_indices: Vec<_> = sst_schema.row_key_indices().collect();
        assert_eq!([0, 1], &row_key_indices[..]);
        let value_indices: Vec<_> = sst_schema.value_indices().collect();
        assert_eq!([2], &value_indices[..]);

        // Test batch and chunk conversion.
        let batch = new_batch();
        // Convert batch to chunk.
        let chunk = sst_schema.batch_to_arrow_chunk(&batch);
        check_chunk_batch(&chunk, &batch);

        // Convert chunk to batch.
        let converted_batch = sst_schema.arrow_chunk_to_batch(&chunk).unwrap();
        check_chunk_batch(&chunk, &converted_batch);
    }
}
