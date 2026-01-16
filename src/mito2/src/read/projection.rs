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

//! Utilities for projection operations.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::RecordBatch;
use common_recordbatch::error::ExternalSnafu;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, build_primary_key_codec};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::cache::CacheStrategy;
use crate::error::{InvalidRequestSnafu, Result};
use crate::read::Batch;
use crate::read::flat_projection::FlatProjectionMapper;

/// Only cache vector when its length `<=` this value.
const MAX_VECTOR_LENGTH_TO_CACHE: usize = 16384;

/// Wrapper enum for different projection mapper implementations.
pub enum ProjectionMapper {
    /// Projection mapper for primary key format.
    PrimaryKey(PrimaryKeyProjectionMapper),
    /// Projection mapper for flat format.
    Flat(FlatProjectionMapper),
}

impl ProjectionMapper {
    /// Returns a new mapper with projection.
    pub fn new(
        metadata: &RegionMetadataRef,
        projection: impl Iterator<Item = usize> + Clone,
        flat_format: bool,
    ) -> Result<Self> {
        if flat_format {
            Ok(ProjectionMapper::Flat(FlatProjectionMapper::new(
                metadata, projection,
            )?))
        } else {
            Ok(ProjectionMapper::PrimaryKey(
                PrimaryKeyProjectionMapper::new(metadata, projection)?,
            ))
        }
    }

    /// Returns a new mapper with output projection and explicit read columns.
    pub fn new_with_read_columns(
        metadata: &RegionMetadataRef,
        projection: impl Iterator<Item = usize>,
        flat_format: bool,
        read_column_ids: Vec<ColumnId>,
    ) -> Result<Self> {
        let projection: Vec<_> = projection.collect();
        if flat_format {
            Ok(ProjectionMapper::Flat(
                FlatProjectionMapper::new_with_read_columns(metadata, projection, read_column_ids)?,
            ))
        } else {
            Ok(ProjectionMapper::PrimaryKey(
                PrimaryKeyProjectionMapper::new_with_read_columns(
                    metadata,
                    projection,
                    read_column_ids,
                )?,
            ))
        }
    }

    /// Returns a new mapper without projection.
    pub fn all(metadata: &RegionMetadataRef, flat_format: bool) -> Result<Self> {
        if flat_format {
            Ok(ProjectionMapper::Flat(FlatProjectionMapper::all(metadata)?))
        } else {
            Ok(ProjectionMapper::PrimaryKey(
                PrimaryKeyProjectionMapper::all(metadata)?,
            ))
        }
    }

    /// Returns the metadata that created the mapper.
    pub(crate) fn metadata(&self) -> &RegionMetadataRef {
        match self {
            ProjectionMapper::PrimaryKey(m) => m.metadata(),
            ProjectionMapper::Flat(m) => m.metadata(),
        }
    }

    /// Returns true if the projection includes any tag columns.
    pub(crate) fn has_tags(&self) -> bool {
        match self {
            ProjectionMapper::PrimaryKey(m) => m.has_tags(),
            ProjectionMapper::Flat(_) => false,
        }
    }

    /// Returns ids of projected columns that we need to read
    /// from memtables and SSTs.
    pub(crate) fn column_ids(&self) -> &[ColumnId] {
        match self {
            ProjectionMapper::PrimaryKey(m) => m.column_ids(),
            ProjectionMapper::Flat(m) => m.column_ids(),
        }
    }

    /// Returns the schema of converted [RecordBatch].
    pub(crate) fn output_schema(&self) -> SchemaRef {
        match self {
            ProjectionMapper::PrimaryKey(m) => m.output_schema(),
            ProjectionMapper::Flat(m) => m.output_schema(),
        }
    }

    /// Returns the primary key projection mapper or None if this is not a primary key mapper.
    pub fn as_primary_key(&self) -> Option<&PrimaryKeyProjectionMapper> {
        match self {
            ProjectionMapper::PrimaryKey(m) => Some(m),
            ProjectionMapper::Flat(_) => None,
        }
    }

    /// Returns the flat projection mapper or None if this is not a flat mapper.
    pub fn as_flat(&self) -> Option<&FlatProjectionMapper> {
        match self {
            ProjectionMapper::PrimaryKey(_) => None,
            ProjectionMapper::Flat(m) => Some(m),
        }
    }

    /// Returns an empty [RecordBatch].
    // TODO(yingwen): This is unused now. Use it after we finishing the flat format.
    pub fn empty_record_batch(&self) -> RecordBatch {
        match self {
            ProjectionMapper::PrimaryKey(m) => m.empty_record_batch(),
            ProjectionMapper::Flat(m) => m.empty_record_batch(),
        }
    }
}

/// Handles projection and converts a projected [Batch] to a projected [RecordBatch].
pub struct PrimaryKeyProjectionMapper {
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Maps column in [RecordBatch] to index in [Batch].
    batch_indices: Vec<BatchIndex>,
    /// Output record batch contains tags.
    has_tags: bool,
    /// Decoder for primary key.
    codec: Arc<dyn PrimaryKeyCodec>,
    /// Schema for converted [RecordBatch].
    output_schema: SchemaRef,
    /// Ids of columns to read from memtables and SSTs.
    read_column_ids: Vec<ColumnId>,
    /// Ids and DataTypes of field columns in the read [Batch].
    batch_fields: Vec<(ColumnId, ConcreteDataType)>,
    /// `true` If the original projection is empty.
    is_empty_projection: bool,
}

impl PrimaryKeyProjectionMapper {
    /// Returns a new mapper with projection.
    /// If `projection` is empty, it outputs [RecordBatch] without any column but only a row count.
    /// `SELECT COUNT(*) FROM table` is an example that uses an empty projection. DataFusion accepts
    /// empty `RecordBatch` and only use its row count in this query.
    pub fn new(
        metadata: &RegionMetadataRef,
        projection: impl Iterator<Item = usize>,
    ) -> Result<PrimaryKeyProjectionMapper> {
        let projection: Vec<_> = projection.collect();
        let read_column_ids = read_column_ids_from_projection(metadata, &projection)?;
        Self::new_with_read_columns(metadata, projection, read_column_ids)
    }

    /// Returns a new mapper with output projection and explicit read columns.
    pub fn new_with_read_columns(
        metadata: &RegionMetadataRef,
        projection: Vec<usize>,
        read_column_ids: Vec<ColumnId>,
    ) -> Result<PrimaryKeyProjectionMapper> {
        // If the original projection is empty.
        let is_empty_projection = projection.is_empty();

        let mut column_schemas = Vec::with_capacity(projection.len());
        for idx in &projection {
            // For each projection index, we get the column id for projection.
            let column = metadata
                .column_metadatas
                .get(*idx)
                .context(InvalidRequestSnafu {
                    region_id: metadata.region_id,
                    reason: format!("projection index {} is out of bound", idx),
                })?;

            // Safety: idx is valid.
            column_schemas.push(metadata.schema.column_schemas()[*idx].clone());
        }

        let codec = build_primary_key_codec(metadata);
        // If projection is empty, we don't output any column.
        let output_schema = if is_empty_projection {
            Arc::new(Schema::new(vec![]))
        } else {
            // Safety: Columns come from existing schema.
            Arc::new(Schema::new(column_schemas))
        };
        // Get fields in each read batch.
        let batch_fields = Batch::projected_fields(metadata, &read_column_ids);

        // Field column id to its index in batch.
        let field_id_to_index: HashMap<_, _> = batch_fields
            .iter()
            .enumerate()
            .map(|(index, (column_id, _))| (*column_id, index))
            .collect();
        // For each projected column, compute its index in batches.
        let mut batch_indices = Vec::with_capacity(projection.len());
        let mut has_tags = false;
        if !is_empty_projection {
            for idx in &projection {
                // Safety: idx is valid.
                let column = &metadata.column_metadatas[*idx];
                // Get column index in a batch by its semantic type and column id.
                let batch_index = match column.semantic_type {
                    SemanticType::Tag => {
                        // Safety: It is a primary key column.
                        let index = metadata.primary_key_index(column.column_id).unwrap();
                        // We need to output a tag.
                        has_tags = true;
                        // We always read all primary key so the column always exists and the tag
                        // index is always valid.
                        BatchIndex::Tag((index, column.column_id))
                    }
                    SemanticType::Timestamp => BatchIndex::Timestamp,
                    SemanticType::Field => {
                        let index = *field_id_to_index.get(&column.column_id).context(
                            InvalidRequestSnafu {
                                region_id: metadata.region_id,
                                reason: format!(
                                    "field column {} is missing in read projection",
                                    column.column_schema.name
                                ),
                            },
                        )?;
                        BatchIndex::Field(index)
                    }
                };
                batch_indices.push(batch_index);
            }
        }

        Ok(PrimaryKeyProjectionMapper {
            metadata: metadata.clone(),
            batch_indices,
            has_tags,
            codec,
            output_schema,
            read_column_ids,
            batch_fields,
            is_empty_projection,
        })
    }

    /// Returns a new mapper without projection.
    pub fn all(metadata: &RegionMetadataRef) -> Result<PrimaryKeyProjectionMapper> {
        PrimaryKeyProjectionMapper::new(metadata, 0..metadata.column_metadatas.len())
    }

    /// Returns the metadata that created the mapper.
    pub(crate) fn metadata(&self) -> &RegionMetadataRef {
        &self.metadata
    }

    /// Returns true if the projection includes any tag columns.
    pub(crate) fn has_tags(&self) -> bool {
        self.has_tags
    }

    /// Returns ids of projected columns that we need to read
    /// from memtables and SSTs.
    pub(crate) fn column_ids(&self) -> &[ColumnId] {
        &self.read_column_ids
    }

    /// Returns ids of fields in [Batch]es the mapper expects to convert.
    pub(crate) fn batch_fields(&self) -> &[(ColumnId, ConcreteDataType)] {
        &self.batch_fields
    }

    /// Returns the schema of converted [RecordBatch].
    /// This is the schema that the stream will output. This schema may contain
    /// less columns than [PrimaryKeyProjectionMapper::column_ids()].
    pub(crate) fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Returns an empty [RecordBatch].
    pub(crate) fn empty_record_batch(&self) -> RecordBatch {
        RecordBatch::new_empty(self.output_schema.clone())
    }

    /// Converts a [Batch] to a [RecordBatch].
    ///
    /// The batch must match the `projection` using to build the mapper.
    /// TODO(discord9): handle batch have exact fields other than projected
    pub(crate) fn convert(
        &self,
        batch: &Batch,
        cache_strategy: &CacheStrategy,
    ) -> common_recordbatch::error::Result<RecordBatch> {
        if self.is_empty_projection {
            return RecordBatch::new_with_count(self.output_schema.clone(), batch.num_rows());
        }

        debug_assert_eq!(self.batch_fields.len(), batch.fields().len());
        debug_assert!(
            self.batch_fields
                .iter()
                .zip(batch.fields())
                .all(|((id, _), batch_col)| *id == batch_col.column_id)
        );

        // Skips decoding pk if we don't need to output it.
        let pk_values = if self.has_tags {
            match batch.pk_values() {
                Some(v) => v.clone(),
                None => self
                    .codec
                    .decode(batch.primary_key())
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?,
            }
        } else {
            CompositeValues::Dense(vec![])
        };

        let mut columns = Vec::with_capacity(self.output_schema.num_columns());
        let num_rows = batch.num_rows();
        for (index, column_schema) in self
            .batch_indices
            .iter()
            .zip(self.output_schema.column_schemas())
        {
            match index {
                BatchIndex::Tag((idx, column_id)) => {
                    let value = match &pk_values {
                        CompositeValues::Dense(v) => &v[*idx].1,
                        CompositeValues::Sparse(v) => v.get_or_null(*column_id),
                    };
                    let vector = repeated_vector_with_cache(
                        &column_schema.data_type,
                        value,
                        num_rows,
                        cache_strategy,
                    )?;
                    columns.push(vector);
                }
                BatchIndex::Timestamp => {
                    columns.push(batch.timestamps().clone());
                }
                BatchIndex::Field(idx) => {
                    columns.push(batch.fields()[*idx].data.clone());
                }
            }
        }

        RecordBatch::new(self.output_schema.clone(), columns)
    }
}

fn read_column_ids_from_projection(
    metadata: &RegionMetadataRef,
    projection: &[usize],
) -> Result<Vec<ColumnId>> {
    let mut column_ids = Vec::with_capacity(projection.len().max(1));
    if projection.is_empty() {
        column_ids.push(metadata.time_index_column().column_id);
        return Ok(column_ids);
    }

    for idx in projection {
        let column = metadata
            .column_metadatas
            .get(*idx)
            .context(InvalidRequestSnafu {
                region_id: metadata.region_id,
                reason: format!("projection index {} is out of bound", idx),
            })?;
        column_ids.push(column.column_id);
    }
    Ok(column_ids)
}

/// Index of a vector in a [Batch].
#[derive(Debug, Clone, Copy)]
enum BatchIndex {
    /// Index in primary keys.
    Tag((usize, ColumnId)),
    /// The time index column.
    Timestamp,
    /// Index in fields.
    Field(usize),
}

/// Gets a vector with repeated values from specific cache or creates a new one.
fn repeated_vector_with_cache(
    data_type: &ConcreteDataType,
    value: &Value,
    num_rows: usize,
    cache_strategy: &CacheStrategy,
) -> common_recordbatch::error::Result<VectorRef> {
    if let Some(vector) = cache_strategy.get_repeated_vector(data_type, value) {
        // Tries to get the vector from cache manager. If the vector doesn't
        // have enough length, creates a new one.
        match vector.len().cmp(&num_rows) {
            Ordering::Less => (),
            Ordering::Equal => return Ok(vector),
            Ordering::Greater => return Ok(vector.slice(0, num_rows)),
        }
    }

    // Creates a new one.
    let vector = new_repeated_vector(data_type, value, num_rows)?;
    // Updates cache.
    if vector.len() <= MAX_VECTOR_LENGTH_TO_CACHE {
        cache_strategy.put_repeated_vector(value.clone(), vector.clone());
    }

    Ok(vector)
}

/// Returns a vector with repeated values.
fn new_repeated_vector(
    data_type: &ConcreteDataType,
    value: &Value,
    num_rows: usize,
) -> common_recordbatch::error::Result<VectorRef> {
    let mut mutable_vector = data_type.create_mutable_vector(1);
    mutable_vector
        .try_push_value_ref(&value.as_value_ref())
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    // This requires an additional allocation.
    let base_vector = mutable_vector.to_vector();
    Ok(base_vector.replicate(&[num_rows]))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::OpType;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray, UInt8Array, UInt64Array};
    use datatypes::arrow::datatypes::Field;
    use datatypes::arrow::util::pretty;
    use datatypes::value::ValueRef;
    use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};
    use mito_codec::test_util::TestRegionMetadataBuilder;
    use store_api::storage::consts::{
        OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
    };

    use super::*;
    use crate::cache::CacheManager;
    use crate::read::BatchBuilder;

    fn new_batch(
        ts_start: i64,
        tags: &[i64],
        fields: &[(ColumnId, i64)],
        num_rows: usize,
    ) -> Batch {
        let converter = DensePrimaryKeyCodec::with_fields(
            (0..tags.len())
                .map(|idx| {
                    (
                        idx as u32,
                        SortField::new(ConcreteDataType::int64_datatype()),
                    )
                })
                .collect(),
        );
        let primary_key = converter
            .encode(tags.iter().map(|v| ValueRef::Int64(*v)))
            .unwrap();

        let mut builder = BatchBuilder::new(primary_key);
        builder
            .timestamps_array(Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..num_rows).map(|i| ts_start + i as i64 * 1000),
            )))
            .unwrap()
            .sequences_array(Arc::new(UInt64Array::from_iter_values(0..num_rows as u64)))
            .unwrap()
            .op_types_array(Arc::new(UInt8Array::from_iter_values(
                (0..num_rows).map(|_| OpType::Put as u8),
            )))
            .unwrap();
        for (column_id, field) in fields {
            builder
                .push_field_array(
                    *column_id,
                    Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                        *field, num_rows,
                    ))),
                )
                .unwrap();
        }
        builder.build().unwrap()
    }

    fn print_record_batch(record_batch: RecordBatch) -> String {
        pretty::pretty_format_batches(&[record_batch.into_df_record_batch()])
            .unwrap()
            .to_string()
    }

    #[test]
    fn test_projection_mapper_all() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        // Create the enum wrapper with default format (primary key)
        let mapper = ProjectionMapper::all(&metadata, false).unwrap();
        assert_eq!([0, 1, 2, 3, 4], mapper.column_ids());
        assert_eq!(
            [
                (3, ConcreteDataType::int64_datatype()),
                (4, ConcreteDataType::int64_datatype())
            ],
            mapper.as_primary_key().unwrap().batch_fields()
        );

        // With vector cache.
        let cache = CacheManager::builder().vector_cache_size(1024).build();
        let cache = CacheStrategy::EnableAll(Arc::new(cache));
        let batch = new_batch(0, &[1, 2], &[(3, 3), (4, 4)], 3);
        let record_batch = mapper
            .as_primary_key()
            .unwrap()
            .convert(&batch, &cache)
            .unwrap();
        let expect = "\
+---------------------+----+----+----+----+
| ts                  | k0 | k1 | v0 | v1 |
+---------------------+----+----+----+----+
| 1970-01-01T00:00:00 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:01 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:02 | 1  | 2  | 3  | 4  |
+---------------------+----+----+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));

        assert!(
            cache
                .get_repeated_vector(&ConcreteDataType::int64_datatype(), &Value::Int64(1))
                .is_some()
        );
        assert!(
            cache
                .get_repeated_vector(&ConcreteDataType::int64_datatype(), &Value::Int64(2))
                .is_some()
        );
        assert!(
            cache
                .get_repeated_vector(&ConcreteDataType::int64_datatype(), &Value::Int64(3))
                .is_none()
        );
        let record_batch = mapper
            .as_primary_key()
            .unwrap()
            .convert(&batch, &cache)
            .unwrap();
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_projection_mapper_with_projection() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        // Columns v1, k0
        let mapper = ProjectionMapper::new(&metadata, [4, 1].into_iter(), false).unwrap();
        assert_eq!([4, 1], mapper.column_ids());
        assert_eq!(
            [(4, ConcreteDataType::int64_datatype())],
            mapper.as_primary_key().unwrap().batch_fields()
        );

        let batch = new_batch(0, &[1, 2], &[(4, 4)], 3);
        let cache = CacheManager::builder().vector_cache_size(1024).build();
        let cache = CacheStrategy::EnableAll(Arc::new(cache));
        let record_batch = mapper
            .as_primary_key()
            .unwrap()
            .convert(&batch, &cache)
            .unwrap();
        let expect = "\
+----+----+
| v1 | k0 |
+----+----+
| 4  | 1  |
| 4  | 1  |
| 4  | 1  |
+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_projection_mapper_read_superset() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        // Output columns v1, k0. Read also includes v0.
        let mapper = ProjectionMapper::new_with_read_columns(
            &metadata,
            [4, 1].into_iter(),
            false,
            vec![4, 1, 3],
        )
        .unwrap();
        assert_eq!([4, 1, 3], mapper.column_ids());

        let batch = new_batch(0, &[1, 2], &[(3, 3), (4, 4)], 3);
        let cache = CacheManager::builder().vector_cache_size(1024).build();
        let cache = CacheStrategy::EnableAll(Arc::new(cache));
        let record_batch = mapper
            .as_primary_key()
            .unwrap()
            .convert(&batch, &cache)
            .unwrap();
        let expect = "\
+----+----+
| v1 | k0 |
+----+----+
| 4  | 1  |
| 4  | 1  |
| 4  | 1  |
+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_projection_mapper_empty_projection() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        // Empty projection
        let mapper = ProjectionMapper::new(&metadata, [].into_iter(), false).unwrap();
        assert_eq!([0], mapper.column_ids()); // Should still read the time index column
        assert!(mapper.output_schema().is_empty());
        let pk_mapper = mapper.as_primary_key().unwrap();
        assert!(pk_mapper.batch_fields().is_empty());
        assert!(!pk_mapper.has_tags);
        assert!(pk_mapper.batch_indices.is_empty());
        assert!(pk_mapper.is_empty_projection);

        let batch = new_batch(0, &[1, 2], &[], 3);
        let cache = CacheManager::builder().vector_cache_size(1024).build();
        let cache = CacheStrategy::EnableAll(Arc::new(cache));
        let record_batch = pk_mapper.convert(&batch, &cache).unwrap();
        assert_eq!(3, record_batch.num_rows());
        assert_eq!(0, record_batch.num_columns());
        assert!(record_batch.schema.is_empty());
    }

    fn new_flat_batch(
        ts_start: Option<i64>,
        idx_tags: &[(usize, i64)],
        idx_fields: &[(usize, i64)],
        num_rows: usize,
    ) -> datatypes::arrow::record_batch::RecordBatch {
        let mut columns = Vec::with_capacity(1 + idx_tags.len() + idx_fields.len() + 3);
        let mut fields = Vec::with_capacity(1 + idx_tags.len() + idx_fields.len() + 3);

        // Flat format: primary key columns, field columns, time index, __primary_key, __sequence, __op_type

        // Primary key columns first
        for (i, tag) in idx_tags {
            let array = Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                *tag, num_rows,
            ))) as _;
            columns.push(array);
            fields.push(Field::new(
                format!("k{i}"),
                datatypes::arrow::datatypes::DataType::Int64,
                true,
            ));
        }

        // Field columns
        for (i, field) in idx_fields {
            let array = Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                *field, num_rows,
            ))) as _;
            columns.push(array);
            fields.push(Field::new(
                format!("v{i}"),
                datatypes::arrow::datatypes::DataType::Int64,
                true,
            ));
        }

        // Time index
        if let Some(ts_start) = ts_start {
            let timestamps = Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..num_rows).map(|i| ts_start + i as i64 * 1000),
            )) as _;
            columns.push(timestamps);
            fields.push(Field::new(
                "ts",
                datatypes::arrow::datatypes::DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Millisecond,
                    None,
                ),
                true,
            ));
        }

        // __primary_key column (encoded primary key as dictionary)
        // Create encoded primary key
        let converter = DensePrimaryKeyCodec::with_fields(
            (0..idx_tags.len())
                .map(|idx| {
                    (
                        idx as u32,
                        SortField::new(ConcreteDataType::int64_datatype()),
                    )
                })
                .collect(),
        );
        let encoded_pk = converter
            .encode(idx_tags.iter().map(|(_, v)| ValueRef::Int64(*v)))
            .unwrap();

        // Create dictionary array for the encoded primary key
        let pk_values: Vec<&[u8]> = std::iter::repeat_n(encoded_pk.as_slice(), num_rows).collect();
        let keys = datatypes::arrow::array::UInt32Array::from_iter(0..num_rows as u32);
        let values = Arc::new(datatypes::arrow::array::BinaryArray::from_vec(pk_values));
        let pk_array =
            Arc::new(datatypes::arrow::array::DictionaryArray::try_new(keys, values).unwrap()) as _;
        columns.push(pk_array);
        fields.push(Field::new_dictionary(
            PRIMARY_KEY_COLUMN_NAME,
            datatypes::arrow::datatypes::DataType::UInt32,
            datatypes::arrow::datatypes::DataType::Binary,
            false,
        ));

        // __sequence column
        columns.push(Arc::new(UInt64Array::from_iter_values(0..num_rows as u64)) as _);
        fields.push(Field::new(
            SEQUENCE_COLUMN_NAME,
            datatypes::arrow::datatypes::DataType::UInt64,
            false,
        ));

        // __op_type column
        columns.push(Arc::new(UInt8Array::from_iter_values(
            (0..num_rows).map(|_| OpType::Put as u8),
        )) as _);
        fields.push(Field::new(
            OP_TYPE_COLUMN_NAME,
            datatypes::arrow::datatypes::DataType::UInt8,
            false,
        ));

        let schema = Arc::new(datatypes::arrow::datatypes::Schema::new(fields));

        datatypes::arrow::record_batch::RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn test_flat_projection_mapper_all() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        let mapper = ProjectionMapper::all(&metadata, true).unwrap();
        assert_eq!([0, 1, 2, 3, 4], mapper.column_ids());
        assert_eq!(
            [
                (1, ConcreteDataType::int64_datatype()),
                (2, ConcreteDataType::int64_datatype()),
                (3, ConcreteDataType::int64_datatype()),
                (4, ConcreteDataType::int64_datatype()),
                (0, ConcreteDataType::timestamp_millisecond_datatype())
            ],
            mapper.as_flat().unwrap().batch_schema()
        );

        let batch = new_flat_batch(Some(0), &[(1, 1), (2, 2)], &[(3, 3), (4, 4)], 3);
        let record_batch = mapper.as_flat().unwrap().convert(&batch).unwrap();
        let expect = "\
+---------------------+----+----+----+----+
| ts                  | k0 | k1 | v0 | v1 |
+---------------------+----+----+----+----+
| 1970-01-01T00:00:00 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:01 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:02 | 1  | 2  | 3  | 4  |
+---------------------+----+----+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_flat_projection_mapper_with_projection() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        // Columns v1, k0
        let mapper = ProjectionMapper::new(&metadata, [4, 1].into_iter(), true).unwrap();
        assert_eq!([4, 1], mapper.column_ids());
        assert_eq!(
            [
                (1, ConcreteDataType::int64_datatype()),
                (4, ConcreteDataType::int64_datatype()),
                (0, ConcreteDataType::timestamp_millisecond_datatype())
            ],
            mapper.as_flat().unwrap().batch_schema()
        );

        let batch = new_flat_batch(None, &[(1, 1)], &[(4, 4)], 3);
        let record_batch = mapper.as_flat().unwrap().convert(&batch).unwrap();
        let expect = "\
+----+----+
| v1 | k0 |
+----+----+
| 4  | 1  |
| 4  | 1  |
| 4  | 1  |
+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_flat_projection_mapper_read_superset() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        // Output columns v1, k0. Read also includes v0.
        let mapper = ProjectionMapper::new_with_read_columns(
            &metadata,
            [4, 1].into_iter(),
            true,
            vec![4, 1, 3],
        )
        .unwrap();
        assert_eq!([4, 1, 3], mapper.column_ids());

        let batch = new_flat_batch(None, &[(1, 1)], &[(3, 3), (4, 4)], 3);
        let record_batch = mapper.as_flat().unwrap().convert(&batch).unwrap();
        let expect = "\
+----+----+
| v1 | k0 |
+----+----+
| 4  | 1  |
| 4  | 1  |
| 4  | 1  |
+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_flat_projection_mapper_empty_projection() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        // Empty projection
        let mapper = ProjectionMapper::new(&metadata, [].into_iter(), true).unwrap();
        assert_eq!([0], mapper.column_ids()); // Should still read the time index column
        assert!(mapper.output_schema().is_empty());
        let flat_mapper = mapper.as_flat().unwrap();
        assert_eq!(
            [(0, ConcreteDataType::timestamp_millisecond_datatype())],
            flat_mapper.batch_schema()
        );

        let batch = new_flat_batch(Some(0), &[], &[], 3);
        let record_batch = flat_mapper.convert(&batch).unwrap();
        assert_eq!(3, record_batch.num_rows());
        assert_eq!(0, record_batch.num_columns());
        assert!(record_batch.schema.is_empty());
    }
}
