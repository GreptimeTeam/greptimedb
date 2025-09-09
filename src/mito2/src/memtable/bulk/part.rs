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

//! Bulk part encoder/decoder.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::helper::{ColumnDataTypeWrapper, value_to_grpc_value};
use api::v1::bulk_wal_entry::Body;
use api::v1::{ArrowIpc, BulkWalEntry, Mutation, OpType, bulk_wal_entry};
use bytes::Bytes;
use common_grpc::flight::{FlightDecoder, FlightEncoder, FlightMessage};
use common_recordbatch::DfRecordBatch as RecordBatch;
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datatypes::arrow;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryBuilder, BinaryDictionaryBuilder, DictionaryArray, StringBuilder,
    StringDictionaryBuilder, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt8Builder, UInt32Array,
    UInt64Array, UInt64Builder,
};
use datatypes::arrow::compute::{SortColumn, SortOptions, TakeOptions};
use datatypes::arrow::datatypes::{SchemaRef, UInt32Type};
use datatypes::arrow_array::BinaryArray;
use datatypes::data_type::DataType;
use datatypes::prelude::{MutableVector, ScalarVectorBuilder, Vector};
use datatypes::value::{Value, ValueRef};
use datatypes::vectors::Helper;
use mito_codec::key_values::{KeyValue, KeyValues, KeyValuesRef};
use mito_codec::row_converter::{
    DensePrimaryKeyCodec, PrimaryKeyCodec, PrimaryKeyCodecExt, build_primary_key_codec,
};
use parquet::arrow::ArrowWriter;
use parquet::data_type::AsBytes;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use snafu::{OptionExt, ResultExt, Snafu};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::SequenceNumber;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use table::predicate::Predicate;

use crate::error::{
    self, ColumnNotFoundSnafu, ComputeArrowSnafu, DataTypeMismatchSnafu, EncodeMemtableSnafu,
    EncodeSnafu, InvalidMetadataSnafu, NewRecordBatchSnafu, Result,
};
use crate::memtable::BoxedRecordBatchIterator;
use crate::memtable::bulk::context::BulkIterContextRef;
use crate::memtable::bulk::part_reader::EncodedBulkPartIter;
use crate::memtable::time_series::{ValueBuilder, Values};
use crate::sst::file::FileId;
use crate::sst::index::IndexOutput;
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::format::{PrimaryKeyArray, PrimaryKeyArrayBuilder, ReadFormat};
use crate::sst::parquet::helper::parse_parquet_metadata;
use crate::sst::parquet::{PARQUET_METADATA_KEY, SstInfo};
use crate::sst::to_sst_arrow_schema;

const INIT_DICT_VALUE_CAPACITY: usize = 8;

/// A raw bulk part in the memtable.
#[derive(Clone)]
pub struct BulkPart {
    pub batch: RecordBatch,
    pub max_timestamp: i64,
    pub min_timestamp: i64,
    pub sequence: u64,
    pub timestamp_index: usize,
    pub raw_data: Option<ArrowIpc>,
}

impl TryFrom<BulkWalEntry> for BulkPart {
    type Error = error::Error;

    fn try_from(value: BulkWalEntry) -> std::result::Result<Self, Self::Error> {
        match value.body.expect("Entry payload should be present") {
            Body::ArrowIpc(ipc) => {
                let mut decoder = FlightDecoder::try_from_schema_bytes(&ipc.schema)
                    .context(error::ConvertBulkWalEntrySnafu)?;
                let batch = decoder
                    .try_decode_record_batch(&ipc.data_header, &ipc.payload)
                    .context(error::ConvertBulkWalEntrySnafu)?;
                Ok(Self {
                    batch,
                    max_timestamp: value.max_ts,
                    min_timestamp: value.min_ts,
                    sequence: value.sequence,
                    timestamp_index: value.timestamp_index as usize,
                    raw_data: Some(ipc),
                })
            }
        }
    }
}

impl TryFrom<&BulkPart> for BulkWalEntry {
    type Error = error::Error;

    fn try_from(value: &BulkPart) -> Result<Self> {
        if let Some(ipc) = &value.raw_data {
            Ok(BulkWalEntry {
                sequence: value.sequence,
                max_ts: value.max_timestamp,
                min_ts: value.min_timestamp,
                timestamp_index: value.timestamp_index as u32,
                body: Some(Body::ArrowIpc(ipc.clone())),
            })
        } else {
            let mut encoder = FlightEncoder::default();
            let schema_bytes = encoder
                .encode_schema(value.batch.schema().as_ref())
                .data_header;
            let [rb_data] = encoder
                .encode(FlightMessage::RecordBatch(value.batch.clone()))
                .try_into()
                .map_err(|_| {
                    error::UnsupportedOperationSnafu {
                        err_msg: "create BulkWalEntry from RecordBatch with dictionary arrays",
                    }
                    .build()
                })?;
            Ok(BulkWalEntry {
                sequence: value.sequence,
                max_ts: value.max_timestamp,
                min_ts: value.min_timestamp,
                timestamp_index: value.timestamp_index as u32,
                body: Some(Body::ArrowIpc(ArrowIpc {
                    schema: schema_bytes,
                    data_header: rb_data.data_header,
                    payload: rb_data.data_body,
                })),
            })
        }
    }
}

impl BulkPart {
    pub(crate) fn estimated_size(&self) -> usize {
        record_batch_estimated_size(&self.batch)
    }

    /// Returns the estimated series count in this BulkPart.
    /// This is calculated from the dictionary values count of the PrimaryKeyArray.
    pub fn estimated_series_count(&self) -> usize {
        let pk_column_idx = primary_key_column_index(self.batch.num_columns());
        let pk_column = self.batch.column(pk_column_idx);
        if let Some(dict_array) = pk_column.as_any().downcast_ref::<PrimaryKeyArray>() {
            dict_array.values().len()
        } else {
            0
        }
    }

    /// Converts [BulkPart] to [Mutation] for fallback `write_bulk` implementation.
    pub(crate) fn to_mutation(&self, region_metadata: &RegionMetadataRef) -> Result<Mutation> {
        let vectors = region_metadata
            .schema
            .column_schemas()
            .iter()
            .map(|col| match self.batch.column_by_name(&col.name) {
                None => Ok(None),
                Some(col) => Helper::try_into_vector(col).map(Some),
            })
            .collect::<datatypes::error::Result<Vec<_>>>()
            .context(error::ComputeVectorSnafu)?;

        let rows = (0..self.num_rows())
            .map(|row_idx| {
                let values = (0..self.batch.num_columns())
                    .map(|col_idx| {
                        if let Some(v) = &vectors[col_idx] {
                            value_to_grpc_value(v.get(row_idx))
                        } else {
                            api::v1::Value { value_data: None }
                        }
                    })
                    .collect::<Vec<_>>();
                api::v1::Row { values }
            })
            .collect::<Vec<_>>();

        let schema = region_metadata
            .column_metadatas
            .iter()
            .map(|c| {
                let data_type_wrapper =
                    ColumnDataTypeWrapper::try_from(c.column_schema.data_type.clone())?;
                Ok(api::v1::ColumnSchema {
                    column_name: c.column_schema.name.clone(),
                    datatype: data_type_wrapper.datatype() as i32,
                    semantic_type: c.semantic_type as i32,
                    ..Default::default()
                })
            })
            .collect::<api::error::Result<Vec<_>>>()
            .context(error::ConvertColumnDataTypeSnafu {
                reason: "failed to convert region metadata to column schema",
            })?;

        let rows = api::v1::Rows { schema, rows };

        Ok(Mutation {
            op_type: OpType::Put as i32,
            sequence: self.sequence,
            rows: Some(rows),
            write_hint: None,
        })
    }

    pub fn timestamps(&self) -> &ArrayRef {
        self.batch.column(self.timestamp_index)
    }

    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

/// More accurate estimation of the size of a record batch.
pub(crate) fn record_batch_estimated_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        // If can not get slice memory size, assume 0 here.
        .map(|c| c.to_data().get_slice_memory_size().unwrap_or(0))
        .sum()
}

/// Primary key column builder for handling strings specially.
enum PrimaryKeyColumnBuilder {
    /// String dictionary builder for string types.
    StringDict(StringDictionaryBuilder<UInt32Type>),
    /// Generic mutable vector for other types.
    Vector(Box<dyn MutableVector>),
}

impl PrimaryKeyColumnBuilder {
    /// Appends a value to the builder.
    fn push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        match self {
            PrimaryKeyColumnBuilder::StringDict(builder) => {
                if let Some(s) = value.as_string().context(DataTypeMismatchSnafu)? {
                    // We know the value is a string.
                    builder.append_value(s);
                } else {
                    builder.append_null();
                }
            }
            PrimaryKeyColumnBuilder::Vector(builder) => {
                builder.push_value_ref(value);
            }
        }
        Ok(())
    }

    /// Converts the builder to an ArrayRef.
    fn into_arrow_array(self) -> ArrayRef {
        match self {
            PrimaryKeyColumnBuilder::StringDict(mut builder) => Arc::new(builder.finish()),
            PrimaryKeyColumnBuilder::Vector(mut builder) => builder.to_vector().to_arrow_array(),
        }
    }
}

/// Converter that converts structs into [BulkPart].
pub struct BulkPartConverter {
    /// Region metadata.
    region_metadata: RegionMetadataRef,
    /// Schema of the converted batch.
    schema: SchemaRef,
    /// Primary key codec for encoding keys
    primary_key_codec: Arc<dyn PrimaryKeyCodec>,
    /// Buffer for encoding primary key.
    key_buf: Vec<u8>,
    /// Primary key array builder.
    key_array_builder: PrimaryKeyArrayBuilder,
    /// Builders for non-primary key columns.
    value_builder: ValueBuilder,
    /// Builders for individual primary key columns.
    /// The order of builders is the same as the order of primary key columns in the region metadata.
    primary_key_column_builders: Vec<PrimaryKeyColumnBuilder>,

    /// Max timestamp value.
    max_ts: i64,
    /// Min timestamp value.
    min_ts: i64,
    /// Max sequence number.
    max_sequence: SequenceNumber,
}

impl BulkPartConverter {
    /// Creates a new converter.
    ///
    /// If `store_primary_key_columns` is true and the encoding is not sparse encoding, it
    /// stores primary key columns in arrays additionally.
    pub fn new(
        region_metadata: &RegionMetadataRef,
        schema: SchemaRef,
        capacity: usize,
        primary_key_codec: Arc<dyn PrimaryKeyCodec>,
        store_primary_key_columns: bool,
    ) -> Self {
        debug_assert_eq!(
            region_metadata.primary_key_encoding,
            primary_key_codec.encoding()
        );

        let primary_key_column_builders = if store_primary_key_columns
            && region_metadata.primary_key_encoding != PrimaryKeyEncoding::Sparse
        {
            new_primary_key_column_builders(region_metadata, capacity)
        } else {
            Vec::new()
        };

        Self {
            region_metadata: region_metadata.clone(),
            schema,
            primary_key_codec,
            key_buf: Vec::new(),
            key_array_builder: PrimaryKeyArrayBuilder::new(),
            value_builder: ValueBuilder::new(region_metadata, capacity),
            primary_key_column_builders,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            max_sequence: SequenceNumber::MIN,
        }
    }

    /// Appends a [KeyValues] into the converter.
    pub fn append_key_values(&mut self, key_values: &KeyValues) -> Result<()> {
        for kv in key_values.iter() {
            self.append_key_value(&kv)?;
        }

        Ok(())
    }

    /// Appends a [KeyValue] to builders.
    ///
    /// If the primary key uses sparse encoding, callers must encoded the primary key in the [KeyValue].
    fn append_key_value(&mut self, kv: &KeyValue) -> Result<()> {
        // Handles primary key based on encoding type
        if self.primary_key_codec.encoding() == PrimaryKeyEncoding::Sparse {
            // For sparse encoding, the primary key is already encoded in the KeyValue
            // Gets the first (and only) primary key value which contains the encoded key
            let mut primary_keys = kv.primary_keys();
            if let Some(encoded) = primary_keys
                .next()
                .context(ColumnNotFoundSnafu {
                    column: PRIMARY_KEY_COLUMN_NAME,
                })?
                .as_binary()
                .context(DataTypeMismatchSnafu)?
            {
                self.key_array_builder
                    .append(encoded)
                    .context(ComputeArrowSnafu)?;
            } else {
                self.key_array_builder
                    .append("")
                    .context(ComputeArrowSnafu)?;
            }
        } else {
            // For dense encoding, we need to encode the primary key columns
            self.key_buf.clear();
            self.primary_key_codec
                .encode_key_value(kv, &mut self.key_buf)
                .context(EncodeSnafu)?;
            self.key_array_builder
                .append(&self.key_buf)
                .context(ComputeArrowSnafu)?;
        };

        // If storing primary key columns, append values to individual builders
        if !self.primary_key_column_builders.is_empty() {
            for (builder, pk_value) in self
                .primary_key_column_builders
                .iter_mut()
                .zip(kv.primary_keys())
            {
                builder.push_value_ref(pk_value)?;
            }
        }

        // Pushes other columns.
        self.value_builder.push(
            kv.timestamp(),
            kv.sequence(),
            kv.op_type() as u8,
            kv.fields(),
        );

        // Updates statistics
        // Safety: timestamp of kv must be both present and a valid timestamp value.
        let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
        self.min_ts = self.min_ts.min(ts);
        self.max_ts = self.max_ts.max(ts);
        self.max_sequence = self.max_sequence.max(kv.sequence());

        Ok(())
    }

    /// Converts buffered content into a [BulkPart].
    ///
    /// It sorts the record batch by (primary key, timestamp, sequence desc).
    pub fn convert(mut self) -> Result<BulkPart> {
        let values = Values::from(self.value_builder);
        let mut columns =
            Vec::with_capacity(4 + values.fields.len() + self.primary_key_column_builders.len());

        // Build primary key column arrays if enabled.
        for builder in self.primary_key_column_builders {
            columns.push(builder.into_arrow_array());
        }
        // Then fields columns.
        columns.extend(values.fields.iter().map(|field| field.to_arrow_array()));
        // Time index.
        let timestamp_index = columns.len();
        columns.push(values.timestamp.to_arrow_array());
        // Primary key.
        let pk_array = self.key_array_builder.finish();
        columns.push(Arc::new(pk_array));
        // Sequence and op type.
        columns.push(values.sequence.to_arrow_array());
        columns.push(values.op_type.to_arrow_array());

        let batch = RecordBatch::try_new(self.schema, columns).context(NewRecordBatchSnafu)?;
        // Sorts the record batch.
        let batch = sort_primary_key_record_batch(&batch)?;

        Ok(BulkPart {
            batch,
            max_timestamp: self.max_ts,
            min_timestamp: self.min_ts,
            sequence: self.max_sequence,
            timestamp_index,
            raw_data: None,
        })
    }
}

fn new_primary_key_column_builders(
    metadata: &RegionMetadata,
    capacity: usize,
) -> Vec<PrimaryKeyColumnBuilder> {
    metadata
        .primary_key_columns()
        .map(|col| {
            if col.column_schema.data_type.is_string() {
                PrimaryKeyColumnBuilder::StringDict(StringDictionaryBuilder::with_capacity(
                    capacity,
                    INIT_DICT_VALUE_CAPACITY,
                    capacity,
                ))
            } else {
                PrimaryKeyColumnBuilder::Vector(
                    col.column_schema.data_type.create_mutable_vector(capacity),
                )
            }
        })
        .collect()
}

/// Sorts the record batch with primary key format.
fn sort_primary_key_record_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let total_columns = batch.num_columns();
    let sort_columns = vec![
        // Primary key column (ascending)
        SortColumn {
            values: batch.column(total_columns - 3).clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        },
        // Time index column (ascending)
        SortColumn {
            values: batch.column(total_columns - 4).clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        },
        // Sequence column (descending)
        SortColumn {
            values: batch.column(total_columns - 2).clone(),
            options: Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
        },
    ];

    let indices = datatypes::arrow::compute::lexsort_to_indices(&sort_columns, None)
        .context(ComputeArrowSnafu)?;

    datatypes::arrow::compute::take_record_batch(batch, &indices).context(ComputeArrowSnafu)
}

#[derive(Debug, Clone)]
pub struct EncodedBulkPart {
    data: Bytes,
    metadata: BulkPartMeta,
}

impl EncodedBulkPart {
    pub fn new(data: Bytes, metadata: BulkPartMeta) -> Self {
        Self { data, metadata }
    }

    pub(crate) fn metadata(&self) -> &BulkPartMeta {
        &self.metadata
    }

    /// Returns the size of the encoded data in bytes
    pub(crate) fn size_bytes(&self) -> usize {
        self.data.len()
    }

    /// Returns the encoded data.
    pub(crate) fn data(&self) -> &Bytes {
        &self.data
    }

    /// Converts this `EncodedBulkPart` to `SstInfo`.
    ///
    /// # Arguments
    /// * `file_id` - The SST file ID to assign to this part
    ///
    /// # Returns
    /// Returns a `SstInfo` instance with information derived from this bulk part's metadata
    pub(crate) fn to_sst_info(&self, file_id: FileId) -> SstInfo {
        let unit = self.metadata.region_metadata.time_index_type().unit();
        SstInfo {
            file_id,
            time_range: (
                Timestamp::new(self.metadata.min_timestamp, unit),
                Timestamp::new(self.metadata.max_timestamp, unit),
            ),
            file_size: self.data.len() as u64,
            num_rows: self.metadata.num_rows,
            num_row_groups: self.metadata.parquet_metadata.num_row_groups() as u64,
            file_metadata: Some(self.metadata.parquet_metadata.clone()),
            index_metadata: IndexOutput::default(),
        }
    }

    pub(crate) fn read(
        &self,
        context: BulkIterContextRef,
        sequence: Option<SequenceNumber>,
    ) -> Result<Option<BoxedRecordBatchIterator>> {
        // use predicate to find row groups to read.
        let row_groups_to_read = context.row_groups_to_read(&self.metadata.parquet_metadata);

        if row_groups_to_read.is_empty() {
            // All row groups are filtered.
            return Ok(None);
        }

        let iter = EncodedBulkPartIter::try_new(
            context,
            row_groups_to_read,
            self.metadata.parquet_metadata.clone(),
            self.data.clone(),
            sequence,
        )?;
        Ok(Some(Box::new(iter) as BoxedRecordBatchIterator))
    }
}

#[derive(Debug, Clone)]
pub struct BulkPartMeta {
    /// Total rows in part.
    pub num_rows: usize,
    /// Max timestamp in part.
    pub max_timestamp: i64,
    /// Min timestamp in part.
    pub min_timestamp: i64,
    /// Part file metadata.
    pub parquet_metadata: Arc<ParquetMetaData>,
    /// Part region schema.
    pub region_metadata: RegionMetadataRef,
}

/// Metrics for encoding a part.
#[derive(Default, Debug)]
pub struct BulkPartEncodeMetrics {
    /// Cost of iterating over the data.
    pub iter_cost: Duration,
    /// Cost of writing the data.
    pub write_cost: Duration,
    /// Size of data before encoding.
    pub raw_size: usize,
    /// Size of data after encoding.
    pub encoded_size: usize,
    /// Number of rows in part.
    pub num_rows: usize,
}

pub struct BulkPartEncoder {
    metadata: RegionMetadataRef,
    row_group_size: usize,
    writer_props: Option<WriterProperties>,
}

impl BulkPartEncoder {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        row_group_size: usize,
    ) -> Result<BulkPartEncoder> {
        // TODO(yingwen): Skip arrow schema if needed.
        let json = metadata.to_json().context(InvalidMetadataSnafu)?;
        let key_value_meta =
            parquet::file::metadata::KeyValue::new(PARQUET_METADATA_KEY.to_string(), json);

        // TODO(yingwen): Do we need compression?
        let writer_props = Some(
            WriterProperties::builder()
                .set_key_value_metadata(Some(vec![key_value_meta]))
                .set_write_batch_size(row_group_size)
                .set_max_row_group_size(row_group_size)
                .build(),
        );

        Ok(Self {
            metadata,
            row_group_size,
            writer_props,
        })
    }
}

impl BulkPartEncoder {
    /// Encodes [BoxedRecordBatchIterator] into [EncodedBulkPart] with min/max timestamps.
    pub fn encode_record_batch_iter(
        &self,
        iter: BoxedRecordBatchIterator,
        arrow_schema: SchemaRef,
        min_timestamp: i64,
        max_timestamp: i64,
        metrics: &mut BulkPartEncodeMetrics,
    ) -> Result<Option<EncodedBulkPart>> {
        let mut buf = Vec::with_capacity(4096);
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, self.writer_props.clone())
            .context(EncodeMemtableSnafu)?;
        let mut total_rows = 0;

        // Process each batch from the iterator
        let mut iter_start = Instant::now();
        for batch_result in iter {
            metrics.iter_cost += iter_start.elapsed();
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }

            metrics.raw_size += record_batch_estimated_size(&batch);
            let write_start = Instant::now();
            writer.write(&batch).context(EncodeMemtableSnafu)?;
            metrics.write_cost += write_start.elapsed();
            total_rows += batch.num_rows();
            iter_start = Instant::now();
        }
        metrics.iter_cost += iter_start.elapsed();
        iter_start = Instant::now();

        if total_rows == 0 {
            return Ok(None);
        }

        let close_start = Instant::now();
        let file_metadata = writer.close().context(EncodeMemtableSnafu)?;
        metrics.write_cost += close_start.elapsed();
        metrics.encoded_size += buf.len();
        metrics.num_rows += total_rows;

        let buf = Bytes::from(buf);
        let parquet_metadata = Arc::new(parse_parquet_metadata(file_metadata)?);

        Ok(Some(EncodedBulkPart {
            data: buf,
            metadata: BulkPartMeta {
                num_rows: total_rows,
                max_timestamp,
                min_timestamp,
                parquet_metadata,
                region_metadata: self.metadata.clone(),
            },
        }))
    }

    /// Encodes bulk part to a [EncodedBulkPart], returns the encoded data.
    fn encode_part(&self, part: &BulkPart) -> Result<Option<EncodedBulkPart>> {
        if part.batch.num_rows() == 0 {
            return Ok(None);
        }

        let mut buf = Vec::with_capacity(4096);
        let arrow_schema = part.batch.schema();

        let file_metadata = {
            let mut writer =
                ArrowWriter::try_new(&mut buf, arrow_schema, self.writer_props.clone())
                    .context(EncodeMemtableSnafu)?;
            writer.write(&part.batch).context(EncodeMemtableSnafu)?;
            writer.finish().context(EncodeMemtableSnafu)?
        };

        let buf = Bytes::from(buf);
        let parquet_metadata = Arc::new(parse_parquet_metadata(file_metadata)?);

        Ok(Some(EncodedBulkPart {
            data: buf,
            metadata: BulkPartMeta {
                num_rows: part.batch.num_rows(),
                max_timestamp: part.max_timestamp,
                min_timestamp: part.min_timestamp,
                parquet_metadata,
                region_metadata: self.metadata.clone(),
            },
        }))
    }
}

/// Converts mutations to record batches.
fn mutations_to_record_batch(
    mutations: &[Mutation],
    metadata: &RegionMetadataRef,
    pk_encoder: &DensePrimaryKeyCodec,
    dedup: bool,
) -> Result<Option<(RecordBatch, i64, i64)>> {
    let total_rows: usize = mutations
        .iter()
        .map(|m| m.rows.as_ref().map(|r| r.rows.len()).unwrap_or(0))
        .sum();

    if total_rows == 0 {
        return Ok(None);
    }

    let mut pk_builder = BinaryBuilder::with_capacity(total_rows, 0);

    let mut ts_vector: Box<dyn MutableVector> = metadata
        .time_index_column()
        .column_schema
        .data_type
        .create_mutable_vector(total_rows);
    let mut sequence_builder = UInt64Builder::with_capacity(total_rows);
    let mut op_type_builder = UInt8Builder::with_capacity(total_rows);

    let mut field_builders: Vec<Box<dyn MutableVector>> = metadata
        .field_columns()
        .map(|f| f.column_schema.data_type.create_mutable_vector(total_rows))
        .collect();

    let mut pk_buffer = vec![];
    for m in mutations {
        let Some(key_values) = KeyValuesRef::new(metadata, m) else {
            continue;
        };

        for row in key_values.iter() {
            pk_buffer.clear();
            pk_encoder
                .encode_to_vec(row.primary_keys(), &mut pk_buffer)
                .context(EncodeSnafu)?;
            pk_builder.append_value(pk_buffer.as_bytes());
            ts_vector.push_value_ref(row.timestamp());
            sequence_builder.append_value(row.sequence());
            op_type_builder.append_value(row.op_type() as u8);
            for (builder, field) in field_builders.iter_mut().zip(row.fields()) {
                builder.push_value_ref(field);
            }
        }
    }

    let arrow_schema = to_sst_arrow_schema(metadata);
    // safety: timestamp column must be valid, and values must not be None.
    let timestamp_unit = metadata
        .time_index_column()
        .column_schema
        .data_type
        .as_timestamp()
        .unwrap()
        .unit();
    let sorter = ArraysSorter {
        encoded_primary_keys: pk_builder.finish(),
        timestamp_unit,
        timestamp: ts_vector.to_vector().to_arrow_array(),
        sequence: sequence_builder.finish(),
        op_type: op_type_builder.finish(),
        fields: field_builders
            .iter_mut()
            .map(|f| f.to_vector().to_arrow_array()),
        dedup,
        arrow_schema,
    };

    sorter.sort().map(Some)
}

struct ArraysSorter<I> {
    encoded_primary_keys: BinaryArray,
    timestamp_unit: TimeUnit,
    timestamp: ArrayRef,
    sequence: UInt64Array,
    op_type: UInt8Array,
    fields: I,
    dedup: bool,
    arrow_schema: SchemaRef,
}

impl<I> ArraysSorter<I>
where
    I: Iterator<Item = ArrayRef>,
{
    /// Converts arrays to record batch.
    fn sort(self) -> Result<(RecordBatch, i64, i64)> {
        debug_assert!(!self.timestamp.is_empty());
        debug_assert!(self.timestamp.len() == self.sequence.len());
        debug_assert!(self.timestamp.len() == self.op_type.len());
        debug_assert!(self.timestamp.len() == self.encoded_primary_keys.len());

        let timestamp_iter = timestamp_array_to_iter(self.timestamp_unit, &self.timestamp);
        let (mut min_timestamp, mut max_timestamp) = (i64::MAX, i64::MIN);
        let mut to_sort = self
            .encoded_primary_keys
            .iter()
            .zip(timestamp_iter)
            .zip(self.sequence.iter())
            .map(|((pk, timestamp), sequence)| {
                max_timestamp = max_timestamp.max(*timestamp);
                min_timestamp = min_timestamp.min(*timestamp);
                (pk, timestamp, sequence)
            })
            .enumerate()
            .collect::<Vec<_>>();

        to_sort.sort_unstable_by(|(_, (l_pk, l_ts, l_seq)), (_, (r_pk, r_ts, r_seq))| {
            l_pk.cmp(r_pk)
                .then(l_ts.cmp(r_ts))
                .then(l_seq.cmp(r_seq).reverse())
        });

        if self.dedup {
            // Dedup by timestamps while ignore sequence.
            to_sort.dedup_by(|(_, (l_pk, l_ts, _)), (_, (r_pk, r_ts, _))| {
                l_pk == r_pk && l_ts == r_ts
            });
        }

        let indices = UInt32Array::from_iter_values(to_sort.iter().map(|v| v.0 as u32));

        let pk_dictionary = Arc::new(binary_array_to_dictionary(
            // safety: pk must be BinaryArray
            arrow::compute::take(
                &self.encoded_primary_keys,
                &indices,
                Some(TakeOptions {
                    check_bounds: false,
                }),
            )
            .context(ComputeArrowSnafu)?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap(),
        )?) as ArrayRef;

        let mut arrays = Vec::with_capacity(self.arrow_schema.fields.len());
        for arr in self.fields {
            arrays.push(
                arrow::compute::take(
                    &arr,
                    &indices,
                    Some(TakeOptions {
                        check_bounds: false,
                    }),
                )
                .context(ComputeArrowSnafu)?,
            );
        }

        let timestamp = arrow::compute::take(
            &self.timestamp,
            &indices,
            Some(TakeOptions {
                check_bounds: false,
            }),
        )
        .context(ComputeArrowSnafu)?;

        arrays.push(timestamp);
        arrays.push(pk_dictionary);
        arrays.push(
            arrow::compute::take(
                &self.sequence,
                &indices,
                Some(TakeOptions {
                    check_bounds: false,
                }),
            )
            .context(ComputeArrowSnafu)?,
        );

        arrays.push(
            arrow::compute::take(
                &self.op_type,
                &indices,
                Some(TakeOptions {
                    check_bounds: false,
                }),
            )
            .context(ComputeArrowSnafu)?,
        );

        let batch = RecordBatch::try_new(self.arrow_schema, arrays).context(NewRecordBatchSnafu)?;
        Ok((batch, min_timestamp, max_timestamp))
    }
}

/// Converts timestamp array to an iter of i64 values.
fn timestamp_array_to_iter(
    timestamp_unit: TimeUnit,
    timestamp: &ArrayRef,
) -> impl Iterator<Item = &i64> {
    match timestamp_unit {
        // safety: timestamp column must be valid.
        TimeUnit::Second => timestamp
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .values()
            .iter(),
        TimeUnit::Millisecond => timestamp
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values()
            .iter(),
        TimeUnit::Microsecond => timestamp
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .values()
            .iter(),
        TimeUnit::Nanosecond => timestamp
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .values()
            .iter(),
    }
}

/// Converts a **sorted** [BinaryArray] to [DictionaryArray].
fn binary_array_to_dictionary(input: &BinaryArray) -> Result<PrimaryKeyArray> {
    if input.is_empty() {
        return Ok(DictionaryArray::new(
            UInt32Array::from(Vec::<u32>::new()),
            Arc::new(BinaryArray::from_vec(vec![])) as ArrayRef,
        ));
    }
    let mut keys = Vec::with_capacity(16);
    let mut values = BinaryBuilder::new();
    let mut prev: usize = 0;
    keys.push(prev as u32);
    values.append_value(input.value(prev));

    for current_bytes in input.iter().skip(1) {
        // safety: encoded pk must present.
        let current_bytes = current_bytes.unwrap();
        let prev_bytes = input.value(prev);
        if current_bytes != prev_bytes {
            values.append_value(current_bytes);
            prev += 1;
        }
        keys.push(prev as u32);
    }

    Ok(DictionaryArray::new(
        UInt32Array::from(keys),
        Arc::new(values.finish()) as ArrayRef,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use api::v1::{Row, WriteHint};
    use datafusion_common::ScalarValue;
    use datatypes::arrow::array::Float64Array;
    use datatypes::prelude::{ConcreteDataType, ScalarVector, Value};
    use datatypes::vectors::{Float64Vector, TimestampMillisecondVector};
    use store_api::storage::consts::ReservedColumnId;

    use super::*;
    use crate::memtable::bulk::context::BulkIterContext;
    use crate::sst::parquet::format::{PrimaryKeyReadFormat, ReadFormat};
    use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
    use crate::test_util::memtable_util::{
        build_key_values_with_ts_seq_values, metadata_for_test, region_metadata_to_row_schema,
    };

    fn check_binary_array_to_dictionary(
        input: &[&[u8]],
        expected_keys: &[u32],
        expected_values: &[&[u8]],
    ) {
        let input = BinaryArray::from_iter_values(input.iter());
        let array = binary_array_to_dictionary(&input).unwrap();
        assert_eq!(
            &expected_keys,
            &array.keys().iter().map(|v| v.unwrap()).collect::<Vec<_>>()
        );
        assert_eq!(
            expected_values,
            &array
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_binary_array_to_dictionary() {
        check_binary_array_to_dictionary(&[], &[], &[]);

        check_binary_array_to_dictionary(&["a".as_bytes()], &[0], &["a".as_bytes()]);

        check_binary_array_to_dictionary(
            &["a".as_bytes(), "a".as_bytes()],
            &[0, 0],
            &["a".as_bytes()],
        );

        check_binary_array_to_dictionary(
            &["a".as_bytes(), "a".as_bytes(), "b".as_bytes()],
            &[0, 0, 1],
            &["a".as_bytes(), "b".as_bytes()],
        );

        check_binary_array_to_dictionary(
            &[
                "a".as_bytes(),
                "a".as_bytes(),
                "b".as_bytes(),
                "c".as_bytes(),
            ],
            &[0, 0, 1, 2],
            &["a".as_bytes(), "b".as_bytes(), "c".as_bytes()],
        );
    }

    struct MutationInput<'a> {
        k0: &'a str,
        k1: u32,
        timestamps: &'a [i64],
        v1: &'a [Option<f64>],
        sequence: u64,
    }

    #[derive(Debug, PartialOrd, PartialEq)]
    struct BatchOutput<'a> {
        pk_values: &'a [Value],
        timestamps: &'a [i64],
        v1: &'a [Option<f64>],
    }

    fn check_mutations_to_record_batches(
        input: &[MutationInput],
        expected: &[BatchOutput],
        expected_timestamp: (i64, i64),
        dedup: bool,
    ) {
        let metadata = metadata_for_test();
        let mutations = input
            .iter()
            .map(|m| {
                build_key_values_with_ts_seq_values(
                    &metadata,
                    m.k0.to_string(),
                    m.k1,
                    m.timestamps.iter().copied(),
                    m.v1.iter().copied(),
                    m.sequence,
                )
                .mutation
            })
            .collect::<Vec<_>>();
        let total_rows: usize = mutations
            .iter()
            .flat_map(|m| m.rows.iter())
            .map(|r| r.rows.len())
            .sum();

        let pk_encoder = DensePrimaryKeyCodec::new(&metadata);

        let (batch, _, _) = mutations_to_record_batch(&mutations, &metadata, &pk_encoder, dedup)
            .unwrap()
            .unwrap();
        let read_format = PrimaryKeyReadFormat::new_with_all_columns(metadata.clone());
        let mut batches = VecDeque::new();
        read_format
            .convert_record_batch(&batch, None, &mut batches)
            .unwrap();
        if !dedup {
            assert_eq!(
                total_rows,
                batches.iter().map(|b| { b.num_rows() }).sum::<usize>()
            );
        }
        let batch_values = batches
            .into_iter()
            .map(|b| {
                let pk_values = pk_encoder.decode(b.primary_key()).unwrap().into_dense();
                let timestamps = b
                    .timestamps()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondVector>()
                    .unwrap()
                    .iter_data()
                    .map(|v| v.unwrap().0.value())
                    .collect::<Vec<_>>();
                let float_values = b.fields()[1]
                    .data
                    .as_any()
                    .downcast_ref::<Float64Vector>()
                    .unwrap()
                    .iter_data()
                    .collect::<Vec<_>>();

                (pk_values, timestamps, float_values)
            })
            .collect::<Vec<_>>();
        assert_eq!(expected.len(), batch_values.len());

        for idx in 0..expected.len() {
            assert_eq!(expected[idx].pk_values, &batch_values[idx].0);
            assert_eq!(expected[idx].timestamps, &batch_values[idx].1);
            assert_eq!(expected[idx].v1, &batch_values[idx].2);
        }
    }

    #[test]
    fn test_mutations_to_record_batch() {
        check_mutations_to_record_batches(
            &[MutationInput {
                k0: "a",
                k1: 0,
                timestamps: &[0],
                v1: &[Some(0.1)],
                sequence: 0,
            }],
            &[BatchOutput {
                pk_values: &[Value::String("a".into()), Value::UInt32(0)],
                timestamps: &[0],
                v1: &[Some(0.1)],
            }],
            (0, 0),
            true,
        );

        check_mutations_to_record_batches(
            &[
                MutationInput {
                    k0: "a",
                    k1: 0,
                    timestamps: &[0],
                    v1: &[Some(0.1)],
                    sequence: 0,
                },
                MutationInput {
                    k0: "b",
                    k1: 0,
                    timestamps: &[0],
                    v1: &[Some(0.0)],
                    sequence: 0,
                },
                MutationInput {
                    k0: "a",
                    k1: 0,
                    timestamps: &[1],
                    v1: &[Some(0.2)],
                    sequence: 1,
                },
                MutationInput {
                    k0: "a",
                    k1: 1,
                    timestamps: &[1],
                    v1: &[Some(0.3)],
                    sequence: 2,
                },
            ],
            &[
                BatchOutput {
                    pk_values: &[Value::String("a".into()), Value::UInt32(0)],
                    timestamps: &[0, 1],
                    v1: &[Some(0.1), Some(0.2)],
                },
                BatchOutput {
                    pk_values: &[Value::String("a".into()), Value::UInt32(1)],
                    timestamps: &[1],
                    v1: &[Some(0.3)],
                },
                BatchOutput {
                    pk_values: &[Value::String("b".into()), Value::UInt32(0)],
                    timestamps: &[0],
                    v1: &[Some(0.0)],
                },
            ],
            (0, 1),
            true,
        );

        check_mutations_to_record_batches(
            &[
                MutationInput {
                    k0: "a",
                    k1: 0,
                    timestamps: &[0],
                    v1: &[Some(0.1)],
                    sequence: 0,
                },
                MutationInput {
                    k0: "b",
                    k1: 0,
                    timestamps: &[0],
                    v1: &[Some(0.0)],
                    sequence: 0,
                },
                MutationInput {
                    k0: "a",
                    k1: 0,
                    timestamps: &[0],
                    v1: &[Some(0.2)],
                    sequence: 1,
                },
            ],
            &[
                BatchOutput {
                    pk_values: &[Value::String("a".into()), Value::UInt32(0)],
                    timestamps: &[0],
                    v1: &[Some(0.2)],
                },
                BatchOutput {
                    pk_values: &[Value::String("b".into()), Value::UInt32(0)],
                    timestamps: &[0],
                    v1: &[Some(0.0)],
                },
            ],
            (0, 0),
            true,
        );
        check_mutations_to_record_batches(
            &[
                MutationInput {
                    k0: "a",
                    k1: 0,
                    timestamps: &[0],
                    v1: &[Some(0.1)],
                    sequence: 0,
                },
                MutationInput {
                    k0: "b",
                    k1: 0,
                    timestamps: &[0],
                    v1: &[Some(0.0)],
                    sequence: 0,
                },
                MutationInput {
                    k0: "a",
                    k1: 0,
                    timestamps: &[0],
                    v1: &[Some(0.2)],
                    sequence: 1,
                },
            ],
            &[
                BatchOutput {
                    pk_values: &[Value::String("a".into()), Value::UInt32(0)],
                    timestamps: &[0, 0],
                    v1: &[Some(0.2), Some(0.1)],
                },
                BatchOutput {
                    pk_values: &[Value::String("b".into()), Value::UInt32(0)],
                    timestamps: &[0],
                    v1: &[Some(0.0)],
                },
            ],
            (0, 0),
            false,
        );
    }

    fn encode(input: &[MutationInput]) -> EncodedBulkPart {
        let metadata = metadata_for_test();
        let kvs = input
            .iter()
            .map(|m| {
                build_key_values_with_ts_seq_values(
                    &metadata,
                    m.k0.to_string(),
                    m.k1,
                    m.timestamps.iter().copied(),
                    m.v1.iter().copied(),
                    m.sequence,
                )
            })
            .collect::<Vec<_>>();
        let schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
        let primary_key_codec = build_primary_key_codec(&metadata);
        let mut converter = BulkPartConverter::new(&metadata, schema, 64, primary_key_codec, true);
        for kv in kvs {
            converter.append_key_values(&kv).unwrap();
        }
        let part = converter.convert().unwrap();
        let encoder = BulkPartEncoder::new(metadata, 1024).unwrap();
        encoder.encode_part(&part).unwrap().unwrap()
    }

    #[test]
    fn test_write_and_read_part_projection() {
        let part = encode(&[
            MutationInput {
                k0: "a",
                k1: 0,
                timestamps: &[1],
                v1: &[Some(0.1)],
                sequence: 0,
            },
            MutationInput {
                k0: "b",
                k1: 0,
                timestamps: &[1],
                v1: &[Some(0.0)],
                sequence: 0,
            },
            MutationInput {
                k0: "a",
                k1: 0,
                timestamps: &[2],
                v1: &[Some(0.2)],
                sequence: 1,
            },
        ]);

        let projection = &[4u32];
        let mut reader = part
            .read(
                Arc::new(BulkIterContext::new(
                    part.metadata.region_metadata.clone(),
                    &Some(projection.as_slice()),
                    None,
                )),
                None,
            )
            .unwrap()
            .expect("expect at least one row group");

        let mut total_rows_read = 0;
        let mut field: Vec<f64> = vec![];
        for res in reader {
            let batch = res.unwrap();
            assert_eq!(5, batch.num_columns());
            field.extend_from_slice(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values(),
            );
            total_rows_read += batch.num_rows();
        }
        assert_eq!(3, total_rows_read);
        assert_eq!(vec![0.1, 0.2, 0.0], field);
    }

    fn prepare(key_values: Vec<(&str, u32, (i64, i64), u64)>) -> EncodedBulkPart {
        let metadata = metadata_for_test();
        let kvs = key_values
            .into_iter()
            .map(|(k0, k1, (start, end), sequence)| {
                let ts = (start..end);
                let v1 = (start..end).map(|_| None);
                build_key_values_with_ts_seq_values(&metadata, k0.to_string(), k1, ts, v1, sequence)
            })
            .collect::<Vec<_>>();
        let schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
        let primary_key_codec = build_primary_key_codec(&metadata);
        let mut converter = BulkPartConverter::new(&metadata, schema, 64, primary_key_codec, true);
        for kv in kvs {
            converter.append_key_values(&kv).unwrap();
        }
        let part = converter.convert().unwrap();
        let encoder = BulkPartEncoder::new(metadata, 1024).unwrap();
        encoder.encode_part(&part).unwrap().unwrap()
    }

    fn check_prune_row_group(
        part: &EncodedBulkPart,
        predicate: Option<Predicate>,
        expected_rows: usize,
    ) {
        let context = Arc::new(BulkIterContext::new(
            part.metadata.region_metadata.clone(),
            &None,
            predicate,
        ));
        let mut reader = part
            .read(context, None)
            .unwrap()
            .expect("expect at least one row group");
        let mut total_rows_read = 0;
        for res in reader {
            let batch = res.unwrap();
            total_rows_read += batch.num_rows();
        }
        // Should only read row group 1.
        assert_eq!(expected_rows, total_rows_read);
    }

    #[test]
    fn test_prune_row_groups() {
        let part = prepare(vec![
            ("a", 0, (0, 40), 1),
            ("a", 1, (0, 60), 1),
            ("b", 0, (0, 100), 2),
            ("b", 1, (100, 180), 3),
            ("b", 1, (180, 210), 4),
        ]);

        let context = Arc::new(BulkIterContext::new(
            part.metadata.region_metadata.clone(),
            &None,
            Some(Predicate::new(vec![datafusion_expr::col("ts").eq(
                datafusion_expr::lit(ScalarValue::TimestampMillisecond(Some(300), None)),
            )])),
        ));
        assert!(part.read(context, None).unwrap().is_none());

        check_prune_row_group(&part, None, 310);

        check_prune_row_group(
            &part,
            Some(Predicate::new(vec![
                datafusion_expr::col("k0").eq(datafusion_expr::lit("a")),
                datafusion_expr::col("k1").eq(datafusion_expr::lit(0u32)),
            ])),
            40,
        );

        check_prune_row_group(
            &part,
            Some(Predicate::new(vec![
                datafusion_expr::col("k0").eq(datafusion_expr::lit("a")),
                datafusion_expr::col("k1").eq(datafusion_expr::lit(1u32)),
            ])),
            60,
        );

        check_prune_row_group(
            &part,
            Some(Predicate::new(vec![
                datafusion_expr::col("k0").eq(datafusion_expr::lit("a")),
            ])),
            100,
        );

        check_prune_row_group(
            &part,
            Some(Predicate::new(vec![
                datafusion_expr::col("k0").eq(datafusion_expr::lit("b")),
                datafusion_expr::col("k1").eq(datafusion_expr::lit(0u32)),
            ])),
            100,
        );

        /// Predicates over field column can do precise filtering.
        check_prune_row_group(
            &part,
            Some(Predicate::new(vec![
                datafusion_expr::col("v0").eq(datafusion_expr::lit(150i64)),
            ])),
            1,
        );
    }

    #[test]
    fn test_bulk_part_converter_append_and_convert() {
        let metadata = metadata_for_test();
        let capacity = 100;
        let primary_key_codec = build_primary_key_codec(&metadata);
        let schema = to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        );

        let mut converter =
            BulkPartConverter::new(&metadata, schema, capacity, primary_key_codec, true);

        let key_values1 = build_key_values_with_ts_seq_values(
            &metadata,
            "key1".to_string(),
            1u32,
            vec![1000, 2000].into_iter(),
            vec![Some(1.0), Some(2.0)].into_iter(),
            1,
        );

        let key_values2 = build_key_values_with_ts_seq_values(
            &metadata,
            "key2".to_string(),
            2u32,
            vec![1500].into_iter(),
            vec![Some(3.0)].into_iter(),
            2,
        );

        converter.append_key_values(&key_values1).unwrap();
        converter.append_key_values(&key_values2).unwrap();

        let bulk_part = converter.convert().unwrap();

        assert_eq!(bulk_part.num_rows(), 3);
        assert_eq!(bulk_part.min_timestamp, 1000);
        assert_eq!(bulk_part.max_timestamp, 2000);
        assert_eq!(bulk_part.sequence, 2);
        assert_eq!(bulk_part.timestamp_index, bulk_part.batch.num_columns() - 4);

        // Validate primary key columns are stored
        // Schema should include primary key columns k0 and k1 at the beginning
        let schema = bulk_part.batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec![
                "k0",
                "k1",
                "v0",
                "v1",
                "ts",
                "__primary_key",
                "__sequence",
                "__op_type"
            ]
        );
    }

    #[test]
    fn test_bulk_part_converter_sorting() {
        let metadata = metadata_for_test();
        let capacity = 100;
        let primary_key_codec = build_primary_key_codec(&metadata);
        let schema = to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        );

        let mut converter =
            BulkPartConverter::new(&metadata, schema, capacity, primary_key_codec, true);

        let key_values1 = build_key_values_with_ts_seq_values(
            &metadata,
            "z_key".to_string(),
            3u32,
            vec![3000].into_iter(),
            vec![Some(3.0)].into_iter(),
            3,
        );

        let key_values2 = build_key_values_with_ts_seq_values(
            &metadata,
            "a_key".to_string(),
            1u32,
            vec![1000].into_iter(),
            vec![Some(1.0)].into_iter(),
            1,
        );

        let key_values3 = build_key_values_with_ts_seq_values(
            &metadata,
            "m_key".to_string(),
            2u32,
            vec![2000].into_iter(),
            vec![Some(2.0)].into_iter(),
            2,
        );

        converter.append_key_values(&key_values1).unwrap();
        converter.append_key_values(&key_values2).unwrap();
        converter.append_key_values(&key_values3).unwrap();

        let bulk_part = converter.convert().unwrap();

        assert_eq!(bulk_part.num_rows(), 3);

        let ts_column = bulk_part.batch.column(bulk_part.timestamp_index);
        let seq_column = bulk_part.batch.column(bulk_part.batch.num_columns() - 2);

        let ts_array = ts_column
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let seq_array = seq_column.as_any().downcast_ref::<UInt64Array>().unwrap();

        assert_eq!(ts_array.values(), &[1000, 2000, 3000]);
        assert_eq!(seq_array.values(), &[1, 2, 3]);

        // Validate primary key columns are stored
        let schema = bulk_part.batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec![
                "k0",
                "k1",
                "v0",
                "v1",
                "ts",
                "__primary_key",
                "__sequence",
                "__op_type"
            ]
        );
    }

    #[test]
    fn test_bulk_part_converter_empty() {
        let metadata = metadata_for_test();
        let capacity = 10;
        let primary_key_codec = build_primary_key_codec(&metadata);
        let schema = to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        );

        let converter =
            BulkPartConverter::new(&metadata, schema, capacity, primary_key_codec, true);

        let bulk_part = converter.convert().unwrap();

        assert_eq!(bulk_part.num_rows(), 0);
        assert_eq!(bulk_part.min_timestamp, i64::MAX);
        assert_eq!(bulk_part.max_timestamp, i64::MIN);
        assert_eq!(bulk_part.sequence, SequenceNumber::MIN);

        // Validate primary key columns are present in schema even for empty batch
        let schema = bulk_part.batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec![
                "k0",
                "k1",
                "v0",
                "v1",
                "ts",
                "__primary_key",
                "__sequence",
                "__op_type"
            ]
        );
    }

    #[test]
    fn test_bulk_part_converter_without_primary_key_columns() {
        let metadata = metadata_for_test();
        let primary_key_codec = build_primary_key_codec(&metadata);
        let schema = to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions {
                raw_pk_columns: false,
                string_pk_use_dict: true,
            },
        );

        let capacity = 100;
        let mut converter =
            BulkPartConverter::new(&metadata, schema, capacity, primary_key_codec, false);

        let key_values1 = build_key_values_with_ts_seq_values(
            &metadata,
            "key1".to_string(),
            1u32,
            vec![1000, 2000].into_iter(),
            vec![Some(1.0), Some(2.0)].into_iter(),
            1,
        );

        let key_values2 = build_key_values_with_ts_seq_values(
            &metadata,
            "key2".to_string(),
            2u32,
            vec![1500].into_iter(),
            vec![Some(3.0)].into_iter(),
            2,
        );

        converter.append_key_values(&key_values1).unwrap();
        converter.append_key_values(&key_values2).unwrap();

        let bulk_part = converter.convert().unwrap();

        assert_eq!(bulk_part.num_rows(), 3);
        assert_eq!(bulk_part.min_timestamp, 1000);
        assert_eq!(bulk_part.max_timestamp, 2000);
        assert_eq!(bulk_part.sequence, 2);
        assert_eq!(bulk_part.timestamp_index, bulk_part.batch.num_columns() - 4);

        // Validate primary key columns are NOT stored individually
        let schema = bulk_part.batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec!["v0", "v1", "ts", "__primary_key", "__sequence", "__op_type"]
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn build_key_values_with_sparse_encoding(
        metadata: &RegionMetadataRef,
        primary_key_codec: &Arc<dyn PrimaryKeyCodec>,
        table_id: u32,
        tsid: u64,
        k0: String,
        k1: String,
        timestamps: impl Iterator<Item = i64>,
        values: impl Iterator<Item = Option<f64>>,
        sequence: SequenceNumber,
    ) -> KeyValues {
        // Encode the primary key (__table_id, __tsid, k0, k1) into binary format using the sparse codec
        let pk_values = vec![
            (ReservedColumnId::table_id(), Value::UInt32(table_id)),
            (ReservedColumnId::tsid(), Value::UInt64(tsid)),
            (0, Value::String(k0.clone().into())),
            (1, Value::String(k1.clone().into())),
        ];
        let mut encoded_key = Vec::new();
        primary_key_codec
            .encode_values(&pk_values, &mut encoded_key)
            .unwrap();
        assert!(!encoded_key.is_empty());

        // Create schema for sparse encoding: __primary_key, ts, v0, v1
        let column_schema = vec![
            api::v1::ColumnSchema {
                column_name: PRIMARY_KEY_COLUMN_NAME.to_string(),
                datatype: api::helper::ColumnDataTypeWrapper::try_from(
                    ConcreteDataType::binary_datatype(),
                )
                .unwrap()
                .datatype() as i32,
                semantic_type: api::v1::SemanticType::Tag as i32,
                ..Default::default()
            },
            api::v1::ColumnSchema {
                column_name: "ts".to_string(),
                datatype: api::helper::ColumnDataTypeWrapper::try_from(
                    ConcreteDataType::timestamp_millisecond_datatype(),
                )
                .unwrap()
                .datatype() as i32,
                semantic_type: api::v1::SemanticType::Timestamp as i32,
                ..Default::default()
            },
            api::v1::ColumnSchema {
                column_name: "v0".to_string(),
                datatype: api::helper::ColumnDataTypeWrapper::try_from(
                    ConcreteDataType::int64_datatype(),
                )
                .unwrap()
                .datatype() as i32,
                semantic_type: api::v1::SemanticType::Field as i32,
                ..Default::default()
            },
            api::v1::ColumnSchema {
                column_name: "v1".to_string(),
                datatype: api::helper::ColumnDataTypeWrapper::try_from(
                    ConcreteDataType::float64_datatype(),
                )
                .unwrap()
                .datatype() as i32,
                semantic_type: api::v1::SemanticType::Field as i32,
                ..Default::default()
            },
        ];

        let rows = timestamps
            .zip(values)
            .map(|(ts, v)| Row {
                values: vec![
                    api::v1::Value {
                        value_data: Some(api::v1::value::ValueData::BinaryValue(
                            encoded_key.clone(),
                        )),
                    },
                    api::v1::Value {
                        value_data: Some(api::v1::value::ValueData::TimestampMillisecondValue(ts)),
                    },
                    api::v1::Value {
                        value_data: Some(api::v1::value::ValueData::I64Value(ts)),
                    },
                    api::v1::Value {
                        value_data: v.map(api::v1::value::ValueData::F64Value),
                    },
                ],
            })
            .collect();

        let mutation = api::v1::Mutation {
            op_type: 1,
            sequence,
            rows: Some(api::v1::Rows {
                schema: column_schema,
                rows,
            }),
            write_hint: Some(WriteHint {
                primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
            }),
        };
        KeyValues::new(metadata.as_ref(), mutation).unwrap()
    }

    #[test]
    fn test_bulk_part_converter_sparse_primary_key_encoding() {
        use api::v1::SemanticType;
        use datatypes::schema::ColumnSchema;
        use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
        use store_api::storage::RegionId;

        let mut builder = RegionMetadataBuilder::new(RegionId::new(123, 456));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k1", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::int64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v1", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 4,
            })
            .primary_key(vec![0, 1])
            .primary_key_encoding(PrimaryKeyEncoding::Sparse);
        let metadata = Arc::new(builder.build().unwrap());

        let primary_key_codec = build_primary_key_codec(&metadata);
        let schema = to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        );

        assert_eq!(metadata.primary_key_encoding, PrimaryKeyEncoding::Sparse);
        assert_eq!(primary_key_codec.encoding(), PrimaryKeyEncoding::Sparse);

        let capacity = 100;
        let mut converter =
            BulkPartConverter::new(&metadata, schema, capacity, primary_key_codec.clone(), true);

        let key_values1 = build_key_values_with_sparse_encoding(
            &metadata,
            &primary_key_codec,
            2048u32, // table_id
            100u64,  // tsid
            "key11".to_string(),
            "key21".to_string(),
            vec![1000, 2000].into_iter(),
            vec![Some(1.0), Some(2.0)].into_iter(),
            1,
        );

        let key_values2 = build_key_values_with_sparse_encoding(
            &metadata,
            &primary_key_codec,
            4096u32, // table_id
            200u64,  // tsid
            "key12".to_string(),
            "key22".to_string(),
            vec![1500].into_iter(),
            vec![Some(3.0)].into_iter(),
            2,
        );

        converter.append_key_values(&key_values1).unwrap();
        converter.append_key_values(&key_values2).unwrap();

        let bulk_part = converter.convert().unwrap();

        assert_eq!(bulk_part.num_rows(), 3);
        assert_eq!(bulk_part.min_timestamp, 1000);
        assert_eq!(bulk_part.max_timestamp, 2000);
        assert_eq!(bulk_part.sequence, 2);
        assert_eq!(bulk_part.timestamp_index, bulk_part.batch.num_columns() - 4);

        // For sparse encoding, primary key columns should NOT be stored individually
        // even when store_primary_key_columns is true, because sparse encoding
        // stores the encoded primary key in the __primary_key column
        let schema = bulk_part.batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec!["v0", "v1", "ts", "__primary_key", "__sequence", "__op_type"]
        );

        // Verify the __primary_key column contains encoded sparse keys
        let primary_key_column = bulk_part.batch.column_by_name("__primary_key").unwrap();
        let dict_array = primary_key_column
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .unwrap();

        // Should have non-zero entries indicating encoded primary keys
        assert!(!dict_array.is_empty());
        assert_eq!(dict_array.len(), 3); // 3 rows total

        // Verify values are properly encoded binary data (not empty)
        let values = dict_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for i in 0..values.len() {
            assert!(
                !values.value(i).is_empty(),
                "Encoded primary key should not be empty"
            );
        }
    }
}
