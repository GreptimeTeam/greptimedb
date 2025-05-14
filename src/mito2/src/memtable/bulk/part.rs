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

use api::helper::{value_to_grpc_value, ColumnDataTypeWrapper};
use api::v1::{Mutation, OpType};
use bytes::Bytes;
use common_recordbatch::DfRecordBatch as RecordBatch;
use common_time::timestamp::TimeUnit;
use datafusion::arrow::array::{TimestampNanosecondArray, UInt64Builder};
use datatypes::arrow;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryBuilder, DictionaryArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampSecondArray, UInt32Array, UInt64Array, UInt8Array,
    UInt8Builder,
};
use datatypes::arrow::compute::TakeOptions;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow_array::BinaryArray;
use datatypes::data_type::DataType;
use datatypes::prelude::{MutableVector, ScalarVectorBuilder, Vector};
use datatypes::value::Value;
use datatypes::vectors::Helper;
use parquet::arrow::ArrowWriter;
use parquet::data_type::AsBytes;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use snafu::{OptionExt, ResultExt, Snafu};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::SequenceNumber;
use table::predicate::Predicate;

use crate::error;
use crate::error::{ComputeArrowSnafu, EncodeMemtableSnafu, NewRecordBatchSnafu, Result};
use crate::memtable::bulk::context::BulkIterContextRef;
use crate::memtable::bulk::part_reader::BulkPartIter;
use crate::memtable::key_values::{KeyValue, KeyValuesRef};
use crate::memtable::BoxedBatchIterator;
use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodec, PrimaryKeyCodecExt};
use crate::sst::parquet::format::{PrimaryKeyArray, ReadFormat};
use crate::sst::parquet::helper::parse_parquet_metadata;
use crate::sst::to_sst_arrow_schema;

#[derive(Clone)]
pub struct BulkPart {
    pub batch: RecordBatch,
    pub num_rows: usize,
    pub max_ts: i64,
    pub min_ts: i64,
    pub sequence: u64,
    pub timestamp_index: usize,
}

impl BulkPart {
    pub(crate) fn estimated_size(&self) -> usize {
        self.batch.get_array_memory_size()
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

        let rows = (0..self.num_rows)
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
}

#[derive(Debug)]
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

    pub(crate) fn read(
        &self,
        context: BulkIterContextRef,
        sequence: Option<SequenceNumber>,
    ) -> Result<Option<BoxedBatchIterator>> {
        // use predicate to find row groups to read.
        let row_groups_to_read = context.row_groups_to_read(&self.metadata.parquet_metadata);

        if row_groups_to_read.is_empty() {
            // All row groups are filtered.
            return Ok(None);
        }

        let iter = BulkPartIter::try_new(
            context,
            row_groups_to_read,
            self.metadata.parquet_metadata.clone(),
            self.data.clone(),
            sequence,
        )?;
        Ok(Some(Box::new(iter) as BoxedBatchIterator))
    }
}

#[derive(Debug)]
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

pub struct BulkPartEncoder {
    metadata: RegionMetadataRef,
    pk_encoder: DensePrimaryKeyCodec,
    row_group_size: usize,
    dedup: bool,
    writer_props: Option<WriterProperties>,
}

impl BulkPartEncoder {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        dedup: bool,
        row_group_size: usize,
    ) -> BulkPartEncoder {
        let codec = DensePrimaryKeyCodec::new(&metadata);
        let writer_props = Some(
            WriterProperties::builder()
                .set_write_batch_size(row_group_size)
                .set_max_row_group_size(row_group_size)
                .build(),
        );
        Self {
            metadata,
            pk_encoder: codec,
            row_group_size,
            dedup,
            writer_props,
        }
    }
}

impl BulkPartEncoder {
    /// Encodes mutations to a [EncodedBulkPart], returns true if encoded data has been written to `dest`.
    fn encode_mutations(&self, mutations: &[Mutation]) -> Result<Option<EncodedBulkPart>> {
        let Some((arrow_record_batch, min_ts, max_ts)) =
            mutations_to_record_batch(mutations, &self.metadata, &self.pk_encoder, self.dedup)?
        else {
            return Ok(None);
        };

        let mut buf = Vec::with_capacity(4096);
        let arrow_schema = arrow_record_batch.schema();

        let file_metadata = {
            let mut writer =
                ArrowWriter::try_new(&mut buf, arrow_schema, self.writer_props.clone())
                    .context(EncodeMemtableSnafu)?;
            writer
                .write(&arrow_record_batch)
                .context(EncodeMemtableSnafu)?;
            writer.finish().context(EncodeMemtableSnafu)?
        };

        let buf = Bytes::from(buf);
        let parquet_metadata = Arc::new(parse_parquet_metadata(file_metadata)?);

        Ok(Some(EncodedBulkPart {
            data: buf,
            metadata: BulkPartMeta {
                num_rows: arrow_record_batch.num_rows(),
                max_timestamp: max_ts,
                min_timestamp: min_ts,
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
            pk_encoder.encode_to_vec(row.primary_keys(), &mut pk_buffer)?;
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

    use datafusion_common::ScalarValue;
    use datatypes::prelude::{ScalarVector, Value};
    use datatypes::vectors::{Float64Vector, TimestampMillisecondVector};

    use super::*;
    use crate::memtable::bulk::context::BulkIterContext;
    use crate::sst::parquet::format::ReadFormat;
    use crate::test_util::memtable_util::{build_key_values_with_ts_seq_values, metadata_for_test};

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
        let read_format = ReadFormat::new_with_all_columns(metadata.clone());
        let mut batches = VecDeque::new();
        read_format
            .convert_record_batch(&batch, &mut batches)
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
        let encoder = BulkPartEncoder::new(metadata, true, 1024);
        encoder.encode_mutations(&mutations).unwrap().unwrap()
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
        let mut field = vec![];
        for res in reader {
            let batch = res.unwrap();
            assert_eq!(1, batch.fields().len());
            assert_eq!(4, batch.fields()[0].column_id);
            field.extend(
                batch.fields()[0]
                    .data
                    .as_any()
                    .downcast_ref::<Float64Vector>()
                    .unwrap()
                    .iter_data()
                    .map(|v| v.unwrap()),
            );
            total_rows_read += batch.num_rows();
        }
        assert_eq!(3, total_rows_read);
        assert_eq!(vec![0.1, 0.2, 0.0], field);
    }

    fn prepare(key_values: Vec<(&str, u32, (i64, i64), u64)>) -> EncodedBulkPart {
        let metadata = metadata_for_test();
        let mutations = key_values
            .into_iter()
            .map(|(k0, k1, (start, end), sequence)| {
                let ts = (start..end);
                let v1 = (start..end).map(|_| None);
                build_key_values_with_ts_seq_values(&metadata, k0.to_string(), k1, ts, v1, sequence)
                    .mutation
            })
            .collect::<Vec<_>>();
        let encoder = BulkPartEncoder::new(metadata, true, 100);
        encoder.encode_mutations(&mutations).unwrap().unwrap()
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
                datafusion_expr::col("k0").eq(datafusion_expr::lit("a"))
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
                datafusion_expr::col("v0").eq(datafusion_expr::lit(150i64))
            ])),
            1,
        );
    }
}
