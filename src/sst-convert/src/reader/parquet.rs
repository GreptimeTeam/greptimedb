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

//! Parquet file format support.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;

use api::v1::OpType;
use common_error::ext::BoxedError;
use common_error::snafu::{OptionExt, ResultExt};
use common_recordbatch::error::UnsupportedOperationSnafu;
use common_recordbatch::RecordBatch;
use datatypes::prelude::DataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::Schema;
use datatypes::value::Value;
use datatypes::vectors::{MutableVector, UInt64VectorBuilder, UInt8VectorBuilder};
use futures_util::StreamExt;
use mito2::error::ReadParquetSnafu;
use mito2::read::{Batch, BatchColumn, BatchReader};
use mito2::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodec, SparsePrimaryKeyCodec};
use object_store::ObjectStore;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet_opendal::AsyncReader;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{MitoSnafu, ObjectStoreSnafu, Result};

pub struct OpenDALParquetReader {
    inner: RawParquetReader<AsyncReader>,
}

impl OpenDALParquetReader {
    pub async fn new(
        operator: ObjectStore,
        path: &str,
        metadata: RegionMetadataRef,
        override_sequence: Option<SequenceNumber>,
    ) -> Result<Self> {
        let reader = operator.reader_with(path).await.context(ObjectStoreSnafu)?;

        let content_len = operator
            .stat(path)
            .await
            .context(ObjectStoreSnafu)?
            .content_length();

        let reader = AsyncReader::new(reader, content_len).with_prefetch_footer_size(512 * 1024);
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .context(ReadParquetSnafu { path })
            .context(MitoSnafu)?
            .build()
            .context(ReadParquetSnafu { path })
            .context(MitoSnafu)?;
        Ok(Self {
            inner: RawParquetReader::new(stream, metadata, override_sequence, path),
        })
    }
}

#[async_trait::async_trait]
impl BatchReader for OpenDALParquetReader {
    async fn next_batch(&mut self) -> mito2::error::Result<Option<Batch>> {
        self.inner.next_batch().await
    }
}

pub struct RawParquetReader<T> {
    metadata: RegionMetadataRef,
    override_sequence: Option<SequenceNumber>,
    output_batch_queue: VecDeque<Batch>,
    stream: Pin<Box<ParquetRecordBatchStream<T>>>,
    path: String,
}

impl<T: AsyncFileReader + Unpin + Send + 'static> RawParquetReader<T> {
    pub fn new(
        stream: ParquetRecordBatchStream<T>,
        metadata: RegionMetadataRef,
        override_sequence: Option<SequenceNumber>,
        path: &str,
    ) -> Self {
        Self {
            stream: Box::pin(stream),
            metadata,
            override_sequence,
            output_batch_queue: VecDeque::new(),
            path: path.to_string(),
        }
    }

    pub async fn next_batch_inner(&mut self) -> mito2::error::Result<Option<Batch>> {
        if let Some(batch) = self.output_batch_queue.pop_front() {
            return Ok(Some(batch));
        }
        let Some(next_input_rb) = self.stream.next().await.transpose().with_context(|_| {
            mito2::error::ReadParquetSnafu {
                path: self.path.clone(),
            }
        })?
        else {
            return Ok(None);
        };

        let schema = Arc::new(
            Schema::try_from(next_input_rb.schema())
                .map_err(BoxedError::new)
                .with_context(|_| mito2::error::ExternalSnafu {
                    context: format!(
                        "Failed to convert Schema from DfSchema: {:?}",
                        next_input_rb.schema()
                    ),
                })?,
        );
        let rb = RecordBatch::try_from_df_record_batch(schema, next_input_rb)
            .map_err(BoxedError::new)
            .with_context(|_| mito2::error::ExternalSnafu {
                context: "Failed to convert RecordBatch from DfRecordBatch".to_string(),
            })?;
        let new_batches = extract_to_batches(&rb, &self.metadata, self.override_sequence)
            .map_err(BoxedError::new)
            .with_context(|_| mito2::error::ExternalSnafu {
                context: format!("Failed to extract batches from RecordBatch: {:?}", rb),
            })?;

        self.output_batch_queue.extend(new_batches);
        Ok(self.output_batch_queue.pop_front())
    }
}

#[async_trait::async_trait]
impl<T: AsyncFileReader + Unpin + Send + 'static> BatchReader for RawParquetReader<T> {
    async fn next_batch(&mut self) -> mito2::error::Result<Option<Batch>> {
        self.next_batch_inner().await
    }
}

pub fn extract_to_batches(
    rb: &RecordBatch,
    metadata: &RegionMetadataRef,
    override_sequence: Option<SequenceNumber>,
) -> Result<Vec<Batch>, BoxedError> {
    let pk_codec: Box<dyn PrimaryKeyCodec> = match metadata.primary_key_encoding {
        PrimaryKeyEncoding::Dense => Box::new(DensePrimaryKeyCodec::new(metadata)),
        PrimaryKeyEncoding::Sparse => Box::new(SparsePrimaryKeyCodec::new(metadata)),
    };
    let pk_ids = metadata.primary_key.clone();
    let pk_names: Vec<_> = pk_ids
        .iter()
        .map(|id| {
            metadata
                .column_by_id(*id)
                .expect("Can't find column by id")
                .column_schema
                .name
                .clone()
        })
        .collect();
    let pk_pos_in_rb: Vec<_> = pk_names
        .into_iter()
        .map(|name| {
            rb.schema
                .column_index_by_name(&name)
                .context(UnsupportedOperationSnafu {
                    reason: format!("Can't find column {} in rb={:?}", name, rb),
                })
                .map_err(BoxedError::new)
        })
        .collect::<Result<_, _>>()?;

    let mut pk_to_batchs: HashMap<Vec<u8>, SSTBatchBuilder> = HashMap::new();
    let mut buffer = Vec::new();

    for row in rb.rows() {
        let pk_values: Vec<_> = pk_ids
            .iter()
            .zip(pk_pos_in_rb.iter())
            .map(|(id, pos)| (*id, row[*pos].clone()))
            .collect();
        pk_codec
            .encode_values(&pk_values, &mut buffer)
            .map_err(BoxedError::new)?;
        let cur_pk = &buffer;
        let builder = if let Some(builder) = pk_to_batchs.get_mut(cur_pk) {
            builder
        } else {
            let builder =
                SSTBatchBuilder::new(rb, metadata, override_sequence).map_err(BoxedError::new)?;
            pk_to_batchs.insert(cur_pk.clone(), builder);
            pk_to_batchs.get_mut(cur_pk).expect("Just inserted")
        };
        builder.push_row(&row).map_err(BoxedError::new)?;
    }

    // sort batches by primary key
    let mut batches = BTreeMap::new();
    for (pk, builder) in pk_to_batchs {
        batches.insert(pk.clone(), builder.finish(pk).map_err(BoxedError::new)?);
    }
    let batches = batches.into_values().collect();
    Ok(batches)
}

struct SSTBatchBuilder {
    /// for extract field column from record batch's row
    field_column_pos: Vec<usize>,
    field_ids: Vec<ColumnId>,
    field_builders: Vec<Box<dyn MutableVector>>,
    timestamp_pos: usize,
    timestamp_builder: Box<dyn MutableVector>,
    /// override sequence number
    override_sequence: Option<SequenceNumber>,
    sequence_builder: UInt64VectorBuilder,
    op_type_builder: UInt8VectorBuilder,
    cur_seq: SequenceNumber,
}

impl SSTBatchBuilder {
    fn finish(mut self, pk: Vec<u8>) -> Result<Batch, BoxedError> {
        let fields: Vec<_> = self
            .field_ids
            .iter()
            .zip(self.field_builders)
            .map(|(id, mut b)| BatchColumn {
                column_id: *id,
                data: b.to_vector(),
            })
            .collect();
        Batch::new(
            pk,
            self.timestamp_builder.to_vector(),
            Arc::new(self.sequence_builder.finish()),
            Arc::new(self.op_type_builder.finish()),
            fields,
        )
        .map_err(BoxedError::new)
    }

    fn push_row(&mut self, row: &[Value]) -> Result<(), BoxedError> {
        for (field_pos, field_builder) in self
            .field_column_pos
            .iter()
            .zip(self.field_builders.iter_mut())
        {
            field_builder.push_value_ref(row[*field_pos].as_value_ref());
        }
        self.timestamp_builder
            .push_value_ref(row[self.timestamp_pos].as_value_ref());
        self.sequence_builder
            .push(Some(self.override_sequence.unwrap_or(self.cur_seq)));
        self.op_type_builder.push(Some(OpType::Put as u8));
        self.cur_seq += 1;
        Ok(())
    }

    fn new(
        rb: &RecordBatch,
        metadata: &RegionMetadataRef,
        override_sequence: Option<SequenceNumber>,
    ) -> Result<Self, BoxedError> {
        let timeindex_name = &metadata.time_index_column().column_schema.name;
        Ok(Self {
            field_ids: metadata.field_columns().map(|c| c.column_id).collect(),
            field_column_pos: metadata
                .field_columns()
                .map(|c| &c.column_schema.name)
                .map(|name| {
                    rb.schema
                        .column_index_by_name(name)
                        .context(UnsupportedOperationSnafu {
                            reason: format!("Can't find column {} in rb={:?}", name, rb),
                        })
                        .map_err(BoxedError::new)
                })
                .collect::<Result<_, _>>()?,
            field_builders: metadata
                .field_columns()
                .map(|c| c.column_schema.data_type.create_mutable_vector(512))
                .collect(),

            timestamp_pos: rb
                .schema
                .column_index_by_name(timeindex_name)
                .context(UnsupportedOperationSnafu {
                    reason: format!("{} in rb={:?}", timeindex_name, rb),
                })
                .map_err(BoxedError::new)?,
            timestamp_builder: metadata
                .time_index_column()
                .column_schema
                .data_type
                .create_mutable_vector(512),

            override_sequence,
            sequence_builder: UInt64VectorBuilder::with_capacity(512),

            op_type_builder: UInt8VectorBuilder::with_capacity(512),
            cur_seq: override_sequence.unwrap_or_default(),
        })
    }
}
