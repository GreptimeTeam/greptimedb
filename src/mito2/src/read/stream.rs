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

//! Record batch stream.

use std::sync::Arc;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::RecordBatchStream;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch};
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::schema::{SchemaRef, Schema};
use datatypes::value::ValueRef;
use datatypes::vectors::VectorRef;
use futures::Stream;
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;
use crate::read::Batch;
use crate::row_converter::{McmpRowCodec, SortField, RowCodec};
use crate::error::Result;

/// Record batch stream implementation.
pub(crate) struct StreamImpl<S> {
    /// [Batch] stream.
    stream: S,
    /// Converts [Batch]es from the `stream` to [RecordBatch].
    converter: BatchConverter,
}

impl<S> StreamImpl<S> {
    /// Returns a new stream from a batch stream.
    pub(crate) fn new(stream: S, converter: BatchConverter) -> StreamImpl<S> {
        StreamImpl {
            stream,
            converter,
        }
    }
}

impl<S: Stream<Item = Result<Batch>> + Unpin> Stream for StreamImpl<S> {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(res)) => {
                let record_batch = res.map_err(BoxedError::new).context(ExternalSnafu).and_then(|batch| {
                    self.converter.convert(&batch)
                });
                Poll::Ready(Some(record_batch))
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: Stream<Item = Result<Batch>> + Unpin> RecordBatchStream for StreamImpl<S> {
    fn schema(&self) -> SchemaRef {
        self.converter.output_schema.clone()
    }
}

/// Converts a [Batch] to a [RecordBatch].
pub(crate) struct BatchConverter {
    /// Maps column in [RecordBatch] to index in [Batch].
    batch_indices: Vec<BatchIndex>,
    /// Decoder for primary key.
    codec: McmpRowCodec,
    /// Schema for converted [RecordBatch].
    output_schema: SchemaRef,
}

impl BatchConverter {
    /// Returns a new converter with projection.
    ///
    /// # Panics
    /// Panics if any index in `projection` is out of bound.
    pub(crate) fn new(metadata: &RegionMetadata, projection: impl Iterator<Item = usize>) -> BatchConverter {
        let mut batch_indices = Vec::with_capacity(projection.size_hint().0);
        let mut column_schemas = Vec::with_capacity(projection.size_hint().0);
        for idx in projection {
            // For each projection index, we get the column id for projection.
            let column = &metadata.column_metadatas[idx];

            // Get column index in a batch by its semantic type and column id.
            let batch_index = match column.semantic_type {
                SemanticType::Tag => {
                    // Safety: It is a primary key column.
                    let index = metadata.primary_key_index(column.column_id).unwrap();
                    BatchIndex::Tag(index)
                },
                SemanticType::Timestamp => BatchIndex::Timestamp,
                SemanticType::Field => {
                    // Safety: It is a field column.
                    let index = metadata.field_index(column.column_id).unwrap();
                    BatchIndex::Field(index)
                }
            };
            batch_indices.push(batch_index);

            column_schemas.push(metadata.schema.column_schemas()[idx].clone());
        }

        let codec = McmpRowCodec::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );
        // Safety: Columns come from existing schema.
        let output_schema = Arc::new(Schema::new(column_schemas));

        BatchConverter {
            batch_indices,
            codec,
            output_schema,
        }
    }

    /// Returns a new converter without projection.
    pub(crate) fn all(metadata: &RegionMetadata) -> BatchConverter {
        BatchConverter::new(metadata, 0..metadata.column_metadatas.len())
    }

    /// Converts a [Batch] to a [RecordBatch].
    ///
    /// The batch must match the `projection` using to build the converter.
    pub(crate) fn convert(&self, batch: &Batch) -> common_recordbatch::error::Result<RecordBatch> {
        let pk_values = self.codec.decode(batch.primary_key()).map_err(BoxedError::new).context(ExternalSnafu)?;

        let mut columns = Vec::with_capacity(self.output_schema.num_columns());
        let num_rows = batch.num_rows();
        for (index, column_schema) in self.batch_indices.iter().zip(self.output_schema.column_schemas()) {
            match index {
                BatchIndex::Tag(idx) => {
                    let value = pk_values[*idx].as_value_ref();
                    let vector = new_repeated_vector(&column_schema.data_type, value, num_rows)?;
                    columns.push(vector);
                },
                BatchIndex::Timestamp => {
                    columns.push(batch.timestamps().clone());
                },
                BatchIndex::Field(idx) => {
                    columns.push(batch.fields()[*idx].data.clone());
                },
            }
        }

        RecordBatch::new(self.output_schema.clone(), columns)
    }
}

/// Index of a vector in a [Batch].
#[derive(Debug, Clone, Copy)]
enum BatchIndex {
    /// Index in primary keys.
    Tag(usize),
    /// The time index column.
    Timestamp,
    /// Index in fields.
    Field(usize),
}

/// Returns a vector with repeated values.
fn new_repeated_vector(data_type: &ConcreteDataType, value: ValueRef, num_rows: usize) -> common_recordbatch::error::Result<VectorRef> {
    let mut mutable_vector = data_type.create_mutable_vector(1);
    mutable_vector.try_push_value_ref(value).map_err(BoxedError::new).context(ExternalSnafu)?;
    // This requires an addtional allocation. TODO(yingwen): Add a way to create repeated vector to data type.
    let base_vector = mutable_vector.to_vector();
    Ok(base_vector.replicate(&[num_rows]))
}
