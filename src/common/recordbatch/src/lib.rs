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

#![feature(never_type)]

pub mod adapter;
pub mod cursor;
pub mod error;
pub mod filter;
mod recordbatch;
pub mod util;

use std::pin::Pin;
use std::sync::Arc;

use adapter::RecordBatchMetrics;
use arc_swap::ArcSwapOption;
pub use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::arrow::compute::SortOptions;
pub use datatypes::arrow::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow::util::pretty;
use datatypes::prelude::{ConcreteDataType, VectorRef};
use datatypes::scalars::{ScalarVector, ScalarVectorBuilder};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::types::{JsonFormat, jsonb_to_string};
use datatypes::vectors::{BinaryVector, StringVectorBuilder};
use error::Result;
use futures::task::{Context, Poll};
use futures::{Stream, TryStreamExt};
pub use recordbatch::RecordBatch;
use snafu::{OptionExt, ResultExt, ensure};

pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn name(&self) -> &str {
        "RecordBatchStream"
    }

    fn schema(&self) -> SchemaRef;

    fn output_ordering(&self) -> Option<&[OrderOption]>;

    fn metrics(&self) -> Option<RecordBatchMetrics>;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderOption {
    pub name: String,
    pub options: SortOptions,
}

/// A wrapper that maps a [RecordBatchStream] to a new [RecordBatchStream] by applying a function to each [RecordBatch].
///
/// The mapper function is applied to each [RecordBatch] in the stream.
/// The schema of the new [RecordBatchStream] is the same as the schema of the inner [RecordBatchStream] after applying the schema mapper function.
/// The output ordering of the new [RecordBatchStream] is the same as the output ordering of the inner [RecordBatchStream].
/// The metrics of the new [RecordBatchStream] is the same as the metrics of the inner [RecordBatchStream] if it is not `None`.
pub struct SendableRecordBatchMapper {
    inner: SendableRecordBatchStream,
    /// The mapper function is applied to each [RecordBatch] in the stream.
    /// The original schema and the mapped schema are passed to the mapper function.
    mapper: fn(RecordBatch, &SchemaRef, &SchemaRef) -> Result<RecordBatch>,
    /// The schema of the new [RecordBatchStream] is the same as the schema of the inner [RecordBatchStream] after applying the schema mapper function.
    schema: SchemaRef,
    /// Whether the mapper function is applied to each [RecordBatch] in the stream.
    apply_mapper: bool,
}

/// Maps the json type to string in the batch.
///
/// The json type is mapped to string by converting the json value to string.
/// The batch is updated to have the same number of columns as the original batch,
/// but with the json type mapped to string.
pub fn map_json_type_to_string(
    batch: RecordBatch,
    original_schema: &SchemaRef,
    mapped_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut vectors = Vec::with_capacity(original_schema.column_schemas().len());
    for (vector, schema) in batch.columns.iter().zip(original_schema.column_schemas()) {
        if let ConcreteDataType::Json(j) = &schema.data_type {
            if matches!(j.format(), JsonFormat::Jsonb) {
                let mut string_vector_builder = StringVectorBuilder::with_capacity(vector.len());
                let binary_vector = vector
                    .as_any()
                    .downcast_ref::<BinaryVector>()
                    .with_context(|| error::DowncastVectorSnafu {
                        from_type: schema.data_type.clone(),
                        to_type: ConcreteDataType::binary_datatype(),
                    })?;
                for value in binary_vector.iter_data() {
                    let Some(value) = value else {
                        string_vector_builder.push(None);
                        continue;
                    };
                    let string_value =
                        jsonb_to_string(value).with_context(|_| error::CastVectorSnafu {
                            from_type: schema.data_type.clone(),
                            to_type: ConcreteDataType::string_datatype(),
                        })?;
                    string_vector_builder.push(Some(string_value.as_str()));
                }

                let string_vector = string_vector_builder.finish();
                vectors.push(Arc::new(string_vector) as VectorRef);
            } else {
                vectors.push(vector.clone());
            }
        } else {
            vectors.push(vector.clone());
        }
    }

    RecordBatch::new(mapped_schema.clone(), vectors)
}

/// Maps the json type to string in the schema.
///
/// The json type is mapped to string by converting the json value to string.
/// The schema is updated to have the same number of columns as the original schema,
/// but with the json type mapped to string.
///
/// Returns the new schema and whether the schema needs to be mapped to string.
pub fn map_json_type_to_string_schema(schema: SchemaRef) -> (SchemaRef, bool) {
    let mut new_columns = Vec::with_capacity(schema.column_schemas().len());
    let mut apply_mapper = false;
    for column in schema.column_schemas() {
        if matches!(column.data_type, ConcreteDataType::Json(_)) {
            new_columns.push(ColumnSchema::new(
                column.name.clone(),
                ConcreteDataType::string_datatype(),
                column.is_nullable(),
            ));
            apply_mapper = true;
        } else {
            new_columns.push(column.clone());
        }
    }
    (Arc::new(Schema::new(new_columns)), apply_mapper)
}

impl SendableRecordBatchMapper {
    /// Creates a new [SendableRecordBatchMapper] with the given inner [RecordBatchStream], mapper function, and schema mapper function.
    pub fn new(
        inner: SendableRecordBatchStream,
        mapper: fn(RecordBatch, &SchemaRef, &SchemaRef) -> Result<RecordBatch>,
        schema_mapper: fn(SchemaRef) -> (SchemaRef, bool),
    ) -> Self {
        let (mapped_schema, apply_mapper) = schema_mapper(inner.schema());
        Self {
            inner,
            mapper,
            schema: mapped_schema,
            apply_mapper,
        }
    }
}

impl RecordBatchStream for SendableRecordBatchMapper {
    fn name(&self) -> &str {
        "SendableRecordBatchMapper"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.inner.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.inner.metrics()
    }
}

impl Stream for SendableRecordBatchMapper {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.apply_mapper {
            Pin::new(&mut self.inner).poll_next(cx).map(|opt| {
                opt.map(|result| {
                    result
                        .and_then(|batch| (self.mapper)(batch, &self.inner.schema(), &self.schema))
                })
            })
        } else {
            Pin::new(&mut self.inner).poll_next(cx)
        }
    }
}

/// EmptyRecordBatchStream can be used to create a RecordBatchStream
/// that will produce no results
pub struct EmptyRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    /// Create an empty RecordBatchStream
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

#[derive(Debug, PartialEq)]
pub struct RecordBatches {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl RecordBatches {
    pub fn try_from_columns<I: IntoIterator<Item = VectorRef>>(
        schema: SchemaRef,
        columns: I,
    ) -> Result<Self> {
        let batches = vec![RecordBatch::new(schema.clone(), columns)?];
        Ok(Self { schema, batches })
    }

    pub async fn try_collect(stream: SendableRecordBatchStream) -> Result<Self> {
        let schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(Self { schema, batches })
    }

    #[inline]
    pub fn empty() -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![])),
            batches: vec![],
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &RecordBatch> {
        self.batches.iter()
    }

    pub fn pretty_print(&self) -> Result<String> {
        let df_batches = &self
            .iter()
            .map(|x| x.df_record_batch().clone())
            .collect::<Vec<_>>();
        let result = pretty::pretty_format_batches(df_batches).context(error::FormatSnafu)?;

        Ok(result.to_string())
    }

    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        for batch in &batches {
            ensure!(
                batch.schema == schema,
                error::CreateRecordBatchesSnafu {
                    reason: format!(
                        "expect RecordBatch schema equals {:?}, actual: {:?}",
                        schema, batch.schema
                    )
                }
            )
        }
        Ok(Self { schema, batches })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn take(self) -> Vec<RecordBatch> {
        self.batches
    }

    pub fn as_stream(&self) -> SendableRecordBatchStream {
        Box::pin(SimpleRecordBatchStream {
            inner: RecordBatches {
                schema: self.schema(),
                batches: self.batches.clone(),
            },
            index: 0,
        })
    }
}

impl IntoIterator for RecordBatches {
    type Item = RecordBatch;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.batches.into_iter()
    }
}

pub struct SimpleRecordBatchStream {
    inner: RecordBatches,
    index: usize,
}

impl RecordBatchStream for SimpleRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

impl Stream for SimpleRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.inner.batches.len() {
            let batch = self.inner.batches[self.index].clone();
            self.index += 1;
            Some(Ok(batch))
        } else {
            None
        })
    }
}

/// Adapt a [Stream] of [RecordBatch] to a [RecordBatchStream].
pub struct RecordBatchStreamWrapper<S> {
    pub schema: SchemaRef,
    pub stream: S,
    pub output_ordering: Option<Vec<OrderOption>>,
    pub metrics: Arc<ArcSwapOption<RecordBatchMetrics>>,
}

impl<S> RecordBatchStreamWrapper<S> {
    /// Creates a [RecordBatchStreamWrapper] without output ordering requirement.
    pub fn new(schema: SchemaRef, stream: S) -> RecordBatchStreamWrapper<S> {
        RecordBatchStreamWrapper {
            schema,
            stream,
            output_ordering: None,
            metrics: Default::default(),
        }
    }
}

impl<S: Stream<Item = Result<RecordBatch>> + Unpin> RecordBatchStream
    for RecordBatchStreamWrapper<S>
{
    fn name(&self) -> &str {
        "RecordBatchStreamWrapper"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.output_ordering.as_deref()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.metrics.load().as_ref().map(|s| s.as_ref().clone())
    }
}

impl<S: Stream<Item = Result<RecordBatch>> + Unpin> Stream for RecordBatchStreamWrapper<S> {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{BooleanVector, Int32Vector, StringVector};

    use super::*;

    #[test]
    fn test_recordbatches_try_from_columns() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let result = RecordBatches::try_from_columns(
            schema.clone(),
            vec![Arc::new(StringVector::from(vec!["hello", "world"])) as _],
        );
        assert!(result.is_err());

        let v: VectorRef = Arc::new(Int32Vector::from_slice([1, 2]));
        let expected = vec![RecordBatch::new(schema.clone(), vec![v.clone()]).unwrap()];
        let r = RecordBatches::try_from_columns(schema, vec![v]).unwrap();
        assert_eq!(r.take(), expected);
    }

    #[test]
    fn test_recordbatches_try_new() {
        let column_a = ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false);
        let column_b = ColumnSchema::new("b", ConcreteDataType::string_datatype(), false);
        let column_c = ColumnSchema::new("c", ConcreteDataType::boolean_datatype(), false);

        let va: VectorRef = Arc::new(Int32Vector::from_slice([1, 2]));
        let vb: VectorRef = Arc::new(StringVector::from(vec!["hello", "world"]));
        let vc: VectorRef = Arc::new(BooleanVector::from(vec![true, false]));

        let schema1 = Arc::new(Schema::new(vec![column_a.clone(), column_b]));
        let batch1 = RecordBatch::new(schema1.clone(), vec![va.clone(), vb]).unwrap();

        let schema2 = Arc::new(Schema::new(vec![column_a, column_c]));
        let batch2 = RecordBatch::new(schema2.clone(), vec![va, vc]).unwrap();

        let result = RecordBatches::try_new(schema1.clone(), vec![batch1.clone(), batch2]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Failed to create RecordBatches, reason: expect RecordBatch schema equals {schema1:?}, actual: {schema2:?}",
            )
        );

        let batches = RecordBatches::try_new(schema1.clone(), vec![batch1.clone()]).unwrap();
        let expected = "\
+---+-------+
| a | b     |
+---+-------+
| 1 | hello |
| 2 | world |
+---+-------+";
        assert_eq!(batches.pretty_print().unwrap(), expected);

        assert_eq!(schema1, batches.schema());
        assert_eq!(vec![batch1], batches.take());
    }

    #[tokio::test]
    async fn test_simple_recordbatch_stream() {
        let column_a = ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false);
        let column_b = ColumnSchema::new("b", ConcreteDataType::string_datatype(), false);
        let schema = Arc::new(Schema::new(vec![column_a, column_b]));

        let va1: VectorRef = Arc::new(Int32Vector::from_slice([1, 2]));
        let vb1: VectorRef = Arc::new(StringVector::from(vec!["a", "b"]));
        let batch1 = RecordBatch::new(schema.clone(), vec![va1, vb1]).unwrap();

        let va2: VectorRef = Arc::new(Int32Vector::from_slice([3, 4, 5]));
        let vb2: VectorRef = Arc::new(StringVector::from(vec!["c", "d", "e"]));
        let batch2 = RecordBatch::new(schema.clone(), vec![va2, vb2]).unwrap();

        let recordbatches =
            RecordBatches::try_new(schema.clone(), vec![batch1.clone(), batch2.clone()]).unwrap();
        let stream = recordbatches.as_stream();
        let collected = util::collect(stream).await.unwrap();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], batch1);
        assert_eq!(collected[1], batch2);
    }
}
