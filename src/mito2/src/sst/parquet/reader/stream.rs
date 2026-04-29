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

use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion_common::cast_column;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datatypes::arrow::array::{ArrayRef, new_null_array};
use datatypes::arrow::datatypes::{DataType, FieldRef, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use futures::Stream;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use snafu::{IntoError, ResultExt, ensure};

use crate::error::{
    CastColumnSnafu, NewRecordBatchSnafu, ReadParquetSnafu, Result, UnexpectedSnafu,
};
use crate::sst::parquet::async_reader::SstAsyncFileReader;

/// Wraps a parquet record batch stream and fills missing projected root columns.
///
/// Nested projection may ask parquet to read leaves under a root column. If none
/// of the requested leaves exists in the current parquet file, parquet decoding
/// omits the whole root from the physical `RecordBatch`. The logical projection
/// still contains that root, so this wrapper restores the output shape by
/// inserting a root-level null array.
pub struct MissingColFiller<S> {
    /// Inner stream that yields record batches from parquet reader.
    inner: S,
    /// Output schema expected by the upper reader.
    output_schema: SchemaRef,
    /// Whether each projected root exists in the physical batch returned by parquet.
    projected_root_matches: Vec<bool>,
    /// Number of columns expected from the physical batch returned by parquet.
    expected_input_col_num: usize,
    /// Whether all projected roots are present and the stream can pass batches through.
    all_matched: bool,
}

pub(crate) type ProjectedRecordBatchStream =
    NestedSchemaAligner<MissingColFiller<ParquetErrorAdapter>>;

impl<S> MissingColFiller<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    pub fn new(
        inner: S,
        projected_root_matches: Vec<bool>,
        output_schema: SchemaRef,
    ) -> Result<MissingColFiller<S>> {
        ensure!(
            projected_root_matches.len() == output_schema.fields().len(),
            UnexpectedSnafu {
                reason: format!(
                    "MissingColFiller projected root matches len {} does not match output schema columns {}",
                    projected_root_matches.len(),
                    output_schema.fields().len()
                ),
            }
        );

        let expected_input_col_num = projected_root_matches
            .iter()
            .filter(|matched| **matched)
            .count();
        let all_matched = projected_root_matches.iter().all(|&m| m);

        Ok(MissingColFiller {
            inner,
            output_schema,
            projected_root_matches,
            expected_input_col_num,
            all_matched,
        })
    }
}

impl<S> Stream for MissingColFiller<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(rb))) => {
                let output_schema = &this.output_schema;
                let rb = if this.all_matched {
                    rb
                } else {
                    fill_missing_cols(
                        rb,
                        output_schema,
                        &this.projected_root_matches,
                        this.expected_input_col_num,
                    )?
                };
                Poll::Ready(Some(Ok(rb)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn fill_missing_cols(
    rb: RecordBatch,
    output_schema: &SchemaRef,
    projected_root_matches: &[bool],
    expected_input_col_num: usize,
) -> Result<RecordBatch> {
    ensure!(
        rb.columns().len() == expected_input_col_num,
        UnexpectedSnafu {
            reason: format!(
                "MissingColFiller expected {} input columns but got {}",
                expected_input_col_num,
                rb.columns().len()
            ),
        }
    );

    let mut cols = Vec::with_capacity(projected_root_matches.len());
    let mut idx = 0;

    for (field, matched) in output_schema.fields().iter().zip(projected_root_matches) {
        if *matched {
            cols.push(rb.column(idx).clone());
            idx += 1;
        } else {
            cols.push(new_null_array(field.data_type(), rb.num_rows()));
        }
    }

    RecordBatch::try_new(output_schema.clone(), cols).context(NewRecordBatchSnafu)
}

/// Aligns nested arrays in projected batches to the expected output schema.
///
/// Parquet nested projection can return a root column whose nested fields are a
/// subset of the expected logical schema. This wrapper normalizes those arrays
/// so all projected batches share the same schema before entering upper readers.
pub struct NestedSchemaAligner<S> {
    inner: S,
    output_schema: SchemaRef,
}

impl<S> NestedSchemaAligner<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    pub fn new(inner: S, output_schema: SchemaRef) -> Self {
        Self {
            inner,
            output_schema,
        }
    }
}

impl<S> Stream for NestedSchemaAligner<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(rb))) => {
                Poll::Ready(Some(align_batch_to_schema(rb, &this.output_schema)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn align_batch_to_schema(rb: RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    ensure!(
        rb.num_columns() == output_schema.fields().len(),
        UnexpectedSnafu {
            reason: format!(
                "NestedSchemaAligner expected {} columns but got {}",
                output_schema.fields().len(),
                rb.num_columns()
            ),
        }
    );

    let columns = rb
        .columns()
        .iter()
        .zip(output_schema.fields())
        .map(|(array, field)| align_array_to_field(array, field))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(output_schema.clone(), columns).context(NewRecordBatchSnafu)
}

fn align_array_to_field(array: &ArrayRef, field: &FieldRef) -> Result<ArrayRef> {
    if array.data_type() == field.data_type() {
        return Ok(array.clone());
    }

    if !matches!(field.data_type(), DataType::Struct(_)) {
        return Ok(array.clone());
    }

    cast_column(array, field.as_ref(), &DEFAULT_CAST_OPTIONS).context(CastColumnSnafu)
}

/// Maps parquet stream errors into mito errors before batches enter the filler.
pub(crate) struct ParquetErrorAdapter {
    inner: ParquetRecordBatchStream<SstAsyncFileReader>,
    path: String,
}

impl ParquetErrorAdapter {
    pub(crate) fn new(inner: ParquetRecordBatchStream<SstAsyncFileReader>, path: String) -> Self {
        Self { inner, path }
    }
}

impl Stream for ParquetErrorAdapter {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(rb))) => Poll::Ready(Some(Ok(rb))),
            Poll::Ready(Some(Err(err))) => {
                Poll::Ready(Some(Err(
                    ReadParquetSnafu { path: &this.path }.into_error(err)
                )))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
    use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema};
    use futures::{StreamExt, stream};

    use super::*;

    #[tokio::test]
    async fn test_filler_with_all_projected_roots_match() {
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let input = RecordBatch::try_new(
            output_schema.clone(),
            vec![int_array([1, 2, 3]), string_array(["x", "y", "z"])],
        )
        .unwrap();
        let stream = stream::iter([Ok(input.clone())]);

        let mut filler =
            MissingColFiller::new(stream, vec![true, true], output_schema.clone()).unwrap();
        let output = filler.next().await.unwrap().unwrap();

        assert_eq!(input, output);
        assert!(filler.next().await.is_none());
    }

    #[tokio::test]
    async fn test_filler_with_fills_null_root_columns() {
        let input_schema = schema([Field::new("a", DataType::Int64, true)]);
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("missing", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]);
        let input = RecordBatch::try_new(input_schema, vec![int_array([10, 20])]).unwrap();
        let stream = stream::iter([Ok(input)]);

        let mut filler =
            MissingColFiller::new(stream, vec![true, false, false], output_schema.clone()).unwrap();
        let output = filler.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        assert_eq!(3, output.num_columns());
        assert_eq!(
            &[Some(10), Some(20)],
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
        assert_eq!(DataType::Utf8, *output.column(1).data_type());
        assert_eq!(output.num_rows(), output.column(1).null_count());
        assert_eq!(DataType::Int64, *output.column(2).data_type());
        assert_eq!(output.num_rows(), output.column(2).null_count());
    }

    #[tokio::test]
    async fn test_filler_with_fills_missing_struct_root_column() {
        let input_schema = schema([Field::new("a", DataType::Int64, true)]);
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Utf8, true),
        ]));
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("missing_struct", struct_type.clone(), true),
        ]);
        let input = RecordBatch::try_new(input_schema, vec![int_array([10, 20])]).unwrap();
        let stream = stream::iter([Ok(input)]);

        let mut filler =
            MissingColFiller::new(stream, vec![true, false], output_schema.clone()).unwrap();
        let output = filler.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        assert_eq!(2, output.num_columns());
        assert_eq!(struct_type, output.column(1).data_type().clone());
        assert_eq!(output.num_rows(), output.column(1).null_count());
    }

    #[tokio::test]
    async fn test_filler_with_reject_projection_len_mismatch() {
        let output_schema = schema([Field::new("a", DataType::Int64, true)]);
        let stream = stream::iter([]);

        let err = match MissingColFiller::new(stream, vec![true, false], output_schema) {
            Ok(_) => panic!("MissingColFiller should reject projection length mismatch"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("projected root matches len 2"));
    }

    #[tokio::test]
    async fn test_filler_reject_with_input_column_mismatch() {
        let input_schema = schema([Field::new("a", DataType::Int64, true)]);
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("missing", DataType::Int64, true),
        ]);
        let input = RecordBatch::try_new(input_schema, vec![int_array([1, 2])]).unwrap();
        let stream = stream::iter([Ok(input)]);

        let mut filler =
            MissingColFiller::new(stream, vec![true, true, false], output_schema).unwrap();
        let err = filler.next().await.unwrap().unwrap_err();

        assert!(
            err.to_string()
                .contains("expected 2 input columns but got 1")
        );
    }

    #[tokio::test]
    async fn test_nested_schema_aligner_aligns_struct_field() {
        let output_schema = schema([Field::new(
            "nested",
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int64, true),
                Field::new("y", DataType::Utf8, true),
            ])),
            true,
        )]);
        let input = RecordBatch::try_new(
            schema([Field::new(
                "nested",
                DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
                true,
            )]),
            vec![Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("x", DataType::Int64, true)),
                int_array([1, 2]),
            )]))],
        )
        .unwrap();

        let mut aligner =
            NestedSchemaAligner::new(stream::iter([Ok(input)]), output_schema.clone());
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        let nested = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(2, nested.columns().len());
        assert_eq!(2, nested.column(1).null_count());
    }

    fn schema(fields: impl IntoIterator<Item = Field>) -> SchemaRef {
        Arc::new(Schema::new(fields.into_iter().collect::<Vec<_>>()))
    }

    fn int_array(values: impl IntoIterator<Item = i64>) -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(values))
    }

    fn string_array(values: impl IntoIterator<Item = &'static str>) -> ArrayRef {
        Arc::new(StringArray::from_iter_values(values))
    }
}
