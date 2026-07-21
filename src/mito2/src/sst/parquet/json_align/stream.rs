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
use datatypes::extension::json::is_structured_json_field;
use datatypes::vectors::json::array::JsonArray;
use futures::Stream;
use snafu::{ResultExt, ensure};

use crate::error::{
    CastColumnSnafu, DataTypeMismatchSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::sst::parquet::json_align::fallback::apply_json2_fallbacks_to_root;
use crate::sst::parquet::read_columns::{Json2FallbackItem, Json2FallbackPlan};

/// Aligns projected batches to the expected output schema for nested projections.
///
/// Background
/// ----------
/// Nested projection may ask parquet to read leaves under a root column. If none
/// of the requested leaves exists in the current parquet file, parquet decoding
/// omits the whole root from the physical [`RecordBatch`].
///
/// In addition, after nested-path filtering, returned struct arrays may contain
/// only a subset of fields. The current output schema is not pruned by nested
/// paths, so physical struct fields can be a subset of the expected struct
/// fields, and their nested schema can differ from the expected output schema.
///
/// To keep projected batches schema-consistent before entering upper readers:
/// - Root-column presence alignment restores missing projected root columns by
///   inserting root-level null arrays.
/// - Nested struct alignment aligns struct arrays to the expected nested field
///   layout.
#[derive(derive_more::Debug)]
pub struct NestedSchemaAligner<S> {
    #[debug(skip)]
    inner: S,
    /// Output schema expected by the upper reader.
    output_schema: SchemaRef,
    /// Whether each projected root exists in the physical batch returned by
    /// parquet.
    projected_root_presence: Vec<bool>,
    /// Number of columns expected from the physical batch returned by parquet.
    expected_input_col_num: usize,
    /// JSON2 fallback items grouped by output root index.
    fallbacks_by_output_root: Vec<Vec<Json2FallbackItem>>,
    /// Whether all projected roots are present and the stream can pass batches
    /// through.
    all_roots_present: bool,
    /// The cache for whether incoming batches already match output schema.
    is_schema_matched: Option<bool>,
}

impl<S> NestedSchemaAligner<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    pub fn new(
        inner: S,
        projected_root_presence: Vec<bool>,
        output_schema: SchemaRef,
        json2_fallback_plan: Json2FallbackPlan,
    ) -> Result<NestedSchemaAligner<S>> {
        ensure!(
            projected_root_presence.len() == output_schema.fields().len(),
            UnexpectedSnafu {
                reason: format!(
                    "NestedSchemaAligner projected root presence len {} does not match output schema columns {}",
                    projected_root_presence.len(),
                    output_schema.fields().len()
                ),
            }
        );

        let expected_input_col_num = projected_root_presence
            .iter()
            .filter(|matched| **matched)
            .count();
        let all_roots_present = projected_root_presence.iter().all(|&m| m);
        let mut fallbacks_by_output_root = vec![Vec::new(); output_schema.fields().len()];
        for item in json2_fallback_plan.items {
            ensure!(
                item.output_root_index < fallbacks_by_output_root.len(),
                UnexpectedSnafu {
                    reason: format!(
                        "NestedSchemaAligner fallback output root index {} out of bounds {}",
                        item.output_root_index,
                        fallbacks_by_output_root.len()
                    ),
                }
            );
            fallbacks_by_output_root[item.output_root_index].push(item);
        }

        Ok(NestedSchemaAligner {
            inner,
            output_schema,
            projected_root_presence,
            expected_input_col_num,
            fallbacks_by_output_root,
            all_roots_present,
            is_schema_matched: None,
        })
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
                let has_json2_fallbacks =
                    this.fallbacks_by_output_root.iter().any(|x| !x.is_empty());
                let is_schema_matched = !has_json2_fallbacks
                    && this.all_roots_present
                    && *this
                        .is_schema_matched
                        .get_or_insert_with(|| rb.schema() == this.output_schema);

                if is_schema_matched {
                    Poll::Ready(Some(Ok(rb)))
                } else {
                    Poll::Ready(Some(align_projected_batch(
                        rb,
                        &this.output_schema,
                        &this.projected_root_presence,
                        this.expected_input_col_num,
                        &this.fallbacks_by_output_root,
                    )))
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn align_projected_batch(
    rb: RecordBatch,
    output_schema: &SchemaRef,
    projected_root_presence: &[bool],
    expected_input_col_num: usize,
    fallbacks_by_output_root: &[Vec<Json2FallbackItem>],
) -> Result<RecordBatch> {
    ensure!(
        rb.columns().len() == expected_input_col_num,
        UnexpectedSnafu {
            reason: format!(
                "NestedSchemaAligner expected {} input columns but got {}",
                expected_input_col_num,
                rb.columns().len()
            ),
        }
    );

    let mut cols = Vec::with_capacity(projected_root_presence.len());
    let mut idx = 0;

    for (output_root_index, (field, present)) in output_schema
        .fields()
        .iter()
        .zip(projected_root_presence)
        .enumerate()
    {
        if !present {
            cols.push(new_null_array(field.data_type(), rb.num_rows()));
            continue;
        }

        let fallbacks = &fallbacks_by_output_root[output_root_index];
        let column = if fallbacks.is_empty() {
            rb.column(idx).clone()
        } else {
            apply_json2_fallbacks_to_root(rb.column(idx), field, fallbacks)?
        };
        cols.push(align_array(&column, field)?);
        idx += 1;
    }

    RecordBatch::try_new(output_schema.clone(), cols).context(NewRecordBatchSnafu)
}

fn align_array(array: &ArrayRef, field: &FieldRef) -> Result<ArrayRef> {
    if array.data_type() == field.data_type() {
        return Ok(array.clone());
    }

    if is_structured_json_field(field) {
        return JsonArray::from(array)
            .try_align(field.data_type())
            .context(DataTypeMismatchSnafu);
    }

    if !matches!(field.data_type(), DataType::Struct(_)) {
        return Ok(array.clone());
    }

    cast_column(array, field.as_ref(), &DEFAULT_CAST_OPTIONS).context(CastColumnSnafu)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{
        Array, ArrayRef, BinaryArray, Int64Array, StringArray, StringViewArray, StructArray,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datatypes::extension::json::{JsonExtensionType, JsonMetadata};
    use datatypes::types::parse_string_to_jsonb;
    use futures::{StreamExt, stream};

    use super::*;

    #[tokio::test]
    async fn test_aligner_with_all_projected_roots_match() {
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

        let mut aligner = NestedSchemaAligner::new(
            stream,
            vec![true, true],
            output_schema.clone(),
            Json2FallbackPlan::default(),
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(input, output);
        assert!(aligner.next().await.is_none());
    }

    #[tokio::test]
    async fn test_aligner_with_fills_null_root_columns() {
        let input_schema = schema([Field::new("a", DataType::Int64, true)]);
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("missing", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]);
        let input = RecordBatch::try_new(input_schema, vec![int_array([10, 20])]).unwrap();
        let stream = stream::iter([Ok(input)]);

        let mut aligner = NestedSchemaAligner::new(
            stream,
            vec![true, false, false],
            output_schema.clone(),
            Json2FallbackPlan::default(),
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

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
    async fn test_aligner_with_fills_missing_struct_root_column() {
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

        let mut aligner = NestedSchemaAligner::new(
            stream,
            vec![true, false],
            output_schema.clone(),
            Json2FallbackPlan::default(),
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        assert_eq!(2, output.num_columns());
        assert_eq!(struct_type, output.column(1).data_type().clone());
        assert_eq!(output.num_rows(), output.column(1).null_count());
    }

    #[tokio::test]
    async fn test_aligner_reject_projection_len_mismatch() {
        let output_schema = schema([Field::new("a", DataType::Int64, true)]);
        let stream = stream::iter([]);

        let err = match NestedSchemaAligner::new(
            stream,
            vec![true, false],
            output_schema,
            Json2FallbackPlan::default(),
        ) {
            Ok(_) => panic!("NestedSchemaAligner should reject projection length mismatch"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("projected root presence len 2 does not match output schema columns 1")
        );
    }

    #[tokio::test]
    async fn test_aligner_reject_with_input_column_mismatch() {
        let input_schema = schema([Field::new("a", DataType::Int64, true)]);
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("missing", DataType::Int64, true),
        ]);
        let input = RecordBatch::try_new(input_schema, vec![int_array([1, 2])]).unwrap();
        let stream = stream::iter([Ok(input)]);

        let mut aligner = NestedSchemaAligner::new(
            stream,
            vec![true, true, false],
            output_schema,
            Json2FallbackPlan::default(),
        )
        .unwrap();
        let err = aligner.next().await.unwrap().unwrap_err();

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

        let mut aligner = NestedSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            Json2FallbackPlan::default(),
        )
        .unwrap();
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

    #[tokio::test]
    async fn test_nested_schema_aligner_applies_json2_fallback() {
        let source_values = [
            Some(parse_string_to_jsonb("1").unwrap()),
            Some(parse_string_to_jsonb(r#"{"b":2}"#).unwrap()),
            None,
        ];
        let source = Arc::new(BinaryArray::from_iter(
            source_values.iter().map(|value| value.as_deref()),
        )) as ArrayRef;
        let input_fields = Fields::from(vec![Arc::new(Field::new("a", DataType::Binary, true))]);
        let input = RecordBatch::try_new(
            schema([Field::new(
                "j",
                DataType::Struct(input_fields.clone()),
                true,
            )]),
            vec![Arc::new(StructArray::new(input_fields, vec![source], None))],
        )
        .unwrap();

        let output_schema = schema([Field::new(
            "j",
            DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                "a",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("b", DataType::UInt64, true)),
                    Arc::new(Field::new("c", DataType::Utf8View, true)),
                ])),
                true,
            ))])),
            true,
        )
        .with_extension_type(JsonExtensionType::new(Arc::new(JsonMetadata::default())))]);
        let plan = Json2FallbackPlan {
            items: vec![Json2FallbackItem {
                output_root_index: 0,
                source_path: vec!["j".to_string(), "a".to_string()],
                requested_paths: vec![
                    vec!["j".to_string(), "a".to_string(), "b".to_string()],
                    vec!["j".to_string(), "a".to_string(), "c".to_string()],
                ],
            }],
        };

        let mut aligner = NestedSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            plan,
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        let j = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a = j.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            &[None, Some(2), None],
            a.column(0)
                .as_any()
                .downcast_ref::<datatypes::arrow::array::UInt64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
        assert_eq!(
            &[None, None, None],
            a.column(1)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
    }

    #[tokio::test]
    async fn test_nested_schema_aligner_preserves_json2_fallback_siblings() {
        let source_values = [
            Some(parse_string_to_jsonb(r#"{"x":1}"#).unwrap()),
            Some(parse_string_to_jsonb(r#"{"x":2}"#).unwrap()),
        ];
        let source = Arc::new(BinaryArray::from_iter(
            source_values.iter().map(|value| value.as_deref()),
        )) as ArrayRef;
        let c = Arc::new(Int64Array::from_iter_values([10, 20])) as ArrayRef;

        let a_fields = Fields::from(vec![
            Arc::new(Field::new("b", DataType::Binary, true)),
            Arc::new(Field::new("c", DataType::Int64, true)),
        ]);
        let input_fields = Fields::from(vec![Arc::new(Field::new(
            "a",
            DataType::Struct(a_fields.clone()),
            true,
        ))]);
        let input = RecordBatch::try_new(
            schema([Field::new(
                "j",
                DataType::Struct(input_fields.clone()),
                true,
            )]),
            vec![Arc::new(StructArray::new(
                input_fields,
                vec![Arc::new(StructArray::new(a_fields, vec![source, c], None))],
                None,
            ))],
        )
        .unwrap();

        let output_schema = schema([Field::new(
            "j",
            DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                "a",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new(
                        "b",
                        DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                            "x",
                            DataType::Int64,
                            true,
                        ))])),
                        true,
                    )),
                    Arc::new(Field::new("c", DataType::Int64, true)),
                ])),
                true,
            ))])),
            true,
        )
        .with_extension_type(JsonExtensionType::new(Arc::new(JsonMetadata::default())))]);
        let plan = Json2FallbackPlan {
            items: vec![Json2FallbackItem {
                output_root_index: 0,
                source_path: vec!["j".to_string(), "a".to_string(), "b".to_string()],
                requested_paths: vec![vec![
                    "j".to_string(),
                    "a".to_string(),
                    "b".to_string(),
                    "x".to_string(),
                ]],
            }],
        };

        let mut aligner = NestedSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            plan,
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        let j = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a = j.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        let b = a.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            &[Some(1), Some(2)],
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
        assert_eq!(
            &[Some(10), Some(20)],
            a.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
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
