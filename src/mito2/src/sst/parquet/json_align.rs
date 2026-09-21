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

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion_common::cast_column;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datatypes::arrow::array::{ArrayRef, new_null_array};
use datatypes::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::extension::json::{JsonMetadata, is_json2_extension_type};
use datatypes::json::{JsonSettings, TypeHintMismatchPolicy};
use datatypes::vectors::json::array::JsonArray;
use datatypes::vectors::json::json2_physical_data_type;
use futures::Stream;
use futures::stream::BoxStream;
use serde_json::from_str;
use snafu::{ResultExt, ensure};

use crate::error::{
    CastColumnSnafu, DataTypeMismatchSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::sst::parquet::Json2TargetLayout;

pub(crate) type ProjectedRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Specifies how JSON columns in a record batch are aligned.
#[derive(Debug)]
pub(crate) enum AlignMode {
    /// Aligns JSON columns to the logical fields in the output schema.
    AlignToSchema,
    /// Rewrites JSON columns to physical layouts, typically for compaction.
    Rewrite {
        /// Target layouts keyed by root column name, not nested field path.
        ///
        /// Only listed columns are rewritten. Other existing arrays are reused
        /// unchanged and must already match their output field types.
        /// An empty map therefore only fills missing roots.
        columns: HashMap<String, Json2TargetLayout>,
    },
}

/// Alignment mode with parsed rewrite metadata and validated target layouts.
#[derive(Debug)]
enum ResolvedAlignMode {
    AlignToSchema,
    Rewrite {
        columns: HashMap<String, RewriteSettings>,
    },
}

/// Adapts Parquet record batches to the output schema expected by the reader.
///
/// Nested projection can return only part of a JSON2 column, or omit its root
/// entirely when no requested leaves are read. This stream restores missing
/// roots with null arrays of the expected types.
///
/// Existing JSON2 columns are aligned to the logical schema inferred from
/// type hints or rewritten to the specified JSON2 physical layout, as selected by
/// [`AlignMode`].
#[derive(derive_more::Debug)]
pub struct JsonSchemaAligner<S> {
    #[debug(skip)]
    inner: S,
    /// Output schema expected by the upper reader.
    output_schema: SchemaRef,
    /// Whether each projected root exists in the physical batch returned by
    /// parquet.
    projected_root_presence: Vec<bool>,
    /// Number of columns expected from the physical batch returned by parquet.
    expected_input_col_num: usize,
    /// Whether all projected roots are present and the stream can pass batches
    /// through.
    all_roots_present: bool,
    /// Alignment mode with parsed and validated rewrite settings.
    mode: ResolvedAlignMode,
    /// The cache for whether incoming batches already match output schema.
    is_schema_matched: Option<bool>,
}

impl<S> JsonSchemaAligner<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    /// Creates an aligner with a shared output schema and an explicit operation.
    /// Parses rewrite metadata once and validates layouts against output field types.
    pub(crate) fn new(
        inner: S,
        projected_root_presence: Vec<bool>,
        output_schema: SchemaRef,
        mode: AlignMode,
    ) -> Result<JsonSchemaAligner<S>> {
        ensure!(
            projected_root_presence.len() == output_schema.fields().len(),
            UnexpectedSnafu {
                reason: format!(
                    "JsonSchemaAligner projected root presence len {} does not match output schema columns {}",
                    projected_root_presence.len(),
                    output_schema.fields().len()
                ),
            }
        );

        let mode = resolve_align_mode(mode, output_schema.as_ref())?;

        let expected_input_col_num = projected_root_presence
            .iter()
            .filter(|matched| **matched)
            .count();
        let all_roots_present = projected_root_presence.iter().all(|&m| m);
        Ok(JsonSchemaAligner {
            inner,
            output_schema,
            projected_root_presence,
            expected_input_col_num,
            all_roots_present,
            mode,
            is_schema_matched: None,
        })
    }
}

impl<S> Stream for JsonSchemaAligner<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(rb))) => {
                let is_schema_matched = matches!(this.mode, ResolvedAlignMode::AlignToSchema)
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
                        &this.mode,
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
    mode: &ResolvedAlignMode,
) -> Result<RecordBatch> {
    ensure!(
        rb.columns().len() == expected_input_col_num,
        UnexpectedSnafu {
            reason: format!(
                "JsonSchemaAligner expected {} input columns but got {}",
                expected_input_col_num,
                rb.columns().len()
            ),
        }
    );

    let mut cols = Vec::with_capacity(projected_root_presence.len());
    let mut idx = 0;
    let input_schema = rb.schema_ref();

    for (field, present) in output_schema.fields().iter().zip(projected_root_presence) {
        if !present {
            cols.push(new_null_array(field.data_type(), rb.num_rows()));
            continue;
        }

        let array = match mode {
            ResolvedAlignMode::AlignToSchema => {
                align_array(rb.column(idx), input_schema.field(idx), field)?
            }
            ResolvedAlignMode::Rewrite { columns } => match columns.get(field.name()) {
                Some(settings) => rewrite_array(rb.column(idx), input_schema.field(idx), settings)?,
                None => rb.column(idx).clone(),
            },
        };
        cols.push(array);
        idx += 1;
    }

    RecordBatch::try_new(output_schema.clone(), cols).context(NewRecordBatchSnafu)
}

fn align_array(
    source_array: &ArrayRef,
    source_field: &Field,
    target_field: &FieldRef,
) -> Result<ArrayRef> {
    if source_array.data_type() == target_field.data_type() {
        return Ok(source_array.clone());
    }

    if is_json2_extension_type(target_field) {
        if is_json2_extension_type(source_field) {
            return JsonArray::from(source_array)
                .project_to_v2(source_field, target_field.data_type())
                .context(DataTypeMismatchSnafu);
        }
        return JsonArray::from(source_array)
            .project_to(target_field.data_type())
            .context(DataTypeMismatchSnafu);
    }

    if !matches!(target_field.data_type(), DataType::Struct(_)) {
        return Ok(source_array.clone());
    }

    cast_column(
        source_array,
        target_field.data_type(),
        &DEFAULT_CAST_OPTIONS,
    )
    .context(CastColumnSnafu)
}

fn rewrite_array(
    source_array: &ArrayRef,
    source_field: &Field,
    settings: &RewriteSettings,
) -> Result<ArrayRef> {
    JsonArray::from(source_array)
        .rewrite_to_v2_with_type_hint_mismatch_policy(
            source_field,
            &settings.logical_settings,
            &settings.target_layout,
            TypeHintMismatchPolicy::CoerceOrNull,
        )
        .context(DataTypeMismatchSnafu)
}

/// Resolved settings for rewriting one JSON2 column to a target physical layout.
///
/// Created from [`Json2TargetLayout`] when resolving the alignment mode, so
/// extension metadata is parsed once and reused across batches.
#[derive(Debug)]
struct RewriteSettings {
    /// Logical settings parsed from extension metadata and applied to JSON values
    /// before encoding them into the target layout.
    logical_settings: JsonSettings,
    /// Settings defining the physical Arrow layout of the rewritten column.
    target_layout: JsonSettings,
}

impl TryFrom<&Json2TargetLayout> for RewriteSettings {
    type Error = crate::error::Error;

    fn try_from(layout: &Json2TargetLayout) -> Result<Self> {
        let metadata = from_str::<JsonMetadata>(&layout.extension_metadata).map_err(|e| {
            UnexpectedSnafu {
                reason: format!("invalid JSON2 extension metadata: {e}"),
            }
            .build()
        })?;
        Ok(Self {
            logical_settings: metadata.into_json_settings(),
            target_layout: layout.target_layout.clone(),
        })
    }
}

/// Parses rewrite metadata and validates target layouts against the output schema.
fn resolve_align_mode(mode: AlignMode, output_schema: &Schema) -> Result<ResolvedAlignMode> {
    let AlignMode::Rewrite { columns } = mode else {
        return Ok(ResolvedAlignMode::AlignToSchema);
    };

    let mut rewrite_columns = HashMap::with_capacity(columns.len());
    for (name, layout) in columns {
        let settings = RewriteSettings::try_from(&layout)?;
        let field = output_schema.field_with_name(&name).map_err(|_| {
            UnexpectedSnafu {
                reason: format!("JSON2 rewrite column '{name}' is missing from output schema"),
            }
            .build()
        })?;
        ensure!(
            is_json2_extension_type(field)
                && field.data_type() == &json2_physical_data_type(&settings.target_layout),
            UnexpectedSnafu {
                reason: format!(
                    "JSON2 rewrite layout for column '{name}' does not match output field"
                ),
            }
        );
        rewrite_columns.insert(name, settings);
    }

    Ok(ResolvedAlignMode::Rewrite {
        columns: rewrite_columns,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datatypes::arrow::array::{
        Array, ArrayRef, BinaryArray, Int64Array, StringArray, StringViewArray, StructArray,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datatypes::extension::json::{Json2ExtensionType, JsonMetadata};
    use datatypes::json::JsonTypeHint;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::types::parse_string_to_jsonb;
    use futures::{StreamExt, stream};

    use super::*;

    #[test]
    fn test_aligner_resolves_json2_rewrite_settings()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let logical_settings = JsonSettings::default();
        let target_layout = JsonSettings::try_new(vec![], Some(0))?;
        let rewrite_targets = HashMap::from([(
            "j".to_string(),
            Json2TargetLayout {
                extension_metadata: serde_json::to_string(&JsonMetadata::new(
                    logical_settings.clone(),
                ))?,
                target_layout: target_layout.clone(),
            },
        )]);
        let aligner = JsonSchemaAligner::new(
            stream::empty::<Result<RecordBatch>>(),
            vec![false],
            schema([
                Field::new("j", json2_physical_data_type(&target_layout), true)
                    .with_extension_type(Json2ExtensionType::default()),
            ]),
            AlignMode::Rewrite {
                columns: rewrite_targets,
            },
        )?;
        let ResolvedAlignMode::Rewrite { columns } = &aligner.mode else {
            panic!("expected rewrite mode");
        };
        let settings = &columns["j"];
        assert_eq!(logical_settings, settings.logical_settings);
        assert_eq!(target_layout, settings.target_layout);
        Ok(())
    }

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

        let mut aligner = JsonSchemaAligner::new(
            stream,
            vec![true, true],
            output_schema.clone(),
            AlignMode::AlignToSchema,
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

        let mut aligner = JsonSchemaAligner::new(
            stream,
            vec![true, false, false],
            output_schema.clone(),
            AlignMode::AlignToSchema,
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

        let mut aligner = JsonSchemaAligner::new(
            stream,
            vec![true, false],
            output_schema.clone(),
            AlignMode::AlignToSchema,
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

        let err = match JsonSchemaAligner::new(
            stream,
            vec![true, false],
            output_schema,
            AlignMode::AlignToSchema,
        ) {
            Ok(_) => panic!("JsonSchemaAligner should reject projection length mismatch"),
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

        let mut aligner = JsonSchemaAligner::new(
            stream,
            vec![true, true, false],
            output_schema,
            AlignMode::AlignToSchema,
        )
        .unwrap();
        let err = aligner.next().await.unwrap().unwrap_err();

        assert!(
            err.to_string()
                .contains("expected 2 input columns but got 1")
        );
    }

    #[tokio::test]
    async fn test_json_schema_aligner_aligns_struct_field() {
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

        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            AlignMode::AlignToSchema,
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
    async fn test_json_schema_aligner_decodes_variant_to_struct() {
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
        .with_extension_type(Json2ExtensionType::default())]);
        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            AlignMode::AlignToSchema,
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
    async fn test_json_schema_aligner_preserves_struct_siblings() {
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
        .with_extension_type(Json2ExtensionType::default())]);
        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            AlignMode::AlignToSchema,
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

    #[tokio::test]
    async fn test_rewrite_multiple_columns_and_fill_missing_roots() {
        let logical_settings = JsonSettings::default();
        let target_layout = JsonSettings::try_new(vec![], Some(0)).unwrap();
        let target_type = json2_physical_data_type(&target_layout);
        let output_schema = schema([
            Field::new("j", target_type.clone(), true)
                .with_extension_type(Json2ExtensionType::default()),
            Field::new("missing", target_type.clone(), true)
                .with_extension_type(Json2ExtensionType::default()),
            Field::new("k", target_type.clone(), true)
                .with_extension_type(Json2ExtensionType::default()),
            Field::new("a", DataType::Int64, true),
        ]);
        let values = [Some(parse_string_to_jsonb(r#"{"x":1}"#).unwrap()), None];
        let source = Arc::new(BinaryArray::from_iter(
            values.iter().map(|value| value.as_deref()),
        )) as ArrayRef;
        let source_field = Field::new("j", DataType::Binary, true)
            .with_extension_type(Json2ExtensionType::default());
        let expected = JsonArray::from(&source)
            .rewrite_to_v2(&source_field, &logical_settings, &target_layout)
            .unwrap();
        let input = RecordBatch::try_new(
            schema([
                source_field,
                Field::new("k", DataType::Binary, true)
                    .with_extension_type(Json2ExtensionType::default()),
                Field::new("a", DataType::Int64, true),
            ]),
            vec![source.clone(), source, int_array([10, 20])],
        )
        .unwrap();
        let columns = ["j", "missing", "k"]
            .into_iter()
            .map(|name| {
                (
                    name.to_string(),
                    Json2TargetLayout {
                        extension_metadata: serde_json::to_string(&JsonMetadata::new(
                            logical_settings.clone(),
                        ))
                        .unwrap(),
                        target_layout: target_layout.clone(),
                    },
                )
            })
            .collect();
        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true, false, true, true],
            output_schema.clone(),
            AlignMode::Rewrite { columns },
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();
        assert_eq!(output_schema, output.schema());
        assert_eq!(expected.as_ref(), output.column(0).as_ref());
        assert_eq!(expected.as_ref(), output.column(2).as_ref());
        assert_eq!(&target_type, output.column(1).data_type());
        assert_eq!(2, output.column(1).null_count());
        assert_eq!(int_array([10, 20]).as_ref(), output.column(3).as_ref());
    }

    #[tokio::test]
    async fn test_rewrite_keeps_rows_with_invalid_json2_settings() {
        let settings = JsonSettings::try_new(
            vec![JsonTypeHint {
                path: vec!["kind".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                inverted_index: false,
            }],
            Some(0),
        )
        .unwrap();
        let target_type = json2_physical_data_type(&settings);
        let output_schema = schema([
            Field::new("j", target_type.clone(), true).with_extension_type(
                Json2ExtensionType::new(Arc::new(JsonMetadata::new(settings.clone()))),
            ),
            Field::new("value", DataType::Int64, true),
        ]);
        let source = Arc::new(BinaryArray::from_iter([
            Some(parse_string_to_jsonb(r#"{"kind":"valid"}"#).unwrap()),
            Some(parse_string_to_jsonb(r#"{"kind":1}"#).unwrap()),
        ])) as ArrayRef;
        let input = RecordBatch::try_new(
            schema([
                Field::new("j", DataType::Binary, true)
                    .with_extension_type(Json2ExtensionType::default()),
                Field::new("value", DataType::Int64, true),
            ]),
            vec![source, int_array([10, 20])],
        )
        .unwrap();
        let columns = HashMap::from([(
            "j".to_string(),
            Json2TargetLayout {
                extension_metadata: serde_json::to_string(&JsonMetadata::new(settings.clone()))
                    .unwrap(),
                target_layout: settings,
            },
        )]);
        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true, true],
            output_schema,
            AlignMode::Rewrite { columns },
        )
        .unwrap();

        let output = aligner.next().await.unwrap().unwrap();
        assert_eq!(2, output.num_rows());
        assert_eq!(
            10,
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        );
        assert_eq!(
            20,
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1)
        );
    }

    #[test]
    fn test_rewrite_rejects_mismatched_output_layout() {
        let columns = HashMap::from([(
            "j".to_string(),
            Json2TargetLayout {
                extension_metadata: serde_json::to_string(&JsonMetadata::new(
                    JsonSettings::default(),
                ))
                .unwrap(),
                target_layout: JsonSettings::try_new(vec![], Some(0)).unwrap(),
            },
        )]);
        let result = JsonSchemaAligner::new(
            stream::empty::<Result<RecordBatch>>(),
            vec![false],
            schema([Field::new("j", DataType::Binary, true)
                .with_extension_type(Json2ExtensionType::default())]),
            AlignMode::Rewrite { columns },
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("does not match output field")
        );
    }

    #[tokio::test]
    async fn test_empty_rewrite_only_fills_missing_roots() {
        let source = int_array([10, 20]);
        let input = RecordBatch::try_new(
            schema([Field::new("a", DataType::Int64, true)]),
            vec![source.clone()],
        )
        .unwrap();
        let output_schema = schema([
            Field::new("missing", DataType::Utf8, true),
            Field::new("a", DataType::Int64, true),
        ]);
        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![false, true],
            output_schema.clone(),
            AlignMode::Rewrite {
                columns: HashMap::new(),
            },
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();
        assert_eq!(output_schema, output.schema());
        assert_eq!(2, output.num_rows());
        assert_eq!(2, output.column(0).null_count());
        assert!(Arc::ptr_eq(&source, output.column(1)));
    }

    #[tokio::test]
    async fn test_empty_rewrite_does_not_align_existing_struct() {
        let source = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("x", DataType::Int64, true)),
            int_array([1, 2]),
        )])) as ArrayRef;
        let input = RecordBatch::try_new(
            schema([Field::new("j", source.data_type().clone(), true)]),
            vec![source],
        )
        .unwrap();
        let output_schema = schema([Field::new(
            "j",
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int64, true),
                Field::new("y", DataType::Utf8, true),
            ])),
            true,
        )]);
        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema,
            AlignMode::Rewrite {
                columns: HashMap::new(),
            },
        )
        .unwrap();
        assert!(matches!(
            aligner.next().await.unwrap(),
            Err(crate::error::Error::NewRecordBatch { .. })
        ));
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
