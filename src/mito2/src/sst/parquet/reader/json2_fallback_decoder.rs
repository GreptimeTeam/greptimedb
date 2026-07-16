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
use std::sync::Arc;
use std::task::{Context, Poll};

use datatypes::arrow::array::{Array, ArrayRef, BinaryBuilder, StructArray, new_null_array};
use datatypes::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::json::value::encode_serde_json_as_jsonb;
use datatypes::vectors::json::array::JsonArray;
use futures::Stream;
use snafu::{OptionExt, ResultExt, ensure};

use crate::error::{DataTypeMismatchSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu};
use crate::sst::parquet::read_columns::{Json2FallbackItem, Json2FallbackPlan, ParquetNestedPath};

/// Recovers requested JSON2 nested paths from fallback parent values.
///
/// In JSON2, a nested field may be promoted to a variant jsonb value after a
/// schema conflict. For example, the user requests `j.a.b`, but an parquet
/// file only has `j.a: Binary(jsonb)` because `j.a` used to contain mixed
/// shapes. The `j.a` jsonb value may still contain `b`, so the projection
/// planner reads `j.a` as a fallback source, and this decoder extracts the
/// requested `j.a.b` value from it.
///
/// The decoder runs before [`NestedSchemaAligner`]. It only processes roots
/// listed in [`Json2FallbackPlan`]; other roots pass through unchanged. Missing
/// projected roots are still handled later by [`NestedSchemaAligner`].
///
/// Example:
///
/// ```text
/// requested schema: j: Struct<a: Struct<b: Int64>>
/// parquet batch:    j: Struct<a: Binary(jsonb)>
/// fallback plan:    source_path = ["j", "a"]
///                   requested_paths = [["j", "a", "b"]]
/// decoder output:   j: Struct<a: Struct<b: Binary(jsonb)>>
/// aligner output:   j: Struct<a: Struct<b: Int64>>
/// ```
#[derive(derive_more::Debug)]
pub struct Json2FallbackDecoder<S> {
    #[debug(skip)]
    inner: S,
    projected_root_presence: Vec<bool>,
    expected_input_col_num: usize,
    fallbacks_by_output_root: Vec<Vec<Json2FallbackItem>>,
    output_schema: SchemaRef,
}

impl<S> Json2FallbackDecoder<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    pub fn new(
        inner: S,
        projected_root_presence: Vec<bool>,
        plan: Json2FallbackPlan,
        output_schema: SchemaRef,
    ) -> Result<Json2FallbackDecoder<S>> {
        ensure!(
            projected_root_presence.len() == output_schema.fields().len(),
            UnexpectedSnafu {
                reason: format!(
                    "Json2FallbackDecoder projected root presence len {} does not match output schema columns {}",
                    projected_root_presence.len(),
                    output_schema.fields().len()
                ),
            }
        );

        let expected_input_col_num = projected_root_presence
            .iter()
            .filter(|matched| **matched)
            .count();

        let mut fallbacks_by_output_root = vec![Vec::new(); output_schema.fields().len()];
        for item in plan.items {
            ensure!(
                item.output_root_index < fallbacks_by_output_root.len(),
                UnexpectedSnafu {
                    reason: format!(
                        "Json2FallbackDecoder fallback output root index {} out of bounds {}",
                        item.output_root_index,
                        fallbacks_by_output_root.len()
                    ),
                }
            );
            fallbacks_by_output_root[item.output_root_index].push(item);
        }

        Ok(Json2FallbackDecoder {
            inner,
            projected_root_presence,
            expected_input_col_num,
            fallbacks_by_output_root,
            output_schema,
        })
    }
}

impl<S> Stream for Json2FallbackDecoder<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(rb))) => Poll::Ready(Some(rebuild_recordbatch_with_fallback(
                rb,
                &this.projected_root_presence,
                this.expected_input_col_num,
                &this.fallbacks_by_output_root,
                &this.output_schema,
            ))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn rebuild_recordbatch_with_fallback(
    rb: RecordBatch,
    projected_root_presence: &[bool],
    expected_input_col_num: usize,
    fallbacks_by_output_root: &[Vec<Json2FallbackItem>],
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    if fallbacks_by_output_root.iter().all(Vec::is_empty) {
        return Ok(rb);
    }

    ensure!(
        rb.columns().len() == expected_input_col_num,
        UnexpectedSnafu {
            reason: format!(
                "Json2FallbackDecoder expected {} input columns but got {}",
                expected_input_col_num,
                rb.columns().len()
            ),
        }
    );

    let mut columns = Vec::with_capacity(rb.num_columns());
    let mut fields = Vec::with_capacity(rb.num_columns());
    let mut input_idx = 0;

    for (output_root_index, (field, present)) in output_schema
        .fields()
        .iter()
        .zip(projected_root_presence)
        .enumerate()
    {
        if !present {
            // The input batch only contains roots read from parquet. Missing
            // roots are synthesized later by NestedSchemaAligner.
            continue;
        }

        let input = rb.column(input_idx);
        let fallbacks = &fallbacks_by_output_root[output_root_index];
        if fallbacks.is_empty() {
            columns.push(input.clone());
            fields.push(Arc::new(rb.schema().field(input_idx).clone()));
        } else {
            let column = rebuild_root_with_fallbacks(input, field, fallbacks)?;
            fields.push(field_with_data_type(field, column.data_type().clone()));
            columns.push(column);
        }

        input_idx += 1;
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).context(NewRecordBatchSnafu)
}

fn rebuild_root_with_fallbacks(
    root_array: &ArrayRef,
    root_field: &FieldRef,
    fallbacks: &[Json2FallbackItem],
) -> Result<ArrayRef> {
    let mut extracted = Vec::new();

    for fallback in fallbacks {
        let source_path = fallback
            .source_path
            .get(1..)
            .with_context(|| UnexpectedSnafu {
                reason: format!(
                    "fallback source path length must be greater than 1, got {:?}",
                    fallback.source_path
                ),
            })?;

        let source_array = find_nested_array(root_array, source_path);
        for requested_path in &fallback.requested_paths {
            let relative_path = requested_path
                .strip_prefix(fallback.source_path.as_slice())
                .with_context(|| UnexpectedSnafu {
                    reason: format!(
                        "requested path {:?} is not under fallback source {:?}",
                        requested_path, fallback.source_path
                    ),
                })?;
            let array = extract_jsonb_array_at_path(
                source_array.as_ref(),
                relative_path,
                root_array.len(),
            )?;
            extracted.push((requested_path.clone(), array));
        }
    }

    build_json2_root(root_array, root_field, &extracted)
}

fn find_nested_array(root: &ArrayRef, path: &[String]) -> Option<ArrayRef> {
    let mut current = root.clone();
    for name in path {
        let struct_array = current.as_any().downcast_ref::<StructArray>()?;
        let idx = struct_array
            .fields()
            .iter()
            .position(|field| field.name() == name)?;
        current = struct_array.column(idx).clone();
    }
    Some(current)
}

pub(crate) fn json2_fallback_output_schema(
    output_schema: &SchemaRef,
    plan: &Json2FallbackPlan,
) -> SchemaRef {
    if plan.is_empty() {
        return output_schema.clone();
    }

    let fields = output_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(output_root_index, field)| {
            let items = plan
                .items
                .iter()
                .filter(|item| item.output_root_index == output_root_index)
                .collect::<Vec<_>>();
            if items.is_empty() {
                field.clone()
            } else {
                let path = vec![field.name().clone()];
                expand_fallback_field(field, &path, &items)
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

fn expand_fallback_field(
    field: &FieldRef,
    path: &[String],
    items: &[&Json2FallbackItem],
) -> FieldRef {
    if items.iter().any(|item| item.source_path == path) {
        return field_with_data_type(
            field,
            DataType::Struct(fallback_fields_from_requested_paths(path, items)),
        );
    }

    let DataType::Struct(fields) = field.data_type() else {
        return field.clone();
    };

    let mut expanded_fields = Vec::new();
    let mut child_names = fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    for name in requested_child_names(path, items) {
        if !child_names.contains(&name) {
            child_names.push(name);
        }
    }

    for name in child_names {
        let mut child_path = path.to_vec();
        child_path.push(name.clone());
        if let Some(field) = fields.iter().find(|field| field.name() == &name) {
            expanded_fields.push(expand_fallback_field(field, &child_path, items));
        } else {
            expanded_fields.push(fallback_field_from_requested_paths(
                &name,
                &child_path,
                items,
            ));
        }
    }

    field_with_data_type(field, DataType::Struct(Fields::from(expanded_fields)))
}

fn fallback_fields_from_requested_paths(path: &[String], items: &[&Json2FallbackItem]) -> Fields {
    let fields = requested_child_names(path, items)
        .into_iter()
        .map(|name| {
            let mut child_path = path.to_vec();
            child_path.push(name.clone());
            fallback_field_from_requested_paths(&name, &child_path, items)
        })
        .collect::<Vec<_>>();
    Fields::from(fields)
}

fn fallback_field_from_requested_paths(
    name: &str,
    path: &[String],
    items: &[&Json2FallbackItem],
) -> FieldRef {
    let data_type = if items.iter().any(|item| {
        item.requested_paths
            .iter()
            .any(|requested_path| requested_path == path)
    }) {
        DataType::Binary
    } else {
        DataType::Struct(fallback_fields_from_requested_paths(path, items))
    };
    Arc::new(Field::new(name, data_type, true))
}

fn requested_child_names(path: &[String], items: &[&Json2FallbackItem]) -> Vec<String> {
    let mut names = Vec::new();
    for item in items {
        for requested_path in &item.requested_paths {
            if requested_path.starts_with(path)
                && let Some(name) = requested_path.get(path.len())
                && !names.contains(name)
            {
                names.push(name.clone());
            }
        }
    }
    names
}

fn extract_jsonb_array_at_path(
    source: Option<&ArrayRef>,
    relative_path: &[String],
    len: usize,
) -> Result<ArrayRef> {
    let Some(source) = source else {
        return Ok(new_null_array(&DataType::Binary, len));
    };

    let json_array = JsonArray::from(source);
    let mut vals = Vec::with_capacity(len);
    let mut total_bytes = 0;
    for row in 0..len {
        let val = json_array
            .try_get_value(row)
            .context(DataTypeMismatchSnafu)?;
        let Some(val) = json_value_at_path(&val, relative_path) else {
            vals.push(None);
            continue;
        };
        if val.is_null() {
            vals.push(None);
            continue;
        }
        let bytes = encode_serde_json_as_jsonb(val.clone());
        total_bytes += bytes.len();
        vals.push(Some(bytes));
    }

    let mut builder = BinaryBuilder::with_capacity(len, total_bytes);
    for value in vals {
        builder.append_option(value);
    }
    Ok(Arc::new(builder.finish()))
}

fn json_value_at_path<'a>(
    mut value: &'a serde_json::Value,
    path: &[String],
) -> Option<&'a serde_json::Value> {
    for name in path {
        value = value.as_object()?.get(name)?;
    }
    Some(value)
}

fn build_json2_root(
    existing_root: &ArrayRef,
    root_field: &FieldRef,
    extracted: &[(ParquetNestedPath, ArrayRef)],
) -> Result<ArrayRef> {
    let DataType::Struct(fields) = root_field.data_type() else {
        return UnexpectedSnafu {
            reason: format!(
                "Json2FallbackDecoder expected struct root field {}, got {}",
                root_field.name(),
                root_field.data_type()
            ),
        }
        .fail();
    };

    let mut tree = FallbackPathTree::default();
    for (path, array) in extracted {
        let relative_path = path.get(1..).with_context(|| UnexpectedSnafu {
            reason: format!(
                "requested fallback path length must be greater than 1, got {:?}",
                path
            ),
        })?;
        tree.insert(relative_path, array.clone());
    }

    let existing_struct = existing_root.as_any().downcast_ref::<StructArray>();
    build_json2_struct(existing_struct, fields, &tree, existing_root.len())
}

#[derive(Default)]
struct FallbackPathTree {
    array: Option<ArrayRef>,
    children: Vec<(String, FallbackPathTree)>,
}

impl FallbackPathTree {
    fn insert(&mut self, path: &[String], array: ArrayRef) {
        let Some((name, rest)) = path.split_first() else {
            self.array = Some(array);
            return;
        };

        if let Some((_, child)) = self.children.iter_mut().find(|(child, _)| child == name) {
            child.insert(rest, array);
            return;
        }

        let mut child = FallbackPathTree::default();
        child.insert(rest, array);
        self.children.push((name.clone(), child));
    }

    fn child(&self, name: &str) -> Option<&FallbackPathTree> {
        self.children
            .iter()
            .find(|(child, _)| child == name)
            .map(|(_, child)| child)
    }
}

fn build_json2_struct(
    existing: Option<&StructArray>,
    fields: &Fields,
    tree: &FallbackPathTree,
    len: usize,
) -> Result<ArrayRef> {
    let fields = merge_fields_with_fallback_tree(fields, tree);
    let mut columns = Vec::with_capacity(fields.len());
    let mut output_fields = Vec::with_capacity(fields.len());

    for field in &fields {
        let existing_child =
            existing.and_then(|struct_array| find_struct_child(struct_array, field.name()));
        let Some(fallback) = tree.child(field.name()) else {
            let column = existing_child.unwrap_or_else(|| new_null_array(field.data_type(), len));
            output_fields.push(field_with_data_type(field, column.data_type().clone()));
            columns.push(column);
            continue;
        };

        if let Some(array) = &fallback.array
            && fallback.children.is_empty()
        {
            output_fields.push(field_with_data_type(field, array.data_type().clone()));
            columns.push(array.clone());
        } else {
            let column = build_json2_child_struct(existing_child.as_ref(), field, fallback, len)?;
            output_fields.push(field_with_data_type(field, column.data_type().clone()));
            columns.push(column);
        }
    }

    let nulls = existing.and_then(|struct_array| struct_array.nulls().cloned());
    let array = StructArray::try_new(Fields::from(output_fields), columns, nulls).map_err(|e| {
        UnexpectedSnafu {
            reason: e.to_string(),
        }
        .build()
    })?;
    Ok(Arc::new(array))
}

fn build_json2_child_struct(
    existing_child: Option<&ArrayRef>,
    field: &FieldRef,
    tree: &FallbackPathTree,
    len: usize,
) -> Result<ArrayRef> {
    let existing_struct =
        existing_child.and_then(|array| array.as_any().downcast_ref::<StructArray>());
    let fields = match field.data_type() {
        DataType::Struct(fields) => merge_fields_with_fallback_tree(fields, tree),
        _ => fallback_fields_from_tree(tree),
    };
    build_json2_struct(existing_struct, &fields, tree, len)
}

fn merge_fields_with_fallback_tree(fields: &Fields, tree: &FallbackPathTree) -> Fields {
    let mut merged = fields.iter().cloned().collect::<Vec<_>>();

    for (name, child) in &tree.children {
        if !merged.iter().any(|field| field.name() == name) {
            merged.push(fallback_field_from_tree(name, child));
        }
    }

    Fields::from(merged)
}

fn fallback_fields_from_tree(tree: &FallbackPathTree) -> Fields {
    let fields = tree
        .children
        .iter()
        .map(|(name, child)| fallback_field_from_tree(name, child))
        .collect::<Vec<_>>();
    Fields::from(fields)
}

fn fallback_field_from_tree(name: &str, tree: &FallbackPathTree) -> FieldRef {
    let data_type = if let Some(array) = &tree.array
        && tree.children.is_empty()
    {
        array.data_type().clone()
    } else {
        DataType::Struct(fallback_fields_from_tree(tree))
    };
    Arc::new(Field::new(name, data_type, true))
}

fn find_struct_child(struct_array: &StructArray, name: &str) -> Option<ArrayRef> {
    struct_array
        .fields()
        .iter()
        .position(|field| field.name() == name)
        .map(|idx| struct_array.column(idx).clone())
}

fn field_with_data_type(field: &FieldRef, data_type: DataType) -> FieldRef {
    let mut output = Field::new(field.name(), data_type, field.is_nullable());
    output.set_metadata(field.metadata().clone());
    Arc::new(output)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{
        Array, ArrayRef, BinaryArray, Int64Array, StringViewArray, StructArray,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datatypes::extension::json::{JsonExtensionType, JsonMetadata};
    use datatypes::types::parse_string_to_jsonb;
    use futures::{StreamExt, stream};

    use super::*;
    use crate::sst::parquet::reader::stream::NestedSchemaAligner;

    #[tokio::test]
    async fn test_json2_fallback_decoder_recovers_requested_paths() {
        let source_values = [
            Some(parse_string_to_jsonb(r#"{"b":1,"c":"x"}"#).unwrap()),
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
                    Arc::new(Field::new("b", DataType::Int64, true)),
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

        let decoder = Json2FallbackDecoder::new(
            stream::iter([Ok(input)]),
            vec![true],
            plan,
            output_schema.clone(),
        )
        .unwrap();
        let mut aligner =
            NestedSchemaAligner::new(decoder, vec![true], output_schema.clone()).unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        let j = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a = j.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            &[Some(1), Some(2), None],
            a.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
        assert_eq!(
            &[Some("x"), None, None],
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
    async fn test_json2_fallback_decoder_preserves_source_siblings() {
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

        let decoder = Json2FallbackDecoder::new(
            stream::iter([Ok(input)]),
            vec![true],
            plan,
            output_schema.clone(),
        )
        .unwrap();
        let mut aligner =
            NestedSchemaAligner::new(decoder, vec![true], output_schema.clone()).unwrap();
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
}
