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

use datatypes::arrow::array::{ArrayRef, BinaryBuilder, StructArray, new_null_array};
use datatypes::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::types::parse_string_to_jsonb;
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
    plan: Json2FallbackPlan,
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

        Ok(Json2FallbackDecoder {
            inner,
            projected_root_presence,
            plan,
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
            Poll::Ready(Some(Ok(rb))) => Poll::Ready(Some(recover_json2_fallbacks(
                rb,
                &this.projected_root_presence,
                &this.plan,
                &this.output_schema,
            ))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
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

fn recover_json2_fallbacks(
    rb: RecordBatch,
    projected_root_presence: &[bool],
    plan: &Json2FallbackPlan,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    if plan.is_empty() {
        return Ok(rb);
    }

    let expected_input_col_num = projected_root_presence
        .iter()
        .filter(|matched| **matched)
        .count();
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
            continue;
        }

        let input = rb.column(input_idx);
        let items = plan
            .items
            .iter()
            .filter(|item| item.output_root_index == output_root_index)
            .collect::<Vec<_>>();
        if items.is_empty() {
            columns.push(input.clone());
            fields.push(Arc::new(rb.schema().field(input_idx).clone()));
        } else {
            let column = recover_json2_root(input, field, &items)?;
            fields.push(field_with_data_type(field, column.data_type().clone()));
            columns.push(column);
        }

        input_idx += 1;
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).context(NewRecordBatchSnafu)
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

fn recover_json2_root(
    root: &ArrayRef,
    root_field: &FieldRef,
    items: &[&Json2FallbackItem],
) -> Result<ArrayRef> {
    let mut extracted = Vec::new();
    for item in items {
        let source = find_nested_array(root, &item.source_path[1..]);
        for requested_path in &item.requested_paths {
            let relative_path = requested_path
                .strip_prefix(item.source_path.as_slice())
                .with_context(|| UnexpectedSnafu {
                    reason: format!(
                        "requested path {:?} is not under fallback source {:?}",
                        requested_path, item.source_path
                    ),
                })?;
            let array = extract_jsonb_path(source.as_ref(), relative_path, root.len())?;
            extracted.push((requested_path.clone(), array));
        }
    }

    build_json2_root(root, root_field, &extracted)
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

fn extract_jsonb_path(
    source: Option<&ArrayRef>,
    relative_path: &[String],
    len: usize,
) -> Result<ArrayRef> {
    let Some(source) = source else {
        return Ok(new_null_array(&DataType::Binary, len));
    };

    let json_array = JsonArray::from(source);
    let mut values = Vec::with_capacity(len);
    let mut total_bytes = 0;
    for row in 0..len {
        let value = json_array
            .try_get_value(row)
            .context(DataTypeMismatchSnafu)?;
        let Some(value) = json_value_at_path(&value, relative_path) else {
            values.push(None);
            continue;
        };
        if value.is_null() {
            values.push(None);
            continue;
        }

        let bytes = parse_string_to_jsonb(&value.to_string()).context(DataTypeMismatchSnafu)?;
        total_bytes += bytes.len();
        values.push(Some(bytes));
    }

    let mut builder = BinaryBuilder::with_capacity(len, total_bytes);
    for value in values {
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

    build_json2_struct(existing_root, root_field.name(), fields, extracted)
}

fn build_json2_struct(
    existing: &ArrayRef,
    path: &str,
    fields: &Fields,
    extracted: &[(ParquetNestedPath, ArrayRef)],
) -> Result<ArrayRef> {
    let existing_struct = existing.as_any().downcast_ref::<StructArray>();
    let mut columns = Vec::with_capacity(fields.len());
    let mut output_fields = Vec::with_capacity(fields.len());

    for field in fields {
        let child_path = format!("{}.{}", path, field.name());
        if let Some(array) = extracted
            .iter()
            .find(|(requested_path, _)| requested_path.join(".") == child_path)
            .map(|(_, array)| array.clone())
        {
            output_fields.push(field_with_data_type(field, array.data_type().clone()));
            columns.push(array);
            continue;
        }

        let nested_extracted = extracted
            .iter()
            .filter(|(requested_path, _)| {
                let requested = requested_path.join(".");
                requested.starts_with(&format!("{child_path}."))
            })
            .map(|(path, array)| (path.clone(), array.clone()))
            .collect::<Vec<_>>();
        if !nested_extracted.is_empty() {
            let existing_child = existing_struct.and_then(|struct_array| {
                struct_array
                    .fields()
                    .iter()
                    .position(|existing_field| existing_field.name() == field.name())
                    .map(|idx| struct_array.column(idx).clone())
            });
            let existing_child = existing_child.unwrap_or_else(|| {
                let data_type = fallback_struct_type(&child_path, &nested_extracted)
                    .unwrap_or_else(|| field.data_type().clone());
                new_null_array(&data_type, existing.len())
            });
            let child_fields = match field.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => fallback_struct_fields(&child_path, &nested_extracted),
            };
            let child = build_json2_struct(
                &existing_child,
                &child_path,
                &child_fields,
                &nested_extracted,
            )?;
            output_fields.push(field_with_data_type(field, child.data_type().clone()));
            columns.push(child);
            continue;
        }

        if let Some(existing_child) = existing_struct.and_then(|struct_array| {
            struct_array
                .fields()
                .iter()
                .position(|existing_field| existing_field.name() == field.name())
                .map(|idx| struct_array.column(idx).clone())
        }) {
            output_fields.push(field_with_data_type(
                field,
                existing_child.data_type().clone(),
            ));
            columns.push(existing_child);
        } else {
            let nulls = new_null_array(field.data_type(), existing.len());
            output_fields.push(field.clone());
            columns.push(nulls);
        }
    }

    let array = StructArray::try_new(Fields::from(output_fields), columns, None).map_err(|e| {
        UnexpectedSnafu {
            reason: e.to_string(),
        }
        .build()
    })?;
    Ok(Arc::new(array))
}

fn fallback_struct_type(
    path: &str,
    extracted: &[(ParquetNestedPath, ArrayRef)],
) -> Option<DataType> {
    Some(DataType::Struct(fallback_struct_fields(path, extracted)))
}

fn fallback_struct_fields(path: &str, extracted: &[(ParquetNestedPath, ArrayRef)]) -> Fields {
    let mut fields = Vec::new();
    let prefix_len = path.split('.').count();

    for (requested_path, array) in extracted {
        let Some(name) = requested_path.get(prefix_len) else {
            continue;
        };
        if fields
            .iter()
            .any(|field: &FieldRef| field.name().as_str() == name)
        {
            continue;
        }

        let data_type = if requested_path.len() == prefix_len + 1 {
            array.data_type().clone()
        } else {
            fallback_struct_type(&format!("{path}.{name}"), extracted)
                .unwrap_or_else(|| DataType::Binary)
        };
        fields.push(Arc::new(Field::new(name, data_type, true)));
    }

    Fields::from(fields)
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

    fn schema(fields: impl IntoIterator<Item = Field>) -> SchemaRef {
        Arc::new(Schema::new(fields.into_iter().collect::<Vec<_>>()))
    }
}
