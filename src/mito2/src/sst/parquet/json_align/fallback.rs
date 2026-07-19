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

use std::sync::Arc;

use datatypes::arrow::array::{Array, ArrayRef, BinaryBuilder, StructArray, new_null_array};
use datatypes::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datatypes::json::value::encode_serde_json_as_jsonb;
use datatypes::vectors::json::array::JsonArray;
use snafu::{OptionExt, ResultExt};

use crate::error::{DataTypeMismatchSnafu, Result, UnexpectedSnafu};
use crate::sst::parquet::read_columns::{Json2FallbackItem, ParquetNestedPath};

/// Applies JSON2 fallback extraction to a projected root array.
///
/// `root_array` is the root column actually read from parquet. `fallbacks`
/// describe requested nested paths that were not found as parquet leaves but can
/// be recovered from a nearest JSONB fallback source, such as reading `j.a` to
/// recover requested `j.a.b`.
///
/// The returned array preserves existing root children when possible and inserts
/// extracted fallback paths as JSONB/Binary leaves. It does not convert those
/// leaves to their final concrete types; nested schema alignment performs that
/// later through `align_array()` / `JsonArray::try_align(...)`.
pub(crate) fn apply_json2_fallbacks_to_root(
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

/// Returns a copy of `field` with `data_type`, preserving name, nullability, and metadata.
pub(crate) fn field_with_data_type(field: &FieldRef, data_type: DataType) -> FieldRef {
    let mut output = Field::new(field.name(), data_type, field.is_nullable());
    output.set_metadata(field.metadata().clone());
    Arc::new(output)
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

/// Rebuilds a JSON2 root array by overlaying extracted fallback arrays.
///
/// `extracted` paths include the root name. Existing children under
/// `existing_root` are preserved unless a fallback path replaces or extends that
/// branch.
fn build_json2_root(
    existing_root: &ArrayRef,
    root_field: &FieldRef,
    extracted: &[(ParquetNestedPath, ArrayRef)],
) -> Result<ArrayRef> {
    let DataType::Struct(fields) = root_field.data_type() else {
        return UnexpectedSnafu {
            reason: format!(
                "json2 fallback expected struct root field {}, got {}",
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

/// Requested fallback arrays organized by root-relative path.
///
/// Each node may hold an extracted array for the exact path and any number of
/// children for deeper requested paths. The tree lets reconstruction merge
/// existing struct children with fallback-only children deterministically.
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

/// Rebuilds one struct level from existing children plus fallback tree children.
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
