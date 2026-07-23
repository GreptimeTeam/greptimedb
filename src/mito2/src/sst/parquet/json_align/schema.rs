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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{DataType as ArrowDataType, FieldRef};
use datatypes::arrow::datatypes::Schema;
use datatypes::extension::json::is_structured_json_field;
use store_api::storage::NestedPath;

/// Aligns nested struct fields according to the requested nested paths.
///
/// For each root field:
/// - An empty path list keeps the whole field unchanged.
/// - Non-JSON root fields ignore nested paths and keep the whole field unchanged.
/// - Structured JSON root fields are rebuilt from `nested_paths`.
/// - Existing schema fields are preserved only when they are the requested leaf.
/// - Requested paths missing from the schema are synthesized with JSONB (`Binary`)
///   leaves.
///
/// For example, if the schema has `j: struct<a: struct<x: Int64, y: Utf8>>`
/// and `nested_paths` requests `j.a.x` and `j.a.z`, the result is
/// `j: struct<a: struct<x: Int64, z: Binary>>`.
pub(crate) fn align_schema_by_nested_paths<'a, I>(schema: &mut Schema, nested_paths: I)
where
    I: IntoIterator<Item = &'a [NestedPath]>,
{
    let fields = schema
        .fields
        .into_iter()
        .zip(nested_paths)
        .map(|(field, paths)| {
            if !paths.is_empty() && is_structured_json_field(field) {
                let child_paths = paths
                    .iter()
                    .map(|path| {
                        if path.first().is_some_and(|root| root == field.name()) {
                            &path[1..]
                        } else {
                            path
                        }
                    })
                    .collect::<Vec<_>>();
                rebuild_field_by_nested_paths(field, &child_paths)
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    schema.fields = fields.into()
}

fn rebuild_field_by_nested_paths(field: &FieldRef, nested_paths: &[&[String]]) -> FieldRef {
    if nested_paths.iter().any(|path| path.is_empty()) {
        return field.clone();
    };

    let fields = group_child_paths(nested_paths)
        .into_iter()
        .map(|(name, paths)| {
            let existing = find_struct_child(field, &name);
            build_field_from_nested_paths(&name, existing, &paths)
        })
        .collect::<Vec<_>>();

    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(ArrowDataType::Struct(fields.into())),
    )
}

fn group_child_paths<'a>(nested_paths: &[&'a [String]]) -> BTreeMap<String, Vec<&'a [String]>> {
    let mut child_paths = BTreeMap::<String, Vec<&'a [String]>>::new();
    for path in nested_paths {
        let Some((name, remaining)) = path.split_first() else {
            continue;
        };
        child_paths.entry(name.clone()).or_default().push(remaining);
    }
    child_paths
}

fn find_struct_child<'a>(field: &'a FieldRef, name: &str) -> Option<&'a FieldRef> {
    let ArrowDataType::Struct(fields) = field.data_type() else {
        return None;
    };
    fields.iter().find(|field| field.name() == name)
}

fn build_field_from_nested_paths(
    name: &str,
    existing: Option<&FieldRef>,
    nested_paths: &[&[String]],
) -> FieldRef {
    if nested_paths.iter().any(|path| path.is_empty()) {
        return existing.cloned().unwrap_or_else(|| new_jsonb_field(name));
    }

    let fields = group_child_paths(nested_paths)
        .into_iter()
        .map(|(name, paths)| {
            let existing_child = existing.and_then(|field| find_struct_child(field, &name));
            build_field_from_nested_paths(&name, existing_child, &paths)
        })
        .collect::<Vec<_>>();

    let field = existing
        .map(|field| field.as_ref().clone())
        .unwrap_or_else(|| arrow_schema::Field::new(name, ArrowDataType::Binary, true));
    Arc::new(field.with_data_type(ArrowDataType::Struct(fields.into())))
}

fn new_jsonb_field(name: &str) -> FieldRef {
    Arc::new(arrow_schema::Field::new(name, ArrowDataType::Binary, true))
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;
    use datatypes::extension::json::{JsonExtensionType, JsonMetadata};

    use super::*;

    #[test]
    fn test_align_schema_by_nested_paths() {
        fn new_field(name: &str, data_type: ArrowDataType) -> FieldRef {
            Arc::new(Field::new(name, data_type, true))
        }

        fn struct_field(name: &str, fields: impl IntoIterator<Item = FieldRef>) -> FieldRef {
            new_field(name, ArrowDataType::Struct(fields.into_iter().collect()))
        }

        fn json_struct_field(name: &str, fields: impl IntoIterator<Item = FieldRef>) -> FieldRef {
            Arc::new(
                Field::new(
                    name,
                    ArrowDataType::Struct(fields.into_iter().collect()),
                    true,
                )
                .with_extension_type(JsonExtensionType::new(Arc::new(JsonMetadata::default()))),
            )
        }

        let mut schema = Schema::new([
            json_struct_field(
                "j",
                [
                    struct_field(
                        "a",
                        [
                            new_field("x", ArrowDataType::Int64),
                            new_field("y", ArrowDataType::Utf8),
                            struct_field(
                                "z",
                                [
                                    new_field("q", ArrowDataType::Boolean),
                                    new_field("r", ArrowDataType::Float64),
                                ],
                            ),
                        ],
                    ),
                    new_field("b", ArrowDataType::Utf8),
                    struct_field(
                        "c",
                        vec![
                            new_field("d", ArrowDataType::Int64),
                            new_field("e", ArrowDataType::Utf8),
                        ],
                    ),
                ],
            ),
            new_field("tag", ArrowDataType::Utf8),
            struct_field(
                "k",
                [
                    new_field("k_0", ArrowDataType::Int64),
                    new_field("k_1", ArrowDataType::Utf8),
                ],
            ),
        ]);

        let nested_paths = [
            vec![
                ["j", "a", "x"].iter().map(|x| x.to_string()).collect(),
                ["j", "a", "z", "q"].iter().map(|x| x.to_string()).collect(),
                ["j", "a", "m"].iter().map(|x| x.to_string()).collect(),
                ["j", "b", "x"].iter().map(|x| x.to_string()).collect(),
                ["j", "c"].iter().map(|x| x.to_string()).collect(),
                ["j", "d", "e"].iter().map(|x| x.to_string()).collect(),
            ],
            vec![["tag", "ignored"].iter().map(|x| x.to_string()).collect()],
            vec![],
        ];

        align_schema_by_nested_paths(
            &mut schema,
            nested_paths.iter().map(|paths| paths.as_slice()),
        );

        let expected = Schema::new([
            json_struct_field(
                "j",
                [
                    struct_field(
                        "a",
                        [
                            new_field("m", ArrowDataType::Binary),
                            new_field("x", ArrowDataType::Int64),
                            struct_field("z", vec![new_field("q", ArrowDataType::Boolean)]),
                        ],
                    ),
                    struct_field("b", [new_field("x", ArrowDataType::Binary)]),
                    struct_field(
                        "c",
                        [
                            new_field("d", ArrowDataType::Int64),
                            new_field("e", ArrowDataType::Utf8),
                        ],
                    ),
                    struct_field("d", [new_field("e", ArrowDataType::Binary)]),
                ],
            ),
            new_field("tag", ArrowDataType::Utf8),
            struct_field(
                "k",
                [
                    new_field("k_0", ArrowDataType::Int64),
                    new_field("k_1", ArrowDataType::Utf8),
                ],
            ),
        ]);

        assert_eq!(schema, expected);
    }
}
