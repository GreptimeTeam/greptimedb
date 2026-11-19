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

use api::v1::SemanticType;
use arrow_schema::extension::ExtensionType;
use common_function::scalars::json::json_get::{JsonGetWithType, json_object_path};
use datafusion_expr::Expr;
use datatypes::arrow::array::{Array, ArrayRef, StructArray};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::extension::json::Json2ExtensionType;
use datatypes::json::JsonTypeHint;
use datatypes::schema::{FulltextBackend, FulltextOptions};
use datatypes::vectors::{Helper, VectorRef};
use index::target::IndexTarget;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::error::{ConvertVectorSnafu, IndexOptionsSnafu, InvalidRecordBatchSnafu, Result};
use crate::sst::index::IndexBuildType;
use crate::sst::index::handler::JsonIndexRebuildSource;
use crate::sst::index::kind::IndexKind;
use crate::sst::parquet::format::PrimaryKeyArray;

/// Whether a rebuild needs the physical schema before it can plan path indexes.
pub(crate) fn has_json_path_indexes(metadata: &RegionMetadata) -> Result<bool> {
    for column in &metadata.column_metadatas {
        if !column.column_schema.data_type.is_json2() {
            continue;
        }
        if json_extension(column)?.is_some_and(|ext| {
            ext.metadata()
                .json_settings()
                .type_hints()
                .iter()
                .any(|hint| {
                    hint.inverted_index
                        || hint.skipping_index.is_some()
                        || hint
                            .fulltext_index
                            .as_ref()
                            .is_some_and(|options| options.enable)
                })
        }) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Selects JSON hints whose values and index semantics are available in the build input.
pub(crate) fn json_index_hints(
    column: &ColumnMetadata,
    build_type: &IndexBuildType,
    source: Option<JsonIndexRebuildSource<'_>>,
    kind: IndexKind,
) -> Result<Vec<JsonTypeHint>> {
    let rebuilding = matches!(
        build_type,
        IndexBuildType::Manual | IndexBuildType::SchemaChange
    );
    // Manual candidate selection sees current hints; per-file planning narrows them to
    // the paths materialized in the source SST before creating any indexes.
    if !column.column_schema.data_type.is_json2() || (rebuilding && source.is_none()) {
        return Ok(Vec::new());
    }
    let current = match json_extension(column) {
        Ok(Some(extension)) => extension,
        Ok(None) => return Ok(Vec::new()),
        // Flush and compaction skip unreadable JSON hints for inverted and bloom indexes.
        Err(_) if !rebuilding && matches!(kind, IndexKind::Inverted | IndexKind::Bloom) => {
            return Ok(Vec::new());
        }
        Err(err) => return Err(err),
    };
    let hints = current.metadata().json_settings().type_hints();
    let Some(JsonIndexRebuildSource::Sst(metadata, schema)) = source else {
        return Ok(hints.to_vec());
    };
    let Some(original) = metadata.column_by_id(column.column_id) else {
        return Ok(Vec::new());
    };
    if original.column_schema.name != column.column_schema.name
        || !original.column_schema.data_type.is_json2()
    {
        return Ok(Vec::new());
    }
    let Some(original) = json_extension(original)? else {
        return Ok(Vec::new());
    };
    let Ok(root) = schema.field_with_name(&column.column_schema.name) else {
        return Ok(Vec::new());
    };
    let originals: HashMap<_, _> = original
        .metadata()
        .json_settings()
        .type_hints()
        .iter()
        .map(|hint| (hint.path.as_slice(), hint))
        .collect();
    Ok(hints
        .iter()
        .filter(|hint| {
            let Some(previous) = originals.get(hint.path.as_slice()) else {
                return false;
            };
            if previous.data_type != hint.data_type {
                return false;
            }
            if !materialized_leaf_matches(root.data_type(), hint) {
                return false;
            }
            match kind {
                IndexKind::Inverted => previous.inverted_index,
                IndexKind::Bloom => previous.skipping_index.is_some(),
                IndexKind::Fulltext => hint
                    .fulltext_index
                    .as_ref()
                    .zip(previous.fulltext_index.as_ref())
                    .is_some_and(|(expected, actual)| {
                        actual.enable && compatible_fulltext_options(expected, actual)
                    }),
                IndexKind::Extension(_) => false,
            }
        })
        .cloned()
        .collect())
}

fn materialized_leaf_matches(root: &arrow_schema::DataType, hint: &JsonTypeHint) -> bool {
    let leaf = hint.path.iter().try_fold(root, |ty, key| {
        let arrow_schema::DataType::Struct(fields) = ty else {
            return None;
        };
        fields.find(key).map(|(_, field)| field.data_type())
    });
    leaf.is_some_and(|ty| {
        *ty == hint.data_type.as_arrow_type() || (ty.is_string() && hint.data_type.is_string())
    })
}

fn json_extension(column: &ColumnMetadata) -> Result<Option<Json2ExtensionType>> {
    column
        .column_schema
        .extension_type::<Json2ExtensionType>()
        .with_context(|_| IndexOptionsSnafu {
            column_name: column.column_schema.name.clone(),
        })
}

pub(super) fn compatible_fulltext_options(
    expected: &FulltextOptions,
    actual: &FulltextOptions,
) -> bool {
    expected.backend == actual.backend
        && expected.case_sensitive == actual.case_sensitive
        && (expected.backend == FulltextBackend::Bloom || expected.analyzer == actual.analyzer)
}

/// Extracts leaf values for a JSON type hint at the given path.
///
/// Returns an error if the path does not exist, traverses a non-object,
/// or the leaf array cannot be converted to a vector.
///
/// # Details
///
/// The caller guarantees that the materialized leaf type matches the type hint.
///
/// JSON2 builders propagate ancestor nulls to child arrays. SST reads and schema
/// adaptation preserve this invariant, so no ancestor null bitmap merging is needed.
pub(crate) fn extract_json_type_hint_values(root: &ArrayRef, path: &[String]) -> Result<VectorRef> {
    let mut array = root.clone();
    for segment in path {
        let object = array
            .as_any()
            .downcast_ref::<StructArray>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("JSON index path {path:?} traverses a non-object"),
            })?;
        array = object
            .column_by_name(segment)
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("JSON index path {path:?} has no materialized field {segment}"),
            })?
            .clone();
    }
    Helper::try_into_vector(array).context(ConvertVectorSnafu)
}

/// Yields (first row, row count) for indexing a materialized column.
/// Tags are constant within consecutive equal PK dictionary keys. Fields and
/// timestamps must still be visited row by row. Distinct dictionary entries may
/// contain equal PK bytes; keeping those runs separate is correct and avoids decoding.
/// Inputs without a non-null PK dictionary fall back to visiting individual rows.
pub(crate) fn column_index_rows(
    batch: &RecordBatch,
    semantic_type: SemanticType,
) -> impl Iterator<Item = (usize, usize)> + '_ {
    let keys = (semantic_type == SemanticType::Tag)
        .then(|| batch.column_by_name(PRIMARY_KEY_COLUMN_NAME))
        .flatten()
        .and_then(|array| array.as_any().downcast_ref::<PrimaryKeyArray>())
        .filter(|array| array.null_count() == 0 && array.values().null_count() == 0)
        .map(|array| array.keys().values());
    let mut row = 0;
    std::iter::from_fn(move || {
        if row == batch.num_rows() {
            return None;
        }
        let start = row;
        row += 1;
        if let Some(keys) = keys {
            while row < keys.len() && keys[row] == keys[start] {
                row += 1;
            }
        }
        Some((start, row - start))
    })
}

/// Resolves a typed JSON access when its hint enables the requested index.
pub(crate) fn json_index_target(
    metadata: &RegionMetadata,
    expr: &Expr,
    enabled: impl Fn(&JsonTypeHint) -> bool,
) -> Result<Option<(IndexTarget, ConcreteDataType)>> {
    let Expr::ScalarFunction(func) = expr else {
        return Ok(None);
    };

    if func.func.name() != JsonGetWithType::NAME || !(2..=3).contains(&func.args.len()) {
        return Ok(None);
    }

    let Expr::Column(column) = &func.args[0] else {
        return Ok(None);
    };

    let Some(col) = metadata.column_by_name(&column.name) else {
        return Ok(None);
    };

    if !col.column_schema.data_type.is_json2() {
        return Ok(None);
    }

    let Some(path_literal) = func.args[1]
        .as_literal()
        .and_then(|v| v.try_as_str())
        .flatten()
    else {
        return Ok(None);
    };

    let Some(path) = json_object_path(path_literal).ok().flatten() else {
        return Ok(None);
    };

    let Ok(Some(extension)) = col.column_schema.extension_type::<Json2ExtensionType>() else {
        return Ok(None);
    };

    let Some(hint) = extension
        .metadata()
        .json_settings()
        .type_hints()
        .iter()
        .find(|hint| hint.path == path && enabled(hint))
    else {
        return Ok(None);
    };

    if let Some(type_arg) = func.args.get(2) {
        let Expr::Literal(value, _) = type_arg else {
            return Ok(None);
        };
        if !value.is_null() {
            return Ok(None);
        }
        // A type conversion may change comparison semantics and invalidate index pruning.
        let result_type = value.data_type();
        if result_type != hint.data_type.as_arrow_type()
            && !(result_type.is_string() && hint.data_type.is_string())
        {
            return Ok(None);
        }
    }

    if !(hint.data_type.is_numeric() || hint.data_type.is_boolean() || hint.data_type.is_string()) {
        return Ok(None);
    }
    Ok(IndexTarget::new_json_path(col.column_id, path)
        .ok()
        .map(|target| (target, hint.data_type.clone())))
}

/// Resolves a skipping target's encoding type from an SST's own hints.
pub(crate) fn skipping_target_data_type(
    metadata: &RegionMetadata,
    target: &IndexTarget,
) -> Option<ConcreteDataType> {
    let path = match target {
        IndexTarget::ColumnId(id) => {
            return metadata
                .column_by_id(*id)
                .map(|col| col.column_schema.data_type.clone());
        }
        IndexTarget::JsonPath(path) => path,
    };
    let column = metadata.column_by_id(path.column_id())?;
    let extension = column
        .column_schema
        .extension_type::<Json2ExtensionType>()
        .ok()??;
    extension
        .metadata()
        .json_settings()
        .type_hints()
        .iter()
        .find(|hint| hint.path == path.path() && hint.skipping_index.is_some())
        .map(|hint| hint.data_type.clone())
}

/// Resolves enabled fulltext options using a column or string hint's metadata.
pub(crate) fn fulltext_target_options(
    metadata: &RegionMetadata,
    target: &IndexTarget,
) -> Result<Option<FulltextOptions>> {
    let path = match target {
        IndexTarget::ColumnId(id) => {
            let Some(column) = metadata.column_by_id(*id) else {
                return Ok(None);
            };
            if !column.column_schema.data_type.is_string() {
                return Ok(None);
            }
            return column
                .column_schema
                .fulltext_options()
                .with_context(|_| IndexOptionsSnafu {
                    column_name: &column.column_schema.name,
                })
                .map(|options| options.filter(|options| options.enable));
        }
        IndexTarget::JsonPath(path) => path,
    };
    let Some(column) = metadata.column_by_id(path.column_id()) else {
        return Ok(None);
    };
    if !column.column_schema.data_type.is_json2() {
        return Ok(None);
    }
    let Some(extension) = column
        .column_schema
        .extension_type::<Json2ExtensionType>()
        .with_context(|_| IndexOptionsSnafu {
            column_name: &column.column_schema.name,
        })?
    else {
        return Ok(None);
    };
    Ok(extension
        .metadata()
        .json_settings()
        .type_hints()
        .iter()
        .find(|hint| hint.path == path.path() && hint.data_type.is_string())
        .and_then(|hint| hint.fulltext_index.clone())
        .filter(|options| options.enable))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{Int64Array, NullArray};
    use datatypes::arrow::buffer::NullBuffer;
    use datatypes::arrow::datatypes::Field;
    use datatypes::schema::{ColumnSchema, FulltextOptions};
    use datatypes::value::Value;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    #[test]
    fn test_rebuild_hints_require_a_matching_physical_leaf() {
        use arrow_schema::{DataType as ArrowType, Schema};
        use datatypes::extension::json::JsonMetadata;
        use datatypes::json::JsonSettings;
        use datatypes::types::json_type::JsonNativeType;

        let path_hint = |parent: &str| JsonTypeHint {
            path: vec![parent.into(), "leaf".into()],
            data_type: ConcreteDataType::int64_datatype(),
            inverted_index: true,
            skipping_index: None,
            fulltext_index: None,
        };
        let hints = ["valid", "missing", "wrong_type", "scalar_parent"].map(path_hint);
        let mut metadata = crate::test_util::sst_util::sst_region_metadata();
        let column = &mut metadata.column_metadatas[2];
        column.column_schema.data_type = ConcreteDataType::json2(JsonNativeType::object());
        column
            .column_schema
            .with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
                JsonSettings::try_new(hints.to_vec(), Some(0)).unwrap(),
            ))));
        let nested = |key: &str, ty| ArrowType::Struct(vec![Field::new(key, ty, true)].into());
        let root = ArrowType::Struct(
            vec![
                Field::new("valid", nested("leaf", ArrowType::Int64), true),
                Field::new("missing", nested("other", ArrowType::Int64), true),
                Field::new("wrong_type", nested("leaf", ArrowType::Boolean), true),
                Field::new("scalar_parent", ArrowType::Int64, true),
            ]
            .into(),
        );
        let schema = Schema::new(vec![Field::new(&column.column_schema.name, root, true)]);
        let selected = json_index_hints(
            &metadata.column_metadatas[2],
            &IndexBuildType::Manual,
            Some(JsonIndexRebuildSource::Sst(&metadata, &schema)),
            IndexKind::Inverted,
        )
        .unwrap();
        assert_eq!(selected, [path_hint("valid")]);
    }

    #[test]
    fn test_fulltext_target_options_requires_string_column() {
        let options = FulltextOptions {
            enable: true,
            ..Default::default()
        };
        for data_type in [
            ConcreteDataType::string_datatype(),
            ConcreteDataType::int64_datatype(),
        ] {
            let expected = data_type.is_string().then_some(options.clone());
            let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("value", data_type, true)
                    .with_fulltext_options(options.clone())
                    .unwrap(),
                column_id: 1,
                semantic_type: SemanticType::Field,
            });
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                column_id: 2,
                semantic_type: SemanticType::Timestamp,
            });
            let metadata = builder.build().unwrap();
            assert_eq!(
                fulltext_target_options(&metadata, &IndexTarget::ColumnId(1)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn test_extract_json_type_hint_values_null_type() {
        let nulls: ArrayRef = Arc::new(NullArray::new(2));
        let root: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("a", nulls.data_type().clone(), true)].into(),
            vec![nulls],
            None,
        ));
        // A null leaf can be extracted, but cannot be traversed as an object.
        assert!(extract_json_type_hint_values(&root, &["a".into(), "b".into()]).is_err());
        let leaf = extract_json_type_hint_values(&root, &["a".into()]).unwrap();
        assert_eq!(leaf.len(), 2);
        assert_eq!(leaf.get(0), Value::Null);
        assert_eq!(leaf.get(1), Value::Null);

        let values: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
        let root: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("a", values.data_type().clone(), true)].into(),
            vec![values],
            None,
        ));
        let leaf = extract_json_type_hint_values(&root, &["a".into()]).unwrap();
        assert_eq!(leaf.len(), 2);
        assert_eq!(leaf.get(0), Value::Null);
        assert_eq!(leaf.get(1), Value::Null);
    }

    #[test]
    fn test_extract_json_type_hint_values_nulls_and_slices() {
        let child = Arc::new(Int64Array::from(vec![Some(11), None, None, None, Some(55)]));
        let nested = StructArray::new(
            vec![Field::new("a.b", child.data_type().clone(), true)].into(),
            vec![child],
            Some(NullBuffer::from(vec![true, false, false, true, true])),
        );
        let root: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("nested", nested.data_type().clone(), true)].into(),
            vec![Arc::new(nested)],
            Some(NullBuffer::from(vec![true, true, false, true, true])),
        ));
        let path = vec!["nested".into(), "a.b".into()];
        let leaf = extract_json_type_hint_values(&root, &path).unwrap();
        assert_eq!(
            (0..leaf.len()).map(|i| leaf.get(i)).collect::<Vec<_>>(),
            vec![
                Value::Int64(11),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int64(55)
            ]
        );
        let slice = extract_json_type_hint_values(&root.slice(1, 4), &path).unwrap();
        assert_eq!(
            (0..slice.len()).map(|i| slice.get(i)).collect::<Vec<_>>(),
            vec![Value::Null, Value::Null, Value::Null, Value::Int64(55)]
        );
        assert!(extract_json_type_hint_values(&root, &["missing".into()]).is_err());
        assert!(
            extract_json_type_hint_values(&root, &["nested".into(), "a.b".into(), "child".into()])
                .is_err()
        );
    }
}
