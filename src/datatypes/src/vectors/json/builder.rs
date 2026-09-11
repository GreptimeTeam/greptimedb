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

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_schema::DataType;
use parquet_variant_compute::VariantArrayBuilder;
use snafu::{ResultExt, ensure};

use crate::data_type::ConcreteDataType;
use crate::error::{
    ArrowComputeSnafu, Result, TryFromValueSnafu, UnexpectedSnafu, UnimplementedSnafu,
    UnsupportedOperationSnafu,
};
use crate::extension::json::JSON2_REMAINDER_FIELD_NAME;
use crate::json::value::{JsonNumber, JsonVariant, JsonVariantRef, encode_json_variant};
use crate::json::{
    JSON2_DEFAULT_MAX_AUTO_EXPANDED_PATHS, JSON2_MAX_STRUCTURED_DEPTH, JsonSettings,
};
use crate::prelude::{ValueRef, Vector, VectorRef};
use crate::types::StructType;
use crate::types::json_type::{JsonNativeType, is_include};
use crate::value::{ListValue, ListValueRef, StructValue, StructValueRef, Value};
use crate::vectors::json::variant::{append_json_variant, append_json_variant_ref, variant_field};
use crate::vectors::{Helper, MutableVector, NullVector, StructVectorBuilder};

type JsonObjectValue = BTreeMap<String, JsonVariant>;

/// Builds JSON2 vectors from object values.
///
/// Legacy mode merges all observed paths into the explicit Struct schema.
/// Auto-expanding mode always materializes type-hinted paths, selects up to
/// `max_auto_expanded_paths` compatible unhinted leaf paths by frequency, and stores
/// conflicting or unselected paths in the Variant remainder field.
pub(crate) struct JsonVectorBuilder {
    state: JsonVectorBuilderState,
}

enum JsonVectorBuilderState {
    Legacy {
        merged_type: JsonNativeType,
        values: Vec<JsonVariant>,
    },
    ExplicitOnly {
        /// Paths declared by type hints and stored as dedicated Struct fields.
        explicit_type: JsonNativeType,
        /// Concrete Struct type used to append explicit values without buffering rows.
        struct_type: StructType,
        /// Builder for values selected by the explicit type hints.
        explicit: StructVectorBuilder,
        /// Builder for all values outside the explicit type hints.
        remainder: VariantArrayBuilder,
    },
    AutoExpanding {
        /// Paths declared by type hints and always stored as dedicated Struct fields.
        explicit_type: JsonNativeType,
        /// Maximum number of additional paths selected from buffered values.
        max_auto_expanded_paths: u32,
        /// Buffered values used to infer auto-expanded paths before building the vector.
        values: Vec<JsonVariant>,
    },
}

impl JsonVectorBuilderState {
    fn native_type(&self) -> JsonNativeType {
        match self {
            Self::Legacy { merged_type, .. } => merged_type.clone(),
            Self::ExplicitOnly { explicit_type, .. } => explicit_type.clone(),
            Self::AutoExpanding {
                explicit_type,
                max_auto_expanded_paths,
                values,
            } => infer_expanded_type(explicit_type, *max_auto_expanded_paths, values),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Legacy { values, .. } | Self::AutoExpanding { values, .. } => values.len(),
            Self::ExplicitOnly { explicit, .. } => explicit.len(),
        }
    }

    fn try_build(&mut self) -> Result<VectorRef> {
        match self {
            Self::Legacy {
                merged_type,
                values,
            } => build_legacy(values, merged_type),
            Self::ExplicitOnly {
                explicit,
                remainder,
                ..
            } => {
                let remainder = std::mem::replace(remainder, VariantArrayBuilder::new(0)).build();
                finish_vector(explicit.to_vector(), ArrayRef::from(remainder))
            }
            Self::AutoExpanding {
                explicit_type,
                max_auto_expanded_paths,
                values,
            } => {
                let expanded_type =
                    infer_expanded_type(explicit_type, *max_auto_expanded_paths, values);
                build_with_remainder(values, &expanded_type)
            }
        }
    }

    fn try_build_cloned(&self) -> Result<VectorRef> {
        let mut state = match self {
            Self::Legacy {
                merged_type,
                values,
            } => Self::Legacy {
                merged_type: merged_type.clone(),
                values: values.clone(),
            },
            Self::AutoExpanding {
                explicit_type,
                max_auto_expanded_paths,
                values,
            } => Self::AutoExpanding {
                explicit_type: explicit_type.clone(),
                max_auto_expanded_paths: *max_auto_expanded_paths,
                values: values.clone(),
            },
            // Only TimeSeriesMemtable requires a non-consuming snapshot, while JSON2 targets
            // BulkMemtable. We've tried our best to support it above, but if this match arm does
            // not, it's OK. The only reason it doesn't is because of `VariantArrayBuilder`. We'll
            // track the upstream and see.
            Self::ExplicitOnly { .. } => {
                return UnimplementedSnafu {
                    feat: "no auto expanded JSON2 array builder",
                }
                .fail();
            }
        };
        state.try_build()
    }

    fn try_push_value_ref(&mut self, value: &ValueRef) -> Result<()> {
        if matches!(value, ValueRef::Null) {
            self.push_null();
            return Ok(());
        }
        let ValueRef::Json(value) = value else {
            return TryFromValueSnafu {
                reason: format!("expected JSON value, got {value:?}"),
            }
            .fail();
        };
        ensure!(
            value.is_object() || value.is_null(),
            TryFromValueSnafu {
                reason: format!("expected JSON object value, got {value:?}"),
            }
        );
        match self {
            Self::Legacy {
                merged_type,
                values,
            } => {
                let json_type = value.json_type();
                if !is_include(merged_type, json_type.as_ref()) {
                    merged_type.merge(json_type.as_ref());
                }
                values.push(JsonVariant::from(value.variant()));
            }
            Self::ExplicitOnly {
                explicit_type,
                struct_type,
                explicit,
                remainder,
            } => {
                if value.is_null() {
                    explicit.push_null();
                    remainder.append_null();
                } else {
                    let (value, rest) =
                        split_to_explicit_ref(value.variant(), explicit_type, struct_type)?;
                    explicit.push_struct_value_ref(value)?;
                    append_json_variant_ref(remainder, &rest).context(ArrowComputeSnafu)?;
                }
            }
            Self::AutoExpanding { values, .. } => {
                values.push(JsonVariant::from(value.variant()));
            }
        }
        Ok(())
    }

    fn push_null(&mut self) {
        match self {
            Self::Legacy { values, .. } | Self::AutoExpanding { values, .. } => {
                values.push(JsonVariant::Null)
            }
            Self::ExplicitOnly {
                explicit,
                remainder,
                ..
            } => {
                explicit.push_null();
                remainder.append_null();
            }
        }
    }
}

/// Returns the fixed v2 Arrow physical type produced from `settings`.
pub fn json2_physical_data_type(settings: &JsonSettings) -> DataType {
    let DataType::Struct(fields) = explicit_type(settings).as_arrow_type() else {
        unreachable!("JSON2 explicit type must map to Arrow Struct")
    };
    let mut fields = fields
        .iter()
        .cloned()
        .chain(std::iter::once(Arc::new(variant_field(
            JSON2_REMAINDER_FIELD_NAME,
            true,
        ))))
        .collect::<Vec<_>>();
    fields.sort_unstable_by(|x, y| x.name().cmp(y.name()));
    DataType::Struct(fields.into())
}

fn explicit_type(settings: &JsonSettings) -> JsonNativeType {
    let mut explicit_type = JsonNativeType::Object(Default::default());
    for hint in settings.type_hints() {
        insert_dynamic_type(&mut explicit_type, &hint.path, (&hint.data_type).into());
    }
    explicit_type
}

impl JsonVectorBuilder {
    /// Creates a builder that merges all observed paths into the explicit schema.
    pub(crate) fn new(initial_native_type: JsonNativeType, capacity: usize) -> Self {
        debug_assert!(matches!(
            initial_native_type,
            JsonNativeType::Object(_) | JsonNativeType::Null
        ));
        Self {
            state: JsonVectorBuilderState::Legacy {
                merged_type: initial_native_type,
                values: Vec::with_capacity(capacity),
            },
        }
    }

    /// Creates a builder bounded by the JSON settings and their type hints.
    pub(crate) fn with_settings(settings: &JsonSettings, capacity: usize) -> Self {
        let explicit_type = explicit_type(settings);
        let state = if settings.max_auto_expanded_paths() == Some(0) {
            let DataType::Struct(fields) = explicit_type.as_arrow_type() else {
                unreachable!("JSON2 explicit type must map to Arrow Struct")
            };
            let struct_type = StructType::from(&fields);
            JsonVectorBuilderState::ExplicitOnly {
                explicit_type,
                explicit: StructVectorBuilder::with_type_and_capacity(
                    struct_type.clone(),
                    capacity,
                ),
                struct_type,
                remainder: VariantArrayBuilder::new(capacity),
            }
        } else {
            JsonVectorBuilderState::AutoExpanding {
                explicit_type,
                max_auto_expanded_paths: settings
                    .max_auto_expanded_paths()
                    .unwrap_or(JSON2_DEFAULT_MAX_AUTO_EXPANDED_PATHS),
                values: Vec::with_capacity(capacity),
            }
        };
        Self { state }
    }

    fn try_build(&mut self) -> Result<VectorRef> {
        self.state.try_build()
    }
}

fn build_legacy(values: &mut Vec<JsonVariant>, merged_type: &JsonNativeType) -> Result<VectorRef> {
    build_explicit(values, merged_type, false, |value| match value {
        JsonVariant::Null => Ok(None),
        JsonVariant::Object(value) => Ok(Some(value)),
        _ => TryFromValueSnafu {
            reason: "expected json object value".to_string(),
        }
        .fail(),
    })
}

fn build_with_remainder(
    values: &mut Vec<JsonVariant>,
    expanded_type: &JsonNativeType,
) -> Result<VectorRef> {
    let mut remainder = VariantArrayBuilder::new(values.len());
    let explicit = build_explicit(values, expanded_type, true, |value| {
        if matches!(value, JsonVariant::Null) {
            remainder.append_null();
            return Ok(None);
        }
        let (value, rest) = split_to_explicit(value, expanded_type)?;
        append_json_variant(&mut remainder, &JsonVariant::Object(rest))
            .context(ArrowComputeSnafu)?;
        Ok(Some(value))
    })?;
    finish_vector(explicit, ArrayRef::from(remainder.build()))
}

fn build_explicit(
    values: &mut Vec<JsonVariant>,
    explicit_type: &JsonNativeType,
    // Temporary compatibility switch for the legacy storage layout. Once JSON2 fully switches to
    // the v2 storage layout, empty objects should always be preserved instead of treated as null.
    preserve_empty_structs: bool,
    mut project: impl FnMut(JsonVariant) -> Result<Option<JsonObjectValue>>,
) -> Result<VectorRef> {
    let DataType::Struct(fields) = explicit_type.as_arrow_type() else {
        return UnexpectedSnafu {
            reason: "merged JSON2 type must map to Arrow Struct in JsonVectorBuilder",
        }
        .fail();
    };
    // TODO(LFC): Direct use Arrow's Struct datatype here.
    let struct_type = StructType::from(&fields);

    let mut builder =
        StructVectorBuilder::with_type_and_capacity(struct_type.clone(), values.len());
    for value in std::mem::take(values) {
        let Some(value) = project(value)? else {
            builder.push_null();
            continue;
        };
        let value =
            json_variant_into_struct_value(value, struct_type.clone(), preserve_empty_structs)?;
        builder.push_struct_value_ref(StructValueRef::Ref(&value))?;
    }
    Ok(builder.to_vector())
}

fn finish_vector(explicit: VectorRef, remainder: ArrayRef) -> Result<VectorRef> {
    let explicit = explicit.to_arrow_array();
    let explicit = explicit.as_struct();
    let mut children = explicit
        .fields()
        .iter()
        .cloned()
        .zip(explicit.columns().iter().cloned())
        .chain(std::iter::once((
            Arc::new(variant_field(JSON2_REMAINDER_FIELD_NAME, true)),
            remainder,
        )))
        .collect::<Vec<_>>();
    children.sort_unstable_by(|(x, _), (y, _)| x.name().cmp(y.name()));
    let (fields, columns): (Vec<_>, Vec<ArrayRef>) = children.into_iter().unzip();
    let array: ArrayRef = Arc::new(StructArray::new(
        fields.into(),
        columns,
        explicit.nulls().cloned(),
    ));
    Helper::try_into_vector(array)
}

fn infer_expanded_type(
    explicit_type: &JsonNativeType,
    max_auto_expanded_paths: u32,
    values: &[JsonVariant],
) -> JsonNativeType {
    if max_auto_expanded_paths == 0 {
        return explicit_type.clone();
    }

    let mut stats = HashMap::new();
    let mut path = Vec::new();
    init_explicit_path_stats(explicit_type, &mut path, &mut stats);
    for value in values {
        count_dynamic_paths(value, &mut path, &mut stats);
    }
    let mut candidates = stats
        .iter()
        // Explicit paths are already in the output schema and do not consume the dynamic
        // expansion budget.
        .filter(|(_, stat)| !stat.is_explicit && stat.is_leaf)
        // Parquet cannot store empty structs, while widening an empty object to a non-empty struct
        // loses its shape. Keep paths containing empty objects in the Variant remainder.
        .filter(|(_, stat)| !stat.contains_empty_object)
        // A leaf is eligible only when both itself and every object prefix have one stable
        // role and type across all observed values.
        .filter(|(path, _)| {
            !(1..=path.len())
                .any(|len| stats.get(&path[..len]).is_some_and(|stats| stats.conflicts))
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable_by(|(x_path, x), (y_path, y)| {
        y.seen_count
            .cmp(&x.seen_count)
            .then_with(|| x_path.cmp(y_path))
    });

    let mut expanded_type = explicit_type.clone();
    for (path, candidate) in candidates
        .into_iter()
        .take(max_auto_expanded_paths as usize)
    {
        insert_dynamic_type(
            &mut expanded_type,
            path,
            candidate.expected_leaf_type.clone(),
        );
    }
    expanded_type
}

/// Aggregated observations for one JSON path.
///
/// Schema inference first seeds the map with explicit paths, then walks all input values once.
/// Objects, including empty objects, are non-leaf paths; every other non-null value is a leaf.
/// The first dynamic observation fixes the path role and exact leaf type. A later role or type
/// mismatch sets [`PathStats::conflicts`] permanently. Missing paths and null values do not affect
/// the statistics.
///
/// After collection, dynamic leaves are ranked by [`PathStats::seen_count`]. A candidate is
/// rejected when it or any parent path conflicts, so candidate selection never rescans the input
/// values.
struct PathStats {
    /// Whether the path came from a type hint and is already part of the output schema.
    is_explicit: bool,
    /// Whether the path is a non-object value rather than an object prefix.
    is_leaf: bool,
    /// Exact type required for a leaf; unused non-leaf paths keep [`JsonNativeType::Null`].
    expected_leaf_type: JsonNativeType,
    /// Number of compatible observations used to rank dynamic leaves.
    seen_count: usize,
    /// Whether any observed value contains an empty object that requires lossless Variant storage.
    contains_empty_object: bool,
    /// Whether the path has ever had inconsistent roles or leaf types.
    conflicts: bool,
}

/// Seeds path statistics from the configured explicit JSON shape.
fn init_explicit_path_stats<'a>(
    explicit_type: &'a JsonNativeType,
    path: &mut Vec<&'a str>,
    stats: &mut HashMap<Vec<&'a str>, PathStats>,
) {
    let JsonNativeType::Object(fields) = explicit_type else {
        return;
    };
    for (name, data_type) in fields {
        path.push(name);
        let is_leaf = !matches!(data_type, JsonNativeType::Object(_));
        let expected_leaf_type = if is_leaf {
            data_type.clone()
        } else {
            JsonNativeType::default()
        };
        stats.insert(
            path.clone(),
            PathStats {
                is_explicit: true,
                is_leaf,
                expected_leaf_type,
                seen_count: 0,
                contains_empty_object: false,
                conflicts: false,
            },
        );
        init_explicit_path_stats(data_type, path, stats);
        path.pop();
    }
}

/// Collects dynamic path statistics while traversing each input value once.
fn count_dynamic_paths<'a>(
    value: &'a JsonVariant,
    path: &mut Vec<&'a str>,
    stats: &mut HashMap<Vec<&'a str>, PathStats>,
) {
    if matches!(value, JsonVariant::Null) || path.len() > JSON2_MAX_STRUCTURED_DEPTH {
        return;
    }

    if !path.is_empty() {
        let is_leaf = !matches!(value, JsonVariant::Object(_));
        let contains_empty_object = is_leaf && value.contains_empty_object();
        let conflicts = if let Some(stats) = stats.get_mut(path.as_slice()) {
            stats.contains_empty_object |= contains_empty_object;
            if !stats.conflicts {
                let role_conflict = stats.is_leaf != is_leaf;
                let type_conflict = || match (&stats.expected_leaf_type, value) {
                    // If both objects, they are compatible.
                    (JsonNativeType::Null | JsonNativeType::Object(_), JsonVariant::Object(_)) => {
                        false
                    }
                    _ => stats.expected_leaf_type != value.native_type(),
                };
                if role_conflict || type_conflict() {
                    stats.conflicts = true;
                } else {
                    stats.seen_count += 1;
                }
            }
            stats.conflicts
        } else {
            let expected_leaf_type = if is_leaf {
                value.native_type()
            } else {
                JsonNativeType::default()
            };
            stats.insert(
                path.clone(),
                PathStats {
                    is_explicit: false,
                    is_leaf,
                    expected_leaf_type,
                    seen_count: 1,
                    contains_empty_object,
                    conflicts: false,
                },
            );
            false
        };
        if conflicts {
            return;
        }
    }

    if let JsonVariant::Object(object) = value
        && !object.is_empty()
    {
        for (name, value) in object {
            path.push(name);
            count_dynamic_paths(value, path, stats);
            path.pop();
        }
    }
}

fn insert_dynamic_type<S: AsRef<str>>(
    explicit_type: &mut JsonNativeType,
    path: &[S],
    data_type: JsonNativeType,
) {
    let JsonNativeType::Object(fields) = explicit_type else {
        return;
    };
    let Some((name, path)) = path.split_first() else {
        return;
    };
    let name = name.as_ref().to_string();
    if path.is_empty() {
        fields.insert(name, data_type);
        return;
    }
    insert_dynamic_type(
        fields
            .entry(name)
            .or_insert_with(|| JsonNativeType::Object(Default::default())),
        path,
        data_type,
    )
}

fn split_to_explicit_ref<'a>(
    value: &JsonVariantRef<'a>,
    explicit_type: &JsonNativeType,
    struct_type: &StructType,
) -> Result<(StructValueRef<'a>, JsonVariantRef<'a>)> {
    let JsonVariantRef::Object(object) = value else {
        return TryFromValueSnafu {
            reason: "expected json object value".to_string(),
        }
        .fail();
    };
    let explicit = json_object_ref_into_struct_value_ref(object, struct_type)?;
    let remainder = remainder_ref(object, explicit_type)?;
    Ok((explicit, JsonVariantRef::Object(remainder)))
}

fn json_object_ref_into_struct_value_ref<'a>(
    object: &BTreeMap<&'a str, JsonVariantRef<'a>>,
    struct_type: &StructType,
) -> Result<StructValueRef<'a>> {
    let mut values = Vec::with_capacity(struct_type.fields().len());
    for field in struct_type.fields().iter() {
        let value = match object.get(field.name()) {
            Some(value) => json_variant_ref_into_value_ref(value, field.data_type())?,
            None => ValueRef::Null,
        };
        values.push(value);
    }
    Ok(StructValueRef::RefList {
        val: values,
        fields: struct_type.clone(),
    })
}

fn json_variant_ref_into_value_ref<'a>(
    value: &JsonVariantRef<'a>,
    expected_type: &ConcreteDataType,
) -> Result<ValueRef<'a>> {
    let value = match (value, expected_type) {
        (JsonVariantRef::Null, _) | (_, ConcreteDataType::Null(_)) => ValueRef::Null,
        (JsonVariantRef::Object(object), ConcreteDataType::Struct(struct_type)) => {
            ValueRef::Struct(json_object_ref_into_struct_value_ref(object, struct_type)?)
        }
        (JsonVariantRef::Bool(x), ConcreteDataType::Boolean(_)) => ValueRef::Boolean(*x),
        (JsonVariantRef::Number(x), ConcreteDataType::UInt64(_)) => {
            let Some(x) = x.as_u64() else {
                return TryFromValueSnafu {
                    reason: format!("unable to convert {x:?} to UInt64"),
                }
                .fail();
            };
            ValueRef::UInt64(x)
        }
        (JsonVariantRef::Number(x), ConcreteDataType::Int64(_)) => {
            let x = match x {
                JsonNumber::PosInt(x) => i64::try_from(*x).ok(),
                JsonNumber::NegInt(x) => Some(*x),
                JsonNumber::Float(_) => None,
            };
            let Some(x) = x else {
                return TryFromValueSnafu {
                    reason: format!("unable to convert {x:?} to Int64"),
                }
                .fail();
            };
            ValueRef::Int64(x)
        }
        (JsonVariantRef::Number(JsonNumber::PosInt(x)), ConcreteDataType::Float64(_)) => {
            ValueRef::Float64((*x as f64).into())
        }
        (JsonVariantRef::Number(JsonNumber::NegInt(x)), ConcreteDataType::Float64(_)) => {
            ValueRef::Float64((*x as f64).into())
        }
        (JsonVariantRef::Number(JsonNumber::Float(x)), ConcreteDataType::Float64(_)) => {
            ValueRef::Float64(*x)
        }
        (JsonVariantRef::String(x), ConcreteDataType::String(_)) => ValueRef::String(x),
        (JsonVariantRef::Array(array), ConcreteDataType::List(list_type)) => {
            let item_type = list_type.item_type().clone();
            let values = array
                .iter()
                .map(|x| json_variant_ref_into_value_ref(x, &item_type))
                .collect::<Result<Vec<_>>>()?;
            ValueRef::List(ListValueRef::RefList {
                val: values,
                item_datatype: Arc::new(item_type),
            })
        }
        (value, expected_type) => {
            return TryFromValueSnafu {
                reason: format!("unable to convert json value {value:?} to {expected_type}"),
            }
            .fail();
        }
    };
    Ok(value)
}

fn remainder_ref<'a>(
    object: &BTreeMap<&'a str, JsonVariantRef<'a>>,
    explicit_type: &JsonNativeType,
) -> Result<BTreeMap<&'a str, JsonVariantRef<'a>>> {
    let JsonNativeType::Object(fields) = explicit_type else {
        return UnexpectedSnafu {
            reason: "JSON2 explicit type must be an object",
        }
        .fail();
    };
    let mut remainder = BTreeMap::new();
    for (&name, value) in object {
        // Preserve explicit JSON nulls in the remainder because Arrow child nulls cannot
        // distinguish a present JSON null from a missing path.
        if *value == JsonVariantRef::Null {
            remainder.insert(name, JsonVariantRef::Null);
            continue;
        }

        match fields.get(name) {
            Some(data_type @ JsonNativeType::Object(_)) => match value {
                JsonVariantRef::Object(object) => {
                    let child = remainder_ref(object, data_type)?;
                    if !child.is_empty() {
                        remainder.insert(name, JsonVariantRef::Object(child));
                    }
                }
                _ => {
                    return TryFromValueSnafu {
                        reason: "expected json object value".to_string(),
                    }
                    .fail();
                }
            },
            // A non-object entry in the explicit type tree is an explicit leaf and is
            // already written to the Struct builder. So here does nothing.
            Some(_) => {}
            None => {
                remainder.insert(name, value.clone());
            }
        }
    }
    Ok(remainder)
}

fn split_to_explicit(
    value: JsonVariant,
    explicit_type: &JsonNativeType,
) -> Result<(JsonObjectValue, JsonObjectValue)> {
    let JsonVariant::Object(mut remainder) = value else {
        return TryFromValueSnafu {
            reason: "expected json object value".to_string(),
        }
        .fail();
    };
    let JsonNativeType::Object(fields) = explicit_type else {
        return UnexpectedSnafu {
            reason: "JSON2 explicit type must be an object",
        }
        .fail();
    };
    let mut explicit = JsonObjectValue::new();

    for (name, data_type) in fields {
        let Some(value) = remainder.remove(name) else {
            continue;
        };
        if value == JsonVariant::Null {
            explicit.insert(name.clone(), JsonVariant::Null);
            // Preserve explicit JSON nulls in the remainder because Arrow child nulls cannot
            // distinguish a present JSON null from a missing path.
            remainder.insert(name.clone(), JsonVariant::Null);
            continue;
        }
        if matches!(data_type, JsonNativeType::Object(_)) {
            let (child_explicit, child_remainder) = split_to_explicit(value, data_type)?;
            explicit.insert(name.clone(), JsonVariant::Object(child_explicit));
            if !child_remainder.is_empty() {
                remainder.insert(name.clone(), JsonVariant::Object(child_remainder));
            }
        } else {
            explicit.insert(name.clone(), value);
        }
    }
    Ok((explicit, remainder))
}

fn json_variant_into_struct_value(
    object: JsonObjectValue,
    struct_type: StructType,
    preserve_empty_structs: bool,
) -> Result<StructValue> {
    let mut entries = object.into_iter();
    let mut entry = entries.next();
    let mut values = Vec::with_capacity(struct_type.fields().len());
    for field in struct_type.fields().iter() {
        let value = match entry.take() {
            Some((name, value)) if name == field.name() => {
                entry = entries.next();
                json_variant_into_value(value, field.data_type(), preserve_empty_structs)?
            }
            Some((name, _)) if name.as_str() < field.name() => {
                return TryFromValueSnafu {
                    reason: format!("field {name} is missing from merged JSON type"),
                }
                .fail();
            }
            next => {
                entry = next;
                Value::Null
            }
        };
        values.push(value);
    }
    if let Some((name, _)) = entry {
        return TryFromValueSnafu {
            reason: format!("field {name} is missing from merged JSON type"),
        }
        .fail();
    }

    Ok(StructValue::new(values, struct_type))
}

fn json_variant_into_value(
    value: JsonVariant,
    expected_type: &ConcreteDataType,
    preserve_empty_structs: bool,
) -> Result<Value> {
    let value = match (value, expected_type) {
        (JsonVariant::Null, _) | (_, ConcreteDataType::Null(_)) => Value::Null,
        (JsonVariant::Object(object), _) if object.is_empty() && !preserve_empty_structs => {
            Value::Null
        }
        (JsonVariant::Object(object), ConcreteDataType::Struct(struct_type)) => Value::Struct(
            json_variant_into_struct_value(object, struct_type.clone(), preserve_empty_structs)?,
        ),
        (JsonVariant::Bool(x), ConcreteDataType::Boolean(_)) => Value::Boolean(x),
        (JsonVariant::Number(x), ConcreteDataType::UInt64(_)) => {
            let Some(x) = x.as_u64() else {
                return TryFromValueSnafu {
                    reason: format!("unable to convert {x:?} to UInt64"),
                }
                .fail();
            };
            Value::UInt64(x)
        }
        (JsonVariant::Number(x), ConcreteDataType::Int64(_)) => {
            let x = match x {
                JsonNumber::PosInt(x) => i64::try_from(x).ok(),
                JsonNumber::NegInt(x) => Some(x),
                JsonNumber::Float(_) => None,
            };
            let Some(x) = x else {
                return TryFromValueSnafu {
                    reason: format!("unable to convert {x:?} to Int64"),
                }
                .fail();
            };
            Value::Int64(x)
        }
        (JsonVariant::Number(JsonNumber::PosInt(x)), ConcreteDataType::Float64(_)) => {
            Value::Float64((x as f64).into())
        }
        (JsonVariant::Number(JsonNumber::NegInt(x)), ConcreteDataType::Float64(_)) => {
            Value::Float64((x as f64).into())
        }
        (JsonVariant::Number(JsonNumber::Float(x)), ConcreteDataType::Float64(_)) => {
            Value::Float64(x)
        }
        (JsonVariant::String(x), ConcreteDataType::String(_)) => Value::String(x.into()),
        (JsonVariant::Array(array), ConcreteDataType::List(list_type)) => {
            let item_type = list_type.item_type().clone();
            let values = array
                .into_iter()
                .map(|v| json_variant_into_value(v, &item_type, preserve_empty_structs))
                .collect::<Result<Vec<_>>>()?;
            Value::List(ListValue::new(values, Arc::new(item_type)))
        }
        (value, ConcreteDataType::Binary(_)) => Value::from(encode_json_variant(value)?),
        (value, expected_type) => {
            return TryFromValueSnafu {
                reason: format!("unable to convert json value {value:?} to {expected_type}"),
            }
            .fail();
        }
    };
    Ok(value)
}

impl MutableVector for JsonVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::json2(self.state.native_type())
    }

    fn len(&self) -> usize {
        self.state.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn to_vector(&mut self) -> VectorRef {
        self.try_build().unwrap_or_else(|e| {
            // Just try to avoid panicking here.
            common_telemetry::error!(e; "Unable to build JSON2 vector");
            Arc::new(NullVector::new(self.len()))
        })
    }

    fn to_vector_cloned(&self) -> VectorRef {
        self.state.try_build_cloned().unwrap_or_else(|e| {
            // Just try to avoid panicking here.
            common_telemetry::error!(e; "Unable to build JSON2 vector");
            Arc::new(NullVector::new(self.len()))
        })
    }

    fn try_push_value_ref(&mut self, value: &ValueRef) -> Result<()> {
        self.state.try_push_value_ref(value)
    }

    fn push_null(&mut self) {
        self.state.push_null()
    }

    fn extend_slice_of(&mut self, _: &dyn Vector, _: usize, _: usize) -> Result<()> {
        UnsupportedOperationSnafu {
            op: "extend_slice_of",
            vector_type: "JsonVector",
        }
        .fail()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_schema::Field;
    use common_base::bytes::Bytes;
    use serde_json::json;

    use super::*;
    use crate::data_type::ConcreteDataType;
    use crate::extension::json::{Json2ExtensionType, JsonMetadata};
    use crate::json::JsonTypeHint;
    use crate::json::value::decode_json_variant;
    use crate::types::StructField;
    use crate::types::json_type::JsonObjectType;
    use crate::value::{ListValue, StructValue, Value, ValueRef};
    use crate::vectors::json::array::JsonArray;
    use crate::vectors::json::variant::variant_to_json_values;

    #[test]
    fn test_json_vector_builder() -> Result<()> {
        fn parse_json_value(json: &str) -> Value {
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            Value::Json(Box::new(value.into()))
        }

        fn jsonb_bytes(json: &str) -> Bytes {
            Bytes::from(jsonb::parse_value(json.as_bytes()).unwrap().to_vec())
        }

        // Object inputs should merge into a superset schema, preserve null rows,
        // and project conflicting nested values into Variant payloads.
        let mut builder = JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 3);
        let first = parse_json_value(r#"{"id":1,"payload":{"name":"foo"}}"#);
        let second = parse_json_value(r#"{"id":2,"extra":true,"payload":"raw"}"#);
        builder.try_push_value_ref(&first.as_value_ref())?;
        builder.push_null();
        builder.try_push_value_ref(&second.as_value_ref())?;

        let merged_type = JsonNativeType::Object(JsonObjectType::from([
            ("extra".to_string(), JsonNativeType::Bool),
            ("id".to_string(), JsonNativeType::i64()),
            ("payload".to_string(), JsonNativeType::Variant),
        ]));
        assert_eq!(
            builder.data_type(),
            ConcreteDataType::json2(merged_type.clone())
        );

        let DataType::Struct(fields) = merged_type.as_arrow_type() else {
            unreachable!()
        };
        let merged_struct_type = StructType::from(&fields);
        let vector = builder.to_vector();
        assert_eq!(vector.len(), 3);
        assert_eq!(
            vector.get(0),
            Value::Struct(StructValue::new(
                vec![
                    Value::Null,
                    Value::Int64(1),
                    Value::Binary(jsonb_bytes(r#"{"name":"foo"}"#)),
                ],
                merged_struct_type.clone(),
            ))
        );
        assert_eq!(vector.get(1), Value::Null);
        assert_eq!(
            vector.get(2),
            Value::Struct(StructValue::new(
                vec![
                    Value::Boolean(true),
                    Value::Int64(2),
                    Value::Binary(jsonb_bytes(r#""raw""#)),
                ],
                merged_struct_type,
            ))
        );

        // A Null initial type represents an unknown JSON2 runtime type. The first
        // non-null value should set the concrete type instead of aligning all rows to Null.
        let mut inferred_builder = JsonVectorBuilder::new(JsonNativeType::Null, 2);
        let inferred_value = parse_json_value(r#"{"id":3}"#);
        inferred_builder.push_null();
        inferred_builder.try_push_value_ref(&inferred_value.as_value_ref())?;

        let inferred_type = JsonNativeType::Object(JsonObjectType::from([(
            "id".to_string(),
            JsonNativeType::i64(),
        )]));
        assert_eq!(
            inferred_builder.data_type(),
            ConcreteDataType::json2(inferred_type.clone())
        );

        let DataType::Struct(fields) = inferred_type.as_arrow_type() else {
            unreachable!()
        };
        let inferred_struct_type = StructType::from(&fields);
        let vector = inferred_builder.to_vector();
        assert_eq!(vector.get(0), Value::Null);
        assert_eq!(
            vector.get(1),
            Value::Struct(StructValue::new(
                vec![Value::Int64(3)],
                inferred_struct_type,
            ))
        );

        // Non-object initial types are rejected by the builder invariant.
        let result = std::panic::catch_unwind(|| JsonVectorBuilder::new(JsonNativeType::Bool, 2));
        assert!(result.is_err());

        // Non-object root values should be rejected at push time.
        let mut object_builder =
            JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 2);
        let object = parse_json_value(r#"{"k":1}"#);
        let boolean = parse_json_value("true");
        let err = object_builder
            .try_push_value_ref(&boolean.as_value_ref())
            .unwrap_err();
        assert!(err.to_string().contains("expected JSON object value"));
        object_builder.try_push_value_ref(&object.as_value_ref())?;

        // Non-JSON values should be rejected at push time.
        let mut invalid_builder =
            JsonVectorBuilder::new(JsonNativeType::Object(Default::default()), 1);
        let err = invalid_builder
            .try_push_value_ref(&ValueRef::Boolean(true))
            .unwrap_err();
        assert!(err.to_string().contains("expected JSON value"));

        Ok(())
    }

    #[test]
    fn test_zero_budget_builder_uses_explicit_only_schema_and_remainder() -> Result<()> {
        let settings = JsonSettings::try_new(
            vec![
                JsonTypeHint {
                    path: vec!["kind".to_string()],
                    data_type: ConcreteDataType::string_datatype(),
                    nullable: true,
                    default_constraint: None,
                    inverted_index: false,
                },
                JsonTypeHint {
                    path: vec!["commit".to_string(), "operation".to_string()],
                    data_type: ConcreteDataType::string_datatype(),
                    nullable: true,
                    default_constraint: None,
                    inverted_index: false,
                },
                JsonTypeHint {
                    path: vec!["time_us".to_string()],
                    data_type: ConcreteDataType::int64_datatype(),
                    nullable: true,
                    default_constraint: None,
                    inverted_index: false,
                },
            ],
            Some(0),
        )?;
        let mut builder = JsonVectorBuilder::with_settings(&settings, 2);
        assert!(matches!(
            &builder.state,
            JsonVectorBuilderState::ExplicitOnly { .. }
        ));
        let values = [
            json!({
                "kind": "record",
                "commit": {"operation": "create", "collection": "post"},
                "extra": 1,
                "time_us": 1
            }),
            json!({"kind": "other", "dynamic": true, "time_us": 2}),
        ];
        for value in values.clone() {
            let value = settings.encode(value)?;
            builder.try_push_value_ref(&value.as_value_ref())?;
        }
        let array = builder.to_vector().to_arrow_array();
        assert_eq!(&json2_physical_data_type(&settings), array.data_type());
        assert_eq!(0, builder.len());
        let structs = array.as_struct();
        assert_eq!(
            vec![JSON2_REMAINDER_FIELD_NAME, "commit", "kind", "time_us"],
            structs
                .fields()
                .iter()
                .map(|x| x.name().as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![
                Some(json!({"commit": {"collection": "post"}, "extra": 1})),
                Some(json!({"commit": {"operation": null}, "dynamic": true})),
            ],
            variant_to_json_values(structs.column_by_name(JSON2_REMAINDER_FIELD_NAME).unwrap())?
        );

        let field = Field::new("data", array.data_type().clone(), true).with_extension_type(
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(settings))),
        );
        let reconstructed = JsonArray::from(&array).project_to_v2(&field, &DataType::Binary)?;
        let reconstructed = reconstructed.as_binary::<i32>();
        assert_eq!(
            values[0],
            decode_json_variant(reconstructed.value(0)).unwrap()
        );
        assert_eq!(
            json!({
                "kind": "other",
                "commit": {"operation": null},
                "dynamic": true,
                "time_us": 2
            }),
            decode_json_variant(reconstructed.value(1)).unwrap()
        );

        let settings = JsonSettings::try_new(vec![], Some(0))?;
        let mut builder = JsonVectorBuilder::with_settings(&settings, 3);
        for value in [json!({}), json!({"x": 1})] {
            let value = settings.encode(value)?;
            builder.try_push_value_ref(&value.as_value_ref())?;
        }
        builder.push_null();
        let array = builder.to_vector().to_arrow_array();
        let structs = array.as_struct();
        assert_eq!(1, structs.num_columns());
        assert_eq!(
            vec![Some(json!({})), Some(json!({"x": 1})), None],
            variant_to_json_values(structs.column(0))?
        );

        Ok(())
    }

    #[test]
    fn test_finite_budget_selects_dynamic_paths() -> Result<()> {
        let builder = JsonVectorBuilder::with_settings(&JsonSettings::default(), 0);
        assert!(matches!(
            builder.state,
            JsonVectorBuilderState::AutoExpanding {
                max_auto_expanded_paths: JSON2_DEFAULT_MAX_AUTO_EXPANDED_PATHS,
                ..
            }
        ));

        let settings = JsonSettings::try_new(
            vec![JsonTypeHint {
                path: vec!["hint".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: false,
            }],
            Some(2),
        )?;
        let values = [
            json!({
                "hint": "first",
                "conflict": 1,
                "popular": {"nested": 1},
                "tie_a": "a",
                "tie_b": true,
                "rare": 1
            }),
            json!({
                "hint": "second",
                "conflict": "string",
                "popular": {"nested": 2},
                "tie_a": "b",
                "tie_b": false
            }),
            json!({"hint": "third", "popular": "scalar"}),
        ];
        let mut builder = JsonVectorBuilder::with_settings(&settings, values.len());
        for value in values.clone() {
            let value = settings.encode(value)?;
            builder.try_push_value_ref(&value.as_value_ref())?;
        }

        let array = builder.to_vector().to_arrow_array();
        let structs = array.as_struct();
        assert_eq!(
            vec![JSON2_REMAINDER_FIELD_NAME, "hint", "tie_a", "tie_b"],
            structs
                .fields()
                .iter()
                .map(|x| x.name().as_str())
                .collect::<Vec<_>>()
        );
        assert!(structs.column_by_name("popular").is_none());
        assert_eq!(
            vec![
                Some(json!({"conflict": 1, "popular": {"nested": 1}, "rare": 1})),
                Some(json!({"conflict": "string", "popular": {"nested": 2}})),
                Some(json!({"popular": "scalar"})),
            ],
            variant_to_json_values(structs.column_by_name(JSON2_REMAINDER_FIELD_NAME).unwrap())?
        );

        let field = Field::new("data", array.data_type().clone(), true).with_extension_type(
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(settings))),
        );
        let reconstructed = JsonArray::from(&array).project_to_v2(&field, &DataType::Binary)?;
        let reconstructed = reconstructed.as_binary::<i32>();
        assert_eq!(
            values[0],
            decode_json_variant(reconstructed.value(0)).unwrap()
        );
        assert_eq!(
            values[1],
            decode_json_variant(reconstructed.value(1)).unwrap()
        );
        assert_eq!(
            json!({
                "hint": "third",
                "popular": "scalar"
            }),
            decode_json_variant(reconstructed.value(2)).unwrap()
        );

        Ok(())
    }

    #[test]
    fn test_v2_builder_preserves_explicit_null_presence() -> Result<()> {
        let settings = JsonSettings::try_new(vec![], Some(1))?;
        let values = [json!({"value": 1}), json!({"value": null}), json!({})];
        let mut builder = JsonVectorBuilder::with_settings(&settings, values.len());
        for value in values.clone() {
            let value = settings.encode(value)?;
            builder.try_push_value_ref(&value.as_value_ref())?;
        }
        let array = builder.to_vector().to_arrow_array();
        let structs = array.as_struct();
        assert_eq!(
            vec![
                Some(json!({})),
                Some(json!({"value": null})),
                Some(json!({}))
            ],
            variant_to_json_values(structs.column_by_name(JSON2_REMAINDER_FIELD_NAME).unwrap())?
        );
        let field = Field::new("data", array.data_type().clone(), true).with_extension_type(
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(settings))),
        );
        let reconstructed = JsonArray::from(&array).project_to_v2(&field, &DataType::Binary)?;
        let reconstructed = reconstructed.as_binary::<i32>();

        assert_eq!(
            values[0],
            decode_json_variant(reconstructed.value(0)).unwrap()
        );
        assert_eq!(
            values[1],
            decode_json_variant(reconstructed.value(1)).unwrap()
        );
        assert_eq!(
            values[2],
            decode_json_variant(reconstructed.value(2)).unwrap()
        );
        Ok(())
    }

    #[test]
    fn test_v2_builder_accepts_sql_null() -> Result<()> {
        let settings = JsonSettings::try_new(vec![], Some(1))?;
        let mut builder = JsonVectorBuilder::with_settings(&settings, 2);
        builder.try_push_value_ref(&ValueRef::Null)?;
        let value = settings.encode(json!({}))?;
        builder.try_push_value_ref(&value.as_value_ref())?;

        let array = builder.to_vector().to_arrow_array();
        let structs = array.as_struct();
        assert_eq!(
            vec![None, Some(json!({}))],
            variant_to_json_values(structs.column_by_name(JSON2_REMAINDER_FIELD_NAME).unwrap())?
        );
        Ok(())
    }

    #[test]
    fn test_reconstruct_nested_remainder_only_value() -> Result<()> {
        let settings = JsonSettings::try_new(vec![], Some(1))?;
        let values = [
            json!({"a": {"hot": 1}}),
            json!({"a": {"hot": 2}}),
            json!({"a": {"cold": 3}}),
        ];
        let mut builder = JsonVectorBuilder::with_settings(&settings, values.len());
        for value in values.clone() {
            let value = settings.encode(value)?;
            builder.try_push_value_ref(&value.as_value_ref())?;
        }

        let array = builder.to_vector().to_arrow_array();
        let field = Field::new("data", array.data_type().clone(), true).with_extension_type(
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(settings))),
        );
        let reconstructed = JsonArray::from(&array).project_to_v2(&field, &DataType::Binary)?;
        let reconstructed = reconstructed.as_binary::<i32>();
        assert_eq!(
            vec![values[0].clone(), values[1].clone(), values[2].clone()],
            (0..reconstructed.len())
                .map(|i| decode_json_variant(reconstructed.value(i)).unwrap())
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[test]
    fn test_dynamic_paths_require_the_same_leaf_type() -> Result<()> {
        let settings = JsonSettings::try_new(
            vec![JsonTypeHint {
                path: vec!["nested".to_string(), "hinted".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: false,
            }],
            Some(8),
        )?;
        let values = [
            json!({
                "branch": {},
                "different": [1],
                "empty": {},
                "nested": {},
                "reverse": "scalar",
                "same": [1]
            }),
            json!({
                "branch": {"leaf": 1},
                "different": ["x"],
                "empty": {},
                "nested": {"hinted": "x", "leaf": 1},
                "same": [2]
            }),
            json!({
                "branch": {},
                "different": [2],
                "empty": {},
                "nested": {},
                "reverse": {"leaf": 1},
                "same": [3]
            }),
        ];
        let mut builder = JsonVectorBuilder::with_settings(&settings, values.len());
        for value in values {
            let value = settings.encode(value)?;
            builder.try_push_value_ref(&value.as_value_ref())?;
        }

        let JsonNativeType::Object(fields) = builder.state.native_type() else {
            unreachable!();
        };
        assert!(!fields.contains_key("different"));
        assert!(!fields.contains_key("empty"));
        assert!(!fields.contains_key("reverse"));
        assert!(matches!(fields.get("same"), Some(JsonNativeType::Array(_))));
        assert!(matches!(
            fields.get("branch"),
            Some(JsonNativeType::Object(fields)) if fields.contains_key("leaf")
        ));
        assert!(matches!(
            fields.get("nested"),
            Some(JsonNativeType::Object(fields))
                if fields.contains_key("hinted") && fields.contains_key("leaf")
        ));

        Ok(())
    }

    #[test]
    fn test_json_variant_into_struct_value() -> Result<()> {
        let struct_type = StructType::new(Arc::new(vec![StructField::new(
            "value".to_string(),
            ConcreteDataType::string_datatype(),
            true,
        )]));
        assert_eq!(
            json_variant_into_value(
                JsonVariant::Object(Default::default()),
                &ConcreteDataType::struct_datatype(struct_type.clone()),
                false,
            )?,
            Value::Null
        );
        assert_eq!(
            json_variant_into_value(
                JsonVariant::Object(Default::default()),
                &ConcreteDataType::struct_datatype(struct_type.clone()),
                true,
            )?,
            Value::Struct(StructValue::new(vec![Value::Null], struct_type))
        );

        let item_type =
            ConcreteDataType::struct_datatype(StructType::new(Arc::new(vec![StructField::new(
                "id".to_string(),
                ConcreteDataType::int64_datatype(),
                true,
            )])));
        let struct_type = StructType::new(Arc::new(vec![
            StructField::new(
                "items".to_string(),
                ConcreteDataType::list_datatype(Arc::new(item_type.clone())),
                true,
            ),
            StructField::new(
                "meta".to_string(),
                ConcreteDataType::struct_datatype(StructType::new(Arc::new(vec![
                    StructField::new(
                        "name".to_string(),
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                ]))),
                true,
            ),
        ]));
        let variant = JsonObjectValue::from([
            (
                "items".to_string(),
                JsonVariant::Array(vec![
                    JsonVariant::from([("id", JsonVariant::from(1i64))]),
                    JsonVariant::from([("id", JsonVariant::from(2i64))]),
                ]),
            ),
            (
                "meta".to_string(),
                JsonVariant::from([("name", JsonVariant::from("foo"))]),
            ),
        ]);
        let value = Value::Struct(json_variant_into_struct_value(
            variant,
            struct_type.clone(),
            true,
        )?);

        assert_eq!(
            value,
            Value::Struct(StructValue::new(
                vec![
                    Value::List(ListValue::new(
                        vec![
                            Value::Struct(StructValue::new(
                                vec![Value::Int64(1)],
                                StructType::new(Arc::new(vec![StructField::new(
                                    "id".to_string(),
                                    ConcreteDataType::int64_datatype(),
                                    true,
                                )]))
                            )),
                            Value::Struct(StructValue::new(
                                vec![Value::Int64(2)],
                                StructType::new(Arc::new(vec![StructField::new(
                                    "id".to_string(),
                                    ConcreteDataType::int64_datatype(),
                                    true,
                                )]))
                            )),
                        ],
                        Arc::new(item_type),
                    )),
                    Value::Struct(StructValue::new(
                        vec![Value::String("foo".into())],
                        StructType::new(Arc::new(vec![StructField::new(
                            "name".to_string(),
                            ConcreteDataType::string_datatype(),
                            true,
                        )])),
                    )),
                ],
                struct_type,
            ))
        );
        Ok(())
    }
}
