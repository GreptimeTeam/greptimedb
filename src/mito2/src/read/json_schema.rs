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

//! JSON2 schema policy used by the read path.
//!
//! JSON2 schemas used by reads are inferred from query expressions. For example,
//! `json_get(j, "a.b")` gives us a concrete schema for the requested paths, so
//! the reader can prune nested fields and align the output array to that schema.
//!
//! Root JSON2 reads are different: the query can only say that the whole JSON2
//! column should be read, but it cannot provide a concrete root schema. The
//! read path must infer the root schema from the merged source schemas before
//! building output plans. The root read still reads the whole JSON2 column, but
//! later stages use the inferred schema to keep batch schemas consistent.

pub(crate) mod align;

use std::collections::HashMap;

use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::error::{Result as DataTypeResult, UnsupportedOperationSnafu};
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::types::json_type::JsonNativeType;
use datatypes::vectors::json::array::JsonArray;
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;
use store_api::storage::{JsonReadHint, JsonRootReadHint, NestedPath};

use crate::error::{DataTypeMismatchSnafu, Result};
use crate::read::read_columns::{ReadColumns, merge_nested_paths};

/// Applies JSON2 read hints to the nested paths in read columns.
///
/// Behavior by hint:
///
/// - [`JsonReadHint::Root`] reads the whole JSON2 column, so this function
///   clears nested path pruning for that column.
/// - [`JsonReadHint::Paths`] narrows the read to the leaf paths described by
///   the hinted JSON type.
pub(crate) fn apply_json_read_hints_to_read_columns(
    read_cols: &mut ReadColumns,
    json_type_hint: &HashMap<String, JsonReadHint>,
    metadata: &RegionMetadata,
) {
    if json_type_hint.is_empty() {
        return;
    }

    for read_col in &mut read_cols.cols {
        let Some(col) = metadata.column_by_id(read_col.column_id) else {
            continue;
        };
        let col_name = &col.column_schema.name;
        let Some(json_type) = json_type_hint.get(col_name) else {
            continue;
        };

        let JsonReadHint::Paths(json_type) = json_type else {
            read_col.nested_paths.clear();
            continue;
        };

        let mut paths = Vec::new();
        let mut current = vec![col_name.clone()];
        collect_json_nested_paths(json_type, &mut current, &mut paths);
        merge_nested_paths(&mut read_col.nested_paths, paths)
    }
}

fn collect_json_nested_paths(
    json_type: &JsonNativeType,
    current: &mut NestedPath,
    paths: &mut Vec<NestedPath>,
) {
    match json_type {
        JsonNativeType::Object(fields) if !fields.is_empty() => {
            for (field, child) in fields {
                current.push(field.clone());
                collect_json_nested_paths(child, current, paths);
                current.pop();
            }
        }
        _ => paths.push(current.clone()),
    }
}

/// Infers concrete schemas for uninferred JSON2 root read hints from an Arrow schema.
///
/// - `json_type_hint` contains JSON2 read hints produced by the query phase and
///   refined by the read path. This function only updates entries whose hint is
///   `JsonReadHint::Root(JsonRootReadHint::Uninferred)`. Path hints,
///   already-inferred root hints, and non-JSON2 entries are left unchanged.
/// - `schema` is the merged source schema used by the read path. For each
///   uninferred root read hint, this function looks up the field with the same
///   column name, converts the field's Arrow type to `JsonNativeType`, and
///   rewrites the hint to
///   `JsonReadHint::Root(JsonRootReadHint::Inferred(...))`.
///
/// If a hinted root column is not present in `schema`, the hint is left as-is.
/// If the field exists but its Arrow type cannot be converted to a JSON native
/// type, this function returns a data type mismatch error.
pub(crate) fn infer_json2_root_hints_from_schema(
    json_type_hint: &mut HashMap<String, JsonReadHint>,
    schema: &ArrowSchemaRef,
) -> Result<()> {
    for (col_name, hint) in json_type_hint {
        if !matches!(hint, JsonReadHint::Root(JsonRootReadHint::Uninferred)) {
            continue;
        }

        let Some((_, field)) = schema.column_with_name(col_name) else {
            continue;
        };
        let json_type =
            JsonNativeType::try_from(field.data_type()).context(DataTypeMismatchSnafu)?;
        *hint = JsonReadHint::Root(JsonRootReadHint::Inferred(json_type));
    }

    Ok(())
}

/// JSON2 output handling plan for one projected column.
#[derive(Debug, Clone)]
pub(crate) enum Json2OutputPlan {
    /// The column isn't JSON2 and doesn't need JSON2 output handling.
    NonJson2,
    /// Align the array to the expected JSON2 output type.
    Align(ConcreteDataType),
}

impl Json2OutputPlan {
    /// Builds an output plan from a column type and its optional read hint.
    ///
    /// Path hints and inferred root hints both carry a concrete JSON2 schema,
    /// so they produce an [`Json2OutputPlan::Align`] plan. The caller must
    /// infer root hints before calling this function; `Root(Uninferred)` cannot
    /// produce an output type. A missing hint on a JSON2 column is treated as an
    /// unsupported direct projection, such as `SELECT j`.
    pub(crate) fn new(
        data_type: &ConcreteDataType,
        hint: Option<&JsonReadHint>,
    ) -> DataTypeResult<Self> {
        if !is_json2_type(data_type) {
            return Ok(Self::NonJson2);
        }

        let plan = match hint {
            Some(JsonReadHint::Root(JsonRootReadHint::Inferred(json_type)))
            | Some(JsonReadHint::Paths(json_type)) => {
                Self::Align(ConcreteDataType::json2(json_type.clone()))
            }
            Some(JsonReadHint::Root(JsonRootReadHint::Uninferred)) => UnsupportedOperationSnafu {
                op: "JSON2 root read schema must be inferred before building output plans",
                vector_type: "Json2",
            }
            .fail()?,
            None => UnsupportedOperationSnafu {
                op: "direct JSON2 column projection is not supported yet; use json_get(column, '') to read the root JSON2 value",
                vector_type: "Json2",
            }
            .fail()?,
        };
        Ok(plan)
    }

    /// Returns the planned concrete output type, if this plan has one before
    /// reading actual data.
    pub(crate) fn planned_type(&self) -> Option<&ConcreteDataType> {
        match self {
            Self::Align(data_type) => Some(data_type),
            Self::NonJson2 => None,
        }
    }
}

/// Normalizes one output array and its schema type according to its JSON2 read plan.
///
/// Behavior by plan:
///
/// - [`Json2OutputPlan::NonJson2`] returns the array unchanged.
/// - [`Json2OutputPlan::Align`] updates `output_type` to the planned JSON2 type
///   and aligns the array to that type, filling missing fields with nulls.
///
/// Parameters:
///
/// - `array`: the column data read from memtables or SSTs.
/// - `output_type`: the type used in the returned record batch schema. This
///   function updates it when the JSON2 plan determines a more precise type.
/// - `plan`: whether the column is a non-JSON2 column or a JSON2 read with a
///   concrete output schema.
pub(crate) fn normalize_json2_output(
    array: ArrayRef,
    output_type: &mut ConcreteDataType,
    plan: &Json2OutputPlan,
) -> DataTypeResult<ArrayRef> {
    match plan {
        Json2OutputPlan::NonJson2 => Ok(array),
        Json2OutputPlan::Align(data_type) => {
            *output_type = data_type.clone();
            JsonArray::from(&array).try_align(&data_type.as_arrow_type())
        }
    }
}

fn is_json2_type(data_type: &ConcreteDataType) -> bool {
    data_type
        .as_json()
        .is_some_and(|json_type| json_type.is_json2())
}

#[cfg(test)]
mod tests {
    use datatypes::types::json_type::JsonObjectType;

    use super::*;

    #[test]
    fn test_json2_output_plan_rejects_unhinted_json2_projection() {
        let output_type = ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new()));
        let err = Json2OutputPlan::new(&output_type, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("use json_get(column, '')"));
    }
}
