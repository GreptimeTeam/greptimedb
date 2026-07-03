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

//! Helpers for JSON2 schema handling in the read path.
//!
//! JSON2 schemas used by reads are inferred from query expressions. For example,
//! `json_get(j, "a.b")` gives us a concrete schema for the requested paths, so
//! the reader can prune nested fields and align the output array to that schema.
//!
//! Root JSON2 reads are different: the query doesn't provide a concrete schema.
//! They read the whole JSON2 column, and the output schema is whatever the
//! actual array carries. A projected JSON2 column without a read hint is not
//! supported by this module yet; it must not be aligned to the manifest's empty
//! JSON2 schema because that would silently drop fields.

use std::collections::HashMap;

use datatypes::arrow::array::ArrayRef;
use datatypes::error::{Result as DataTypeResult, UnsupportedOperationSnafu};
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::types::json_type::JsonNativeType;
use datatypes::vectors::json::array::JsonArray;
use store_api::metadata::RegionMetadata;
use store_api::storage::{JsonReadHint, NestedPath};

use crate::read::read_columns::{ReadColumns, merge_nested_paths};

/// Applies JSON2 read hints to the nested paths in read columns.
///
/// [`JsonReadHint::Root`] is a read strategy: it reads the whole JSON2 column
/// and therefore clears nested path pruning. [`JsonReadHint::Paths`] narrows the
/// read to the leaves described by the hinted JSON type.
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

/// JSON2 output handling plan for one projected column.
#[derive(Debug, Clone)]
pub(crate) enum Json2OutputPlan {
    /// The column isn't JSON2 and doesn't need JSON2 output handling.
    NonJson2,
    /// The column is a direct JSON2 projection without a read hint.
    UnsupportedDirectProjection,
    /// Read the root JSON2 value. The output type comes from the actual array.
    Root,
    /// Align the array to the expected JSON2 type.
    Align(ConcreteDataType),
}

impl Json2OutputPlan {
    /// Builds an output plan from a column type and its optional read hint.
    ///
    /// JSON2 path hints have a concrete schema and use [`Json2OutputPlan::Align`],
    /// while JSON2 root hints use [`Json2OutputPlan::Root`]. A missing hint on a
    /// JSON2 column means a direct projection such as `SELECT j`, which is not
    /// handled here yet.
    pub(crate) fn new(data_type: &ConcreteDataType, hint: Option<&JsonReadHint>) -> Self {
        if !is_json2_type(data_type) {
            return Self::NonJson2;
        }

        match hint {
            Some(JsonReadHint::Root) => Self::Root,
            Some(JsonReadHint::Paths(json_type)) => {
                Self::Align(ConcreteDataType::json2(json_type.clone()))
            }
            None => Self::UnsupportedDirectProjection,
        }
    }

    /// Returns the planned concrete output type, if this plan has one before
    /// reading actual data.
    pub(crate) fn planned_type(&self) -> Option<&ConcreteDataType> {
        match self {
            Self::Align(data_type) => Some(data_type),
            Self::NonJson2 | Self::UnsupportedDirectProjection | Self::Root => None,
        }
    }
}

/// Normalizes one output array and its schema type according to its JSON2 read plan.
///
/// Behavior by plan:
///
/// - [`Json2OutputPlan::NonJson2`] returns the array unchanged.
/// - [`Json2OutputPlan::UnsupportedDirectProjection`] returns an error. Without
///   a read hint, aligning a direct JSON2 projection to the manifest's empty
///   JSON2 schema would silently drop all fields.
/// - [`Json2OutputPlan::Root`] keeps the array unchanged, but replaces
///   `output_type` with the JSON2 type inferred from the actual Arrow array.
///   Root reads don't have a query-inferred schema, so the actual array schema
///   becomes the output schema.
/// - [`Json2OutputPlan::Align`] updates `output_type` to the planned JSON2 type
///   and aligns the array to that type, filling missing fields with nulls.
///
/// Parameters:
///
/// - `array`: the column data read from memtables or SSTs.
/// - `output_type`: the type used in the returned record batch schema. This
///   function updates it when the JSON2 plan determines a more precise type.
/// - `plan`: whether the column is a non-JSON2 column, an unsupported direct
///   JSON2 projection, a root JSON2 read, or a path JSON2 read with a concrete
///   schema.
pub(crate) fn normalize_json2_output(
    array: ArrayRef,
    output_type: &mut ConcreteDataType,
    plan: &Json2OutputPlan,
) -> DataTypeResult<ArrayRef> {
    match plan {
        Json2OutputPlan::NonJson2 => Ok(array),
        Json2OutputPlan::UnsupportedDirectProjection => UnsupportedOperationSnafu {
            op: "direct JSON2 column projection is not supported yet; use json_get(column, '') to read the root JSON2 value",
            vector_type: "Json2",
        }
        .fail(),
        Json2OutputPlan::Root => {
            let json_type = JsonNativeType::try_from(array.data_type())?;
            *output_type = ConcreteDataType::json2(json_type);
            Ok(array)
        }
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
    use std::sync::Arc;

    use datatypes::arrow::array::NullArray;
    use datatypes::types::json_type::JsonObjectType;

    use super::*;

    #[test]
    fn test_json2_output_plan_rejects_unhinted_json2_projection() {
        let mut output_type =
            ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new()));
        let plan = Json2OutputPlan::new(&output_type, None);
        assert!(matches!(plan, Json2OutputPlan::UnsupportedDirectProjection));

        let err = normalize_json2_output(Arc::new(NullArray::new(1)), &mut output_type, &plan)
            .unwrap_err()
            .to_string();
        assert!(err.contains("use json_get(column, '')"));
    }
}
