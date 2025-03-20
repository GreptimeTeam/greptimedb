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

pub mod coerce;

use std::collections::HashSet;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use api::helper::proto_value_type;
use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnDataTypeExtension, JsonTypeExtension, SemanticType};
use coerce::{coerce_columns, coerce_value};
use greptime_proto::v1::{ColumnSchema, Row, Rows, Value as GreptimeValue};
use itertools::Itertools;
use serde_json::Number;

use crate::error::{
    IdentifyPipelineColumnTypeMismatchSnafu, ReachedMaxNestedLevelsSnafu, Result,
    TransformColumnNameMustBeUniqueSnafu, TransformEmptySnafu,
    TransformMultipleTimestampIndexSnafu, TransformTimestampIndexCountSnafu,
    UnsupportedNumberTypeSnafu,
};
use crate::etl::field::{Field, Fields};
use crate::etl::transform::index::Index;
use crate::etl::transform::{Transform, Transformer, Transforms};
use crate::etl::value::{Timestamp, Value};
use crate::etl::PipelineMap;

const DEFAULT_GREPTIME_TIMESTAMP_COLUMN: &str = "greptime_timestamp";
const DEFAULT_MAX_NESTED_LEVELS_FOR_JSON_FLATTENING: usize = 10;

/// fields not in the columns will be discarded
/// to prevent automatic column creation in GreptimeDB
#[derive(Debug, Clone)]
pub struct GreptimeTransformer {
    transforms: Transforms,
    schema: Vec<ColumnSchema>,
}

/// Parameters that can be used to configure the greptime pipelines.
#[derive(Debug, Clone, Default)]
pub struct GreptimePipelineParams {
    /// The options for configuring the greptime pipelines.
    pub options: HashMap<String, String>,
}

impl GreptimePipelineParams {
    /// Create a `GreptimePipelineParams` from params string which is from the http header with key `x-greptime-pipeline-params`
    /// The params is in the format of `key1=value1&key2=value2`,for example:
    /// x-greptime-pipeline-params: flatten_json_object=true
    pub fn from_params(params: Option<&str>) -> Self {
        let options = params
            .unwrap_or_default()
            .split('&')
            .filter_map(|s| s.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<String, String>>();

        Self { options }
    }

    /// Whether to flatten the JSON object.
    pub fn flatten_json_object(&self) -> bool {
        self.options
            .get("flatten_json_object")
            .map(|v| v == "true")
            .unwrap_or(false)
    }
}

impl GreptimeTransformer {
    /// Add a default timestamp column to the transforms
    fn add_greptime_timestamp_column(transforms: &mut Transforms) {
        let type_ = Value::Timestamp(Timestamp::Nanosecond(0));
        let default = None;

        let transform = Transform {
            fields: Fields::one(Field::new(
                DEFAULT_GREPTIME_TIMESTAMP_COLUMN.to_string(),
                None,
            )),
            type_,
            default,
            index: Some(Index::Time),
            on_failure: Some(crate::etl::transform::OnFailure::Default),
            tag: false,
        };
        transforms.push(transform);
    }

    /// Generate the schema for the GreptimeTransformer
    fn schemas(transforms: &Transforms) -> Result<Vec<ColumnSchema>> {
        let mut schema = vec![];
        for transform in transforms.iter() {
            schema.extend(coerce_columns(transform)?);
        }
        Ok(schema)
    }
}

impl Transformer for GreptimeTransformer {
    type Output = Rows;
    type VecOutput = Row;

    fn new(mut transforms: Transforms) -> Result<Self> {
        if transforms.is_empty() {
            return TransformEmptySnafu.fail();
        }

        let mut column_names_set = HashSet::new();
        let mut timestamp_columns = vec![];

        for transform in transforms.iter() {
            let target_fields_set = transform
                .fields
                .iter()
                .map(|f| f.target_or_input_field())
                .collect::<HashSet<_>>();

            let intersections: Vec<_> = column_names_set.intersection(&target_fields_set).collect();
            if !intersections.is_empty() {
                let duplicates = intersections.iter().join(",");
                return TransformColumnNameMustBeUniqueSnafu { duplicates }.fail();
            }

            column_names_set.extend(target_fields_set);

            if let Some(idx) = transform.index {
                if idx == Index::Time {
                    match transform.fields.len() {
                        //Safety unwrap is fine here because we have checked the length of real_fields
                        1 => {
                            timestamp_columns.push(transform.fields.first().unwrap().input_field())
                        }
                        _ => {
                            return TransformMultipleTimestampIndexSnafu {
                                columns: transform
                                    .fields
                                    .iter()
                                    .map(|x| x.input_field())
                                    .join(", "),
                            }
                            .fail();
                        }
                    }
                }
            }
        }

        match timestamp_columns.len() {
            0 => {
                GreptimeTransformer::add_greptime_timestamp_column(&mut transforms);

                let schema = GreptimeTransformer::schemas(&transforms)?;
                Ok(GreptimeTransformer { transforms, schema })
            }
            1 => {
                let schema = GreptimeTransformer::schemas(&transforms)?;
                Ok(GreptimeTransformer { transforms, schema })
            }
            _ => {
                let columns: String = timestamp_columns.iter().map(|s| s.to_string()).join(", ");
                let count = timestamp_columns.len();
                TransformTimestampIndexCountSnafu { count, columns }.fail()
            }
        }
    }

    fn transform_mut(&self, val: &mut PipelineMap) -> Result<Self::VecOutput> {
        let mut values = vec![GreptimeValue { value_data: None }; self.schema.len()];
        let mut output_index = 0;
        for transform in self.transforms.iter() {
            for field in transform.fields.iter() {
                let index = field.input_field();
                match val.get(index) {
                    Some(v) => {
                        let value_data = coerce_value(v, transform)?;
                        // every transform fields has only one output field
                        values[output_index] = GreptimeValue { value_data };
                    }
                    None => {
                        let value_data = match transform.on_failure {
                            Some(crate::etl::transform::OnFailure::Default) => {
                                match transform.get_default() {
                                    Some(default) => coerce_value(default, transform)?,
                                    None => match transform.get_default_value_when_data_is_none() {
                                        Some(default) => coerce_value(&default, transform)?,
                                        None => None,
                                    },
                                }
                            }
                            Some(crate::etl::transform::OnFailure::Ignore) => None,
                            None => None,
                        };
                        values[output_index] = GreptimeValue { value_data };
                    }
                }
                output_index += 1;
            }
        }
        Ok(Row { values })
    }

    fn transforms(&self) -> &Transforms {
        &self.transforms
    }

    fn schemas(&self) -> &Vec<greptime_proto::v1::ColumnSchema> {
        &self.schema
    }

    fn transforms_mut(&mut self) -> &mut Transforms {
        &mut self.transforms
    }
}

/// This is used to record the current state schema information and a sequential cache of field names.
/// As you traverse the user input JSON, this will change.
/// It will record a superset of all user input schemas.
#[derive(Debug, Default)]
pub struct SchemaInfo {
    /// schema info
    pub schema: Vec<ColumnSchema>,
    /// index of the column name
    pub index: HashMap<String, usize>,
}

impl SchemaInfo {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            schema: Vec::with_capacity(capacity),
            index: HashMap::with_capacity(capacity),
        }
    }
}

fn resolve_schema(
    index: Option<usize>,
    value_data: ValueData,
    column_schema: ColumnSchema,
    row: &mut Vec<GreptimeValue>,
    schema_info: &mut SchemaInfo,
) -> Result<()> {
    if let Some(index) = index {
        let api_value = GreptimeValue {
            value_data: Some(value_data),
        };
        // Safety unwrap is fine here because api_value is always valid
        let value_column_data_type = proto_value_type(&api_value).unwrap();
        // Safety unwrap is fine here because index is always valid
        let schema_column_data_type = schema_info.schema.get(index).unwrap().datatype();
        if value_column_data_type != schema_column_data_type {
            IdentifyPipelineColumnTypeMismatchSnafu {
                column: column_schema.column_name,
                expected: schema_column_data_type.as_str_name(),
                actual: value_column_data_type.as_str_name(),
            }
            .fail()
        } else {
            row[index] = api_value;
            Ok(())
        }
    } else {
        let key = column_schema.column_name.clone();
        schema_info.schema.push(column_schema);
        schema_info.index.insert(key, schema_info.schema.len() - 1);
        let api_value = GreptimeValue {
            value_data: Some(value_data),
        };
        row.push(api_value);
        Ok(())
    }
}

fn resolve_number_schema(
    n: Number,
    column_name: String,
    index: Option<usize>,
    row: &mut Vec<GreptimeValue>,
    schema_info: &mut SchemaInfo,
) -> Result<()> {
    let (value, datatype, semantic_type) = if n.is_i64() {
        (
            ValueData::I64Value(n.as_i64().unwrap()),
            ColumnDataType::Int64 as i32,
            SemanticType::Field as i32,
        )
    } else if n.is_u64() {
        (
            ValueData::U64Value(n.as_u64().unwrap()),
            ColumnDataType::Uint64 as i32,
            SemanticType::Field as i32,
        )
    } else if n.is_f64() {
        (
            ValueData::F64Value(n.as_f64().unwrap()),
            ColumnDataType::Float64 as i32,
            SemanticType::Field as i32,
        )
    } else {
        return UnsupportedNumberTypeSnafu { value: n }.fail();
    };
    resolve_schema(
        index,
        value,
        ColumnSchema {
            column_name,
            datatype,
            semantic_type,
            datatype_extension: None,
            options: None,
        },
        row,
        schema_info,
    )
}

fn values_to_row(schema_info: &mut SchemaInfo, values: PipelineMap) -> Result<Row> {
    let mut row: Vec<GreptimeValue> = Vec::with_capacity(schema_info.schema.len());
    for _ in 0..schema_info.schema.len() {
        row.push(GreptimeValue { value_data: None });
    }

    for (column_name, value) in values.into_iter() {
        if column_name == DEFAULT_GREPTIME_TIMESTAMP_COLUMN {
            continue;
        }

        let index = schema_info.index.get(&column_name).copied();

        match value {
            Value::Null => {}

            Value::Int8(_) | Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => {
                // safe unwrap after type matched
                let v = value.as_i64().unwrap();
                resolve_schema(
                    index,
                    ValueData::I64Value(v),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::Int64 as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }

            Value::Uint8(_) | Value::Uint16(_) | Value::Uint32(_) | Value::Uint64(_) => {
                // safe unwrap after type matched
                let v = value.as_u64().unwrap();
                resolve_schema(
                    index,
                    ValueData::U64Value(v),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::Uint64 as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }

            Value::Float32(_) | Value::Float64(_) => {
                // safe unwrap after type matched
                let v = value.as_f64().unwrap();
                resolve_schema(
                    index,
                    ValueData::F64Value(v),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::Float64 as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }

            Value::Boolean(v) => {
                resolve_schema(
                    index,
                    ValueData::BoolValue(v),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::Boolean as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }
            Value::String(v) => {
                resolve_schema(
                    index,
                    ValueData::StringValue(v),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::String as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }

            Value::Timestamp(Timestamp::Nanosecond(ns)) => {
                resolve_schema(
                    index,
                    ValueData::TimestampNanosecondValue(ns),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::TimestampNanosecond as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }
            Value::Timestamp(Timestamp::Microsecond(us)) => {
                resolve_schema(
                    index,
                    ValueData::TimestampMicrosecondValue(us),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::TimestampMicrosecond as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }
            Value::Timestamp(Timestamp::Millisecond(ms)) => {
                resolve_schema(
                    index,
                    ValueData::TimestampMillisecondValue(ms),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::TimestampMillisecond as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }
            Value::Timestamp(Timestamp::Second(s)) => {
                resolve_schema(
                    index,
                    ValueData::TimestampSecondValue(s),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::TimestampSecond as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }

            Value::Array(_) | Value::Map(_) => {
                let data: jsonb::Value = value.into();
                resolve_schema(
                    index,
                    ValueData::BinaryValue(data.to_vec()),
                    ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::Binary as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: Some(ColumnDataTypeExtension {
                            type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
                        }),
                        options: None,
                    },
                    &mut row,
                    schema_info,
                )?;
            }
        }
    }
    Ok(Row { values: row })
}

fn identity_pipeline_inner<'a>(
    array: Vec<PipelineMap>,
    tag_column_names: Option<impl Iterator<Item = &'a String>>,
    _params: &GreptimePipelineParams,
) -> Result<Rows> {
    let mut rows = Vec::with_capacity(array.len());
    let mut schema_info = SchemaInfo::default();

    for values in array {
        let row = values_to_row(&mut schema_info, values)?;
        rows.push(row);
    }

    let greptime_timestamp_schema = ColumnSchema {
        column_name: DEFAULT_GREPTIME_TIMESTAMP_COLUMN.to_string(),
        datatype: ColumnDataType::TimestampNanosecond as i32,
        semantic_type: SemanticType::Timestamp as i32,
        datatype_extension: None,
        options: None,
    };
    let ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let ts = GreptimeValue {
        value_data: Some(ValueData::TimestampNanosecondValue(ns)),
    };
    let column_count = schema_info.schema.len();
    for row in rows.iter_mut() {
        let diff = column_count - row.values.len();
        for _ in 0..diff {
            row.values.push(GreptimeValue { value_data: None });
        }
        row.values.push(ts.clone());
    }
    schema_info.schema.push(greptime_timestamp_schema);

    // set the semantic type of the row key column to Tag
    if let Some(tag_column_names) = tag_column_names {
        tag_column_names.for_each(|tag_column_name| {
            if let Some(index) = schema_info.index.get(tag_column_name) {
                schema_info.schema[*index].semantic_type = SemanticType::Tag as i32;
            }
        });
    }
    Ok(Rows {
        schema: schema_info.schema,
        rows,
    })
}

/// Identity pipeline for Greptime
/// This pipeline will convert the input JSON array to Greptime Rows
/// params table is used to set the semantic type of the row key column to Tag
/// 1. The pipeline will add a default timestamp column to the schema
/// 2. The pipeline not resolve NULL value
/// 3. The pipeline assumes that the json format is fixed
/// 4. The pipeline will return an error if the same column datatype is mismatched
/// 5. The pipeline will analyze the schema of each json record and merge them to get the final schema.
pub fn identity_pipeline(
    array: Vec<PipelineMap>,
    table: Option<Arc<table::Table>>,
    params: &GreptimePipelineParams,
) -> Result<Rows> {
    let input = if params.flatten_json_object() {
        array
            .into_iter()
            .map(|item| flatten_object(item, DEFAULT_MAX_NESTED_LEVELS_FOR_JSON_FLATTENING))
            .collect::<Result<Vec<PipelineMap>>>()?
    } else {
        array
    };

    match table {
        Some(table) => {
            let table_info = table.table_info();
            let tag_column_names = table_info.meta.row_key_column_names();
            identity_pipeline_inner(input, Some(tag_column_names), params)
        }
        None => identity_pipeline_inner(input, None::<std::iter::Empty<&String>>, params),
    }
}

/// Consumes the JSON object and consumes it into a single-level object.
///
/// The `max_nested_levels` parameter is used to limit the nested levels of the JSON object.
/// The error will be returned if the nested levels is greater than the `max_nested_levels`.
pub fn flatten_object(object: PipelineMap, max_nested_levels: usize) -> Result<PipelineMap> {
    let mut flattened = PipelineMap::new();

    if !object.is_empty() {
        // it will use recursion to flatten the object.
        do_flatten_object(&mut flattened, None, object, 1, max_nested_levels)?;
    }

    Ok(flattened)
}

fn do_flatten_object(
    dest: &mut PipelineMap,
    base: Option<&str>,
    object: PipelineMap,
    current_level: usize,
    max_nested_levels: usize,
) -> Result<()> {
    // For safety, we do not allow the depth to be greater than the max_object_depth.
    if current_level > max_nested_levels {
        return ReachedMaxNestedLevelsSnafu { max_nested_levels }.fail();
    }

    for (key, value) in object {
        let new_key = base.map_or_else(|| key.clone(), |base_key| format!("{base_key}.{key}"));

        match value {
            Value::Map(object) => {
                do_flatten_object(
                    dest,
                    Some(&new_key),
                    object.values,
                    current_level + 1,
                    max_nested_levels,
                )?;
            }
            // For other types, we will directly insert them into as JSON type.
            _ => {
                dest.insert(new_key, value);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;

    use super::*;
    use crate::etl::{json_array_to_intermediate_state, json_to_intermediate_state};
    use crate::identity_pipeline;

    #[test]
    fn test_identify_pipeline() {
        {
            let array = vec![
                serde_json::json!({
                    "woshinull": null,
                    "name": "Alice",
                    "age": 20,
                    "is_student": true,
                    "score": 99.5,
                    "hobbies": "reading",
                    "address": "Beijing",
                }),
                serde_json::json!({
                    "name": "Bob",
                    "age": 21,
                    "is_student": false,
                    "score": "88.5",
                    "hobbies": "swimming",
                    "address": "Shanghai",
                    "gaga": "gaga"
                }),
            ];
            let array = json_array_to_intermediate_state(array).unwrap();
            let rows = identity_pipeline(array, None, &GreptimePipelineParams::default());
            assert!(rows.is_err());
            assert_eq!(
                rows.err().unwrap().to_string(),
                "Column datatype mismatch. For column: score, expected datatype: FLOAT64, actual datatype: STRING".to_string(),
            );
        }
        {
            let array = vec![
                serde_json::json!({
                    "woshinull": null,
                    "name": "Alice",
                    "age": 20,
                    "is_student": true,
                    "score": 99.5,
                    "hobbies": "reading",
                    "address": "Beijing",
                }),
                serde_json::json!({
                    "name": "Bob",
                    "age": 21,
                    "is_student": false,
                    "score": 88,
                    "hobbies": "swimming",
                    "address": "Shanghai",
                    "gaga": "gaga"
                }),
            ];
            let rows = identity_pipeline(
                json_array_to_intermediate_state(array).unwrap(),
                None,
                &GreptimePipelineParams::default(),
            );
            assert!(rows.is_err());
            assert_eq!(
                rows.err().unwrap().to_string(),
                "Column datatype mismatch. For column: score, expected datatype: FLOAT64, actual datatype: INT64".to_string(),
            );
        }
        {
            let array = vec![
                serde_json::json!({
                    "woshinull": null,
                    "name": "Alice",
                    "age": 20,
                    "is_student": true,
                    "score": 99.5,
                    "hobbies": "reading",
                    "address": "Beijing",
                }),
                serde_json::json!({
                    "name": "Bob",
                    "age": 21,
                    "is_student": false,
                    "score": 88.5,
                    "hobbies": "swimming",
                    "address": "Shanghai",
                    "gaga": "gaga"
                }),
            ];
            let rows = identity_pipeline(
                json_array_to_intermediate_state(array).unwrap(),
                None,
                &GreptimePipelineParams::default(),
            );
            assert!(rows.is_ok());
            let rows = rows.unwrap();
            assert_eq!(rows.schema.len(), 8);
            assert_eq!(rows.rows.len(), 2);
            assert_eq!(8, rows.rows[0].values.len());
            assert_eq!(8, rows.rows[1].values.len());
        }
        {
            let array = vec![
                serde_json::json!({
                    "woshinull": null,
                    "name": "Alice",
                    "age": 20,
                    "is_student": true,
                    "score": 99.5,
                    "hobbies": "reading",
                    "address": "Beijing",
                }),
                serde_json::json!({
                    "name": "Bob",
                    "age": 21,
                    "is_student": false,
                    "score": 88.5,
                    "hobbies": "swimming",
                    "address": "Shanghai",
                    "gaga": "gaga"
                }),
            ];
            let tag_column_names = ["name".to_string(), "address".to_string()];
            let rows = identity_pipeline_inner(
                json_array_to_intermediate_state(array).unwrap(),
                Some(tag_column_names.iter()),
                &GreptimePipelineParams::default(),
            );
            assert!(rows.is_ok());
            let rows = rows.unwrap();
            assert_eq!(rows.schema.len(), 8);
            assert_eq!(rows.rows.len(), 2);
            assert_eq!(8, rows.rows[0].values.len());
            assert_eq!(8, rows.rows[1].values.len());
            assert_eq!(
                rows.schema
                    .iter()
                    .find(|x| x.column_name == "name")
                    .unwrap()
                    .semantic_type,
                SemanticType::Tag as i32
            );
            assert_eq!(
                rows.schema
                    .iter()
                    .find(|x| x.column_name == "address")
                    .unwrap()
                    .semantic_type,
                SemanticType::Tag as i32
            );
            assert_eq!(
                rows.schema
                    .iter()
                    .filter(|x| x.semantic_type == SemanticType::Tag as i32)
                    .count(),
                2
            );
        }
    }

    #[test]
    fn test_flatten() {
        let test_cases = vec![
            // Basic case.
            (
                serde_json::json!(
                    {
                        "a": {
                            "b": {
                                "c": [1, 2, 3]
                            }
                        },
                        "d": [
                            "foo",
                            "bar"
                        ],
                        "e": {
                            "f": [7, 8, 9],
                            "g": {
                                "h": 123,
                                "i": "hello",
                                "j": {
                                    "k": true
                                }
                            }
                        }
                    }
                ),
                10,
                Some(serde_json::json!(
                    {
                        "a.b.c": [1,2,3],
                        "d": ["foo","bar"],
                        "e.f": [7,8,9],
                        "e.g.h": 123,
                        "e.g.i": "hello",
                        "e.g.j.k": true
                    }
                )),
            ),
            // Test the case where the object has more than 3 nested levels.
            (
                serde_json::json!(
                    {
                        "a": {
                            "b": {
                                "c": {
                                    "d": [1, 2, 3]
                                }
                            }
                        },
                        "e": [
                            "foo",
                            "bar"
                        ]
                    }
                ),
                3,
                None,
            ),
        ];

        for (input, max_depth, expected) in test_cases {
            let input = json_to_intermediate_state(input).unwrap();
            let expected = expected.map(|e| json_to_intermediate_state(e).unwrap());

            let flattened_object = flatten_object(input, max_depth).ok();
            assert_eq!(flattened_object, expected);
        }
    }

    #[test]
    fn test_greptime_pipeline_params() {
        let params = Some("flatten_json_object=true");
        let pipeline_params = GreptimePipelineParams::from_params(params);
        assert!(pipeline_params.flatten_json_object());

        let params = None;
        let pipeline_params = GreptimePipelineParams::from_params(params);
        assert!(!pipeline_params.flatten_json_object());
    }
}
