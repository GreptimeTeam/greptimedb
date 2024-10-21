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

use ahash::HashMap;
use api::helper::proto_value_type;
use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnDataTypeExtension, JsonTypeExtension, SemanticType};
use coerce::{coerce_columns, coerce_value};
use greptime_proto::v1::{ColumnSchema, Row, Rows, Value as GreptimeValue};
use itertools::Itertools;
use serde_json::{Map, Number};

use crate::etl::error::{
    IdentifyPipelineColumnTypeMismatchSnafu, Result, TransformColumnNameMustBeUniqueSnafu,
    TransformEmptySnafu, TransformMultipleTimestampIndexSnafu, TransformTimestampIndexCountSnafu,
    UnsupportedNumberTypeSnafu,
};
use crate::etl::field::{InputFieldInfo, OneInputOneOutputField};
use crate::etl::transform::index::Index;
use crate::etl::transform::{Transform, Transformer, Transforms};
use crate::etl::value::{Timestamp, Value};

const DEFAULT_GREPTIME_TIMESTAMP_COLUMN: &str = "greptime_timestamp";

/// fields not in the columns will be discarded
/// to prevent automatic column creation in GreptimeDB
#[derive(Debug, Clone)]
pub struct GreptimeTransformer {
    transforms: Transforms,
    schema: Vec<ColumnSchema>,
}

impl GreptimeTransformer {
    /// Add a default timestamp column to the transforms
    fn add_greptime_timestamp_column(transforms: &mut Transforms) {
        let ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let type_ = Value::Timestamp(Timestamp::Nanosecond(ns));
        let default = Some(type_.clone());

        let transform = Transform {
            real_fields: vec![OneInputOneOutputField::new(
                InputFieldInfo {
                    name: DEFAULT_GREPTIME_TIMESTAMP_COLUMN.to_string(),
                    index: usize::MAX,
                },
                (
                    DEFAULT_GREPTIME_TIMESTAMP_COLUMN.to_string(),
                    transforms
                        .transforms
                        .iter()
                        .map(|x| x.real_fields.len())
                        .sum(),
                ),
            )],
            type_,
            default,
            index: Some(Index::Time),
            on_failure: Some(crate::etl::transform::OnFailure::Default),
        };
        let required_keys = transforms.required_keys_mut();
        required_keys.push(DEFAULT_GREPTIME_TIMESTAMP_COLUMN.to_string());

        let output_keys = transforms.output_keys_mut();
        output_keys.push(DEFAULT_GREPTIME_TIMESTAMP_COLUMN.to_string());
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

impl std::fmt::Display for GreptimeTransformer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "GreptimeTransformer.\nColumns: {}", self.transforms)
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
                .real_fields
                .iter()
                .map(|f| f.output_name())
                .collect::<HashSet<_>>();

            let intersections: Vec<_> = column_names_set.intersection(&target_fields_set).collect();
            if !intersections.is_empty() {
                let duplicates = intersections.iter().join(",");
                return TransformColumnNameMustBeUniqueSnafu { duplicates }.fail();
            }

            column_names_set.extend(target_fields_set);

            if let Some(idx) = transform.index {
                if idx == Index::Time {
                    match transform.real_fields.len() {
                        //Safety unwrap is fine here because we have checked the length of real_fields
                        1 => timestamp_columns
                            .push(transform.real_fields.first().unwrap().input_name()),
                        _ => {
                            return TransformMultipleTimestampIndexSnafu {
                                columns: transform
                                    .real_fields
                                    .iter()
                                    .map(|x| x.input_name())
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

    fn transform_mut(&self, val: &mut Vec<Value>) -> Result<Self::VecOutput> {
        let mut values = vec![GreptimeValue { value_data: None }; self.schema.len()];
        for transform in self.transforms.iter() {
            for field in transform.real_fields.iter() {
                let index = field.input_index();
                let output_index = field.output_index();
                match val.get(index) {
                    Some(v) => {
                        let value_data = coerce_value(v, transform)?;
                        // every transform fields has only one output field
                        values[output_index] = GreptimeValue { value_data };
                    }
                    None => {
                        let default = transform.get_default();
                        let value_data = match default {
                            Some(default) => coerce_value(default, transform)?,
                            None => None,
                        };
                        values[output_index] = GreptimeValue { value_data };
                    }
                }
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

fn json_value_to_row(
    schema_info: &mut SchemaInfo,
    map: Map<String, serde_json::Value>,
) -> Result<Row> {
    let mut row: Vec<GreptimeValue> = Vec::with_capacity(schema_info.schema.len());
    for _ in 0..schema_info.schema.len() {
        row.push(GreptimeValue { value_data: None });
    }
    for (column_name, value) in map {
        if column_name == DEFAULT_GREPTIME_TIMESTAMP_COLUMN {
            continue;
        }
        let index = schema_info.index.get(&column_name).copied();
        match value {
            serde_json::Value::Null => {
                // do nothing
            }
            serde_json::Value::String(s) => {
                resolve_schema(
                    index,
                    ValueData::StringValue(s),
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
            serde_json::Value::Bool(b) => {
                resolve_schema(
                    index,
                    ValueData::BoolValue(b),
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
            serde_json::Value::Number(n) => {
                resolve_number_schema(n, column_name, index, &mut row, schema_info)?;
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                resolve_schema(
                    index,
                    ValueData::BinaryValue(jsonb::Value::from(value).to_vec()),
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

/// Identity pipeline for Greptime
/// This pipeline will convert the input JSON array to Greptime Rows
/// 1. The pipeline will add a default timestamp column to the schema
/// 2. The pipeline not resolve NULL value
/// 3. The pipeline assumes that the json format is fixed
/// 4. The pipeline will return an error if the same column datatype is mismatched
/// 5. The pipeline will analyze the schema of each json record and merge them to get the final schema.
pub fn identity_pipeline(array: Vec<serde_json::Value>) -> Result<Rows> {
    let mut rows = Vec::with_capacity(array.len());

    let mut schema = SchemaInfo::default();
    for value in array {
        if let serde_json::Value::Object(map) = value {
            let row = json_value_to_row(&mut schema, map)?;
            rows.push(row);
        }
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
    let column_count = schema.schema.len();
    for row in rows.iter_mut() {
        let diff = column_count - row.values.len();
        for _ in 0..diff {
            row.values.push(GreptimeValue { value_data: None });
        }
        row.values.push(ts.clone());
    }
    schema.schema.push(greptime_timestamp_schema);
    Ok(Rows {
        schema: schema.schema,
        rows,
    })
}

#[cfg(test)]
mod tests {
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
            let rows = identity_pipeline(array);
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
            let rows = identity_pipeline(array);
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
            let rows = identity_pipeline(array);
            assert!(rows.is_ok());
            let rows = rows.unwrap();
            assert_eq!(rows.schema.len(), 8);
            assert_eq!(rows.rows.len(), 2);
            assert_eq!(8, rows.rows[0].values.len());
            assert_eq!(8, rows.rows[1].values.len());
        }
    }
}
