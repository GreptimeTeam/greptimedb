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

use coerce::{coerce_columns, coerce_value};
use greptime_proto::v1::{ColumnSchema, Row, Rows, Value as GreptimeValue};
use itertools::Itertools;

use crate::etl::field::{Field, Fields};
use crate::etl::transform::index::Index;
use crate::etl::transform::{Transform, Transformer, Transforms};
use crate::etl::value::{Array, Map, Timestamp, Value};

const DEFAULT_GREPTIME_TIMESTAMP_COLUMN: &str = "greptime_timestamp";

/// fields not in the columns will be discarded
/// to prevent automatic column creation in GreptimeDB
#[derive(Debug, Clone)]
pub struct GreptimeTransformer {
    transforms: Transforms,
    schema: Vec<ColumnSchema>,
}

impl GreptimeTransformer {
    fn default_greptime_timestamp_column() -> Transform {
        let ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let type_ = Value::Timestamp(Timestamp::Nanosecond(ns));
        let default = Some(type_.clone());
        let mut field = Field::new(DEFAULT_GREPTIME_TIMESTAMP_COLUMN);
        field.insert_output_index(DEFAULT_GREPTIME_TIMESTAMP_COLUMN.to_string(), 0);
        let fields = Fields::new(vec![field]).unwrap();

        Transform {
            fields,
            type_,
            default,
            index: Some(Index::Time),
            on_failure: Some(crate::etl::transform::OnFailure::Default),
        }
    }

    fn schemas(transforms: &Transforms) -> Result<Vec<ColumnSchema>, String> {
        let mut schema = vec![];
        for transform in transforms.iter() {
            schema.extend(coerce_columns(transform)?);
        }
        Ok(schema
            .into_iter()
            .sorted_by(|left, right| left.column_name.cmp(&right.column_name))
            .collect())
    }

    fn transform_map(&self, map: &Map) -> Result<Row, String> {
        let mut values = vec![GreptimeValue { value_data: None }; self.schema.len()];
        for transform in self.transforms.iter() {
            for field in transform.fields.iter() {
                let value_data = match map.get(field.get_field_name()) {
                    Some(val) => coerce_value(val, transform)?,
                    None => {
                        let default = transform.get_default();
                        if default.is_some() {
                            coerce_value(default.unwrap(), transform)?
                        } else {
                            None
                        }
                    }
                };
                if let Some(i) = field
                    .output_fields_index_mapping
                    .iter()
                    .next()
                    .map(|kv| kv.1)
                {
                    values[*i] = GreptimeValue { value_data }
                } else {
                    return Err(format!(
                        "field: {} output_fields is empty.",
                        field.get_field_name()
                    ));
                }
            }
        }

        Ok(Row { values })
    }

    fn transform_array(&self, arr: &Array) -> Result<Vec<Row>, String> {
        let mut rows = Vec::with_capacity(arr.len());
        for v in arr.iter() {
            match v {
                Value::Map(map) => {
                    let row = self.transform_map(map)?;
                    rows.push(row);
                }
                _ => return Err(format!("Expected map, found: {v:?}")),
            }
        }
        Ok(rows)
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

    fn new(mut transforms: Transforms) -> Result<Self, String> {
        if transforms.is_empty() {
            return Err("transform cannot be empty".to_string());
        }

        let mut column_names_set = HashSet::new();
        let mut timestamp_columns = vec![];

        for transform in transforms.iter() {
            let target_fields_set = transform
                .fields
                .iter()
                .map(|f| f.get_target_field())
                .collect::<HashSet<_>>();

            let intersections: Vec<_> = column_names_set.intersection(&target_fields_set).collect();
            if !intersections.is_empty() {
                let duplicates = intersections.iter().join(",");
                return Err(format!(
                    "column name must be unique, but got duplicated: {duplicates}"
                ));
            }

            column_names_set.extend(target_fields_set);

            if let Some(idx) = transform.index {
                if idx == Index::Time {
                    match transform.fields.len() {
                        1 => timestamp_columns.push(transform.fields.first().unwrap().get_field_name()),
                        _ => return Err(format!(
                            "Illegal to set multiple timestamp Index columns, please set only one: {}",
                            transform.fields.get_target_fields().join(", ")
                        )),
                    }
                }
            }
        }

        match timestamp_columns.len() {
            0 => {
                transforms.push(GreptimeTransformer::default_greptime_timestamp_column());

                let required_keys = transforms.required_keys_mut();
                required_keys.push(DEFAULT_GREPTIME_TIMESTAMP_COLUMN.to_string());
                required_keys.sort();

                let output_keys = transforms.output_keys_mut();
                output_keys.push(DEFAULT_GREPTIME_TIMESTAMP_COLUMN.to_string());
                output_keys.sort();
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
                Err(
                    format!("transform must have exactly one field specified as timestamp Index, but got {count}: {columns}")
                )
            }
        }
    }

    fn transform(&self, value: Value) -> Result<Self::Output, String> {
        match value {
            Value::Map(map) => {
                let rows = vec![self.transform_map(&map)?];
                Ok(Rows {
                    schema: self.schema.clone(),
                    rows,
                })
            }
            Value::Array(arr) => {
                let rows = self.transform_array(&arr)?;
                Ok(Rows {
                    schema: self.schema.clone(),
                    rows,
                })
            }
            _ => Err(format!("Expected map or array, found: {}", value)),
        }
    }

    fn transform_mut(&self, val: &mut Vec<Value>) -> Result<Self::VecOutput, String> {
        let mut values = vec![GreptimeValue { value_data: None }; self.schema.len()];
        for transform in self.transforms.iter() {
            for field in transform.fields.iter() {
                let index = field.input_field.index;
                match val.get(index) {
                    Some(v) => {
                        let value_data = coerce_value(v, transform)
                            .map_err(|e| format!("{} processor: {}", field.get_field_name(), e))?;
                        // every transform fields has only one output field
                        if let Some(i) = field
                            .output_fields_index_mapping
                            .iter()
                            .next()
                            .map(|kv| kv.1)
                        {
                            values[*i] = GreptimeValue { value_data }
                        } else {
                            return Err(format!(
                                "field: {} output_fields is empty.",
                                field.get_field_name()
                            ));
                        }
                    }
                    _ => {
                        return Err(format!(
                            "Get field not in the array field: {field:?}, {val:?}"
                        ))
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
