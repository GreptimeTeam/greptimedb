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

use std::collections::{HashMap, HashSet};

use api::v1::value::ValueData;
use api::v1::{JsonValue, Rows, json_value};
use arrow::datatypes::DataType;
use arrow_schema::extension::ExtensionType;
use datatypes::extension::json::Json2ExtensionType;
use datatypes::json::JSON2_DEFAULT_MAX_AUTO_EXPANDED_PATHS;
use datatypes::json::value::JsonVariant;
use datatypes::schema::ColumnDefaultConstraint;
use datatypes::value::Value;
use datatypes::vectors::json;
use table::metadata::TableInfo;

const ARRAY_OVERHEAD: usize = 512;
const VIEW_BLOCK_BYTES: usize = 8192;
const VALUE_SCRATCH_BYTES: usize = 256;

/// Estimates additional memory for conversion, including destination columns absent
/// from the request. Overflow fails closed. Source rows remain separately reserved.
///
/// This budgets buffers, capacity growth and JSON2 conversion scratch, not allocator
/// RSS. shortcut: account for flush concatenation, partitioning and IPC separately
/// when extending admission to the entire flush pipeline.
pub(crate) fn estimated_record_batch_bytes(rows: &Rows, table: &TableInfo) -> Option<usize> {
    let source = rows
        .schema
        .iter()
        .enumerate()
        .map(|(i, column)| (column.column_name.as_str(), i))
        .collect::<HashMap<_, _>>();
    let mut total = 0usize;
    for (column, field) in table
        .meta
        .schema
        .column_schemas()
        .iter()
        .zip(table.meta.schema.arrow_schema().fields())
    {
        let mut values = ValueSize::default();
        let source_index = source.get(column.name.as_str());
        if let Some(&index) = source_index {
            for row in &rows.rows {
                values.proto(row.values.get(index)?.value_data.as_ref())?;
            }
        } else if let Some(ColumnDefaultConstraint::Value(value)) = column.default_constraint() {
            values.default_value(value)?;
            values.payload = values.payload.checked_mul(rows.rows.len())?;
            values.items = values.items.checked_mul(rows.rows.len())?;
            values.scratch = values.scratch.checked_mul(rows.rows.len())?;
        }

        let count = rows.rows.len().checked_add(values.items)?;
        let extension = column.extension_type::<Json2ExtensionType>().ok()?;
        // Only supplied v2 JSON2 values use the settings-aware builder; defaults use the datatype.
        let settings = extension
            .as_ref()
            .filter(|extension| {
                source_index.is_some()
                    && column.data_type.is_json2()
                    && extension.metadata().is_version_2()
            })
            .map(|extension| extension.metadata().json_settings());
        let (mut bytes, paths) = if let Some(settings) = settings {
            let limit = settings
                .max_auto_expanded_paths()
                .unwrap_or(JSON2_DEFAULT_MAX_AUTO_EXPANDED_PATHS);
            (
                array_bytes(&json::json2_physical_data_type(settings), count)?,
                values.expanded_paths(usize::try_from(limit).ok()?)?,
            )
        } else {
            (array_bytes(field.data_type(), count)?, values.paths.len())
        };
        // Each selected path can need buffers for every row or nested element.
        let path_bytes = count
            .checked_add(1)?
            .checked_mul(48)?
            .checked_add(3 * ARRAY_OVERHEAD + VIEW_BLOCK_BYTES)?;
        bytes = bytes.checked_add(paths.checked_mul(path_bytes)?)?;
        // Builders can double capacity; payload and JSON scratch can coexist with
        // the final arrays. Fixed per-array slack covers alignment and empty buffers.
        total = total
            .checked_add(bytes.checked_mul(2)?)?
            .checked_add(field.size().checked_mul(2)?)?
            .checked_add(values.payload.checked_mul(4)?)?
            .checked_add(values.scratch)?;
    }
    Some(total)
}

fn array_bytes(data_type: &DataType, count: usize) -> Option<usize> {
    let width = match data_type {
        DataType::Null => 0,
        DataType::Boolean => 1,
        DataType::Utf8 | DataType::Binary | DataType::List(_) => 4,
        DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => 8,
        DataType::Utf8View | DataType::BinaryView => 16,
        DataType::Struct(_) | DataType::FixedSizeList(_, _) | DataType::Dictionary(_, _) => 0,
        DataType::FixedSizeBinary(width) => usize::try_from(*width).ok()?,
        other => other.primitive_width()?,
    };
    // A byte per validity bit is deliberately conservative, including null padding.
    let mut bytes = count
        .checked_add(1)?
        .checked_mul(width.checked_add(1)?)?
        .checked_add(ARRAY_OVERHEAD)?;
    if matches!(data_type, DataType::Utf8View | DataType::BinaryView) {
        // Arrow starts out-of-line view data with an 8 KiB block.
        bytes = bytes.checked_add(VIEW_BLOCK_BYTES)?;
    }
    match data_type {
        DataType::Struct(fields) => {
            for field in fields {
                bytes = bytes.checked_add(array_bytes(field.data_type(), count)?)?;
            }
        }
        DataType::List(field) | DataType::LargeList(field) => {
            bytes = bytes.checked_add(array_bytes(field.data_type(), count)?)?;
        }
        DataType::FixedSizeList(field, len) => {
            bytes = bytes.checked_add(array_bytes(
                field.data_type(),
                count.checked_mul(usize::try_from(*len).ok()?)?,
            )?)?;
        }
        DataType::Dictionary(key, value) => {
            bytes = bytes
                .checked_add(array_bytes(key, count)?)?
                .checked_add(array_bytes(value, count)?)?;
        }
        _ => {}
    }
    Some(bytes)
}

#[derive(Default)]
struct ValueSize<'a> {
    payload: usize,
    items: usize,
    scratch: usize,
    paths: HashSet<Vec<PathComponent<'a>>>,
    path: Vec<PathComponent<'a>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum PathComponent<'a> {
    Key(&'a str),
    Field(usize),
    Element,
}

impl<'a> ValueSize<'a> {
    fn expanded_paths(&self, limit: usize) -> Option<usize> {
        if limit == 0 {
            return Some(0);
        }
        if self.paths.len() <= limit {
            return Some(self.paths.len());
        }

        // Arrays count as one selectable leaf, but all their element fields can expand.
        let mut path_sizes = HashMap::new();
        for path in &self.paths {
            let element = path
                .iter()
                .position(|part| matches!(part, PathComponent::Element));
            let leaf = &path[..element.unwrap_or(path.len())];
            let size = path_sizes.entry(leaf).or_insert(leaf.len());
            if element.is_some() {
                *size = size.checked_add(1)?;
            }
        }
        // The largest candidates bound any frequency-based selection, including parent structs.
        let mut sizes = path_sizes.into_values().collect::<Vec<_>>();
        sizes.sort_unstable_by(|a, b| b.cmp(a));
        let count = sizes
            .into_iter()
            .take(limit)
            .try_fold(0usize, usize::checked_add)?;
        Some(count.min(self.paths.len()))
    }

    fn payload(&mut self, bytes: usize) -> Option<()> {
        self.payload = self.payload.checked_add(bytes)?;
        Some(())
    }

    fn children(&mut self, len: usize, array: bool) -> Option<()> {
        if array {
            self.items = self.items.checked_add(len)?;
        }
        self.scratch = self
            .scratch
            .checked_add(len.checked_mul(VALUE_SCRATCH_BYTES)?)?;
        Some(())
    }

    fn variant(&mut self, bytes: usize) -> Option<()> {
        self.payload(bytes)?;
        // Opaque JSONB is decoded into a tree when written to a Variant remainder.
        self.scratch = self.scratch.checked_add(bytes.checked_mul(64)?)?;
        Some(())
    }

    fn proto(&mut self, value: Option<&'a ValueData>) -> Option<()> {
        match value {
            Some(ValueData::StringValue(value)) => self.payload(value.len())?,
            Some(ValueData::BinaryValue(value)) => self.payload(value.len())?,
            Some(ValueData::ListValue(value)) => {
                self.children(value.items.len(), true)?;
                self.path.push(PathComponent::Element);
                for value in &value.items {
                    self.proto(value.value_data.as_ref())?;
                }
                self.path.pop();
            }
            Some(ValueData::StructValue(value)) => {
                self.children(value.items.len(), false)?;
                for (index, value) in value.items.iter().enumerate() {
                    self.path.push(PathComponent::Field(index));
                    self.proto(value.value_data.as_ref())?;
                    self.path.pop();
                }
            }
            Some(ValueData::JsonValue(value)) => self.json(value)?,
            _ => {}
        }
        Some(())
    }

    fn json(&mut self, value: &'a JsonValue) -> Option<()> {
        if !self.paths.contains(&self.path) {
            self.paths.insert(self.path.clone());
        }
        self.scratch = self.scratch.checked_add(VALUE_SCRATCH_BYTES)?;
        match value.value.as_ref() {
            Some(json_value::Value::Str(value)) => self.payload(value.len())?,
            Some(json_value::Value::Variant(value)) => self.variant(value.len())?,
            Some(json_value::Value::Array(value)) => {
                self.items = self.items.checked_add(value.items.len())?;
                self.path.push(PathComponent::Element);
                for value in &value.items {
                    self.json(value)?;
                }
                self.path.pop();
            }
            Some(json_value::Value::Object(value)) => {
                for entry in &value.entries {
                    self.payload(entry.key.len())?;
                    self.path.push(PathComponent::Key(&entry.key));
                    if let Some(value) = &entry.value {
                        self.json(value)?;
                    }
                    self.path.pop();
                }
            }
            _ => {}
        }
        Some(())
    }

    fn default_value(&mut self, value: &'a Value) -> Option<()> {
        match value {
            Value::String(value) => self.payload(value.len())?,
            Value::Binary(value) => self.payload(value.len())?,
            Value::List(value) => {
                self.children(value.items().len(), true)?;
                self.path.push(PathComponent::Element);
                for value in value.items() {
                    self.default_value(value)?;
                }
                self.path.pop();
            }
            Value::Struct(value) => {
                self.children(value.items().len(), false)?;
                for (index, value) in value.items().iter().enumerate() {
                    self.path.push(PathComponent::Field(index));
                    self.default_value(value)?;
                    self.path.pop();
                }
            }
            Value::Json(value) => self.default_json(value.variant())?,
            _ => {}
        }
        Some(())
    }

    fn default_json(&mut self, value: &'a JsonVariant) -> Option<()> {
        if !self.paths.contains(&self.path) {
            self.paths.insert(self.path.clone());
        }
        self.scratch = self.scratch.checked_add(VALUE_SCRATCH_BYTES)?;
        match value {
            JsonVariant::String(value) => self.payload(value.len())?,
            JsonVariant::Variant(value) => self.variant(value.len())?,
            JsonVariant::Array(values) => {
                self.items = self.items.checked_add(values.len())?;
                self.path.push(PathComponent::Element);
                for value in values {
                    self.default_json(value)?;
                }
                self.path.pop();
            }
            JsonVariant::Object(values) => {
                for (key, value) in values {
                    self.payload(key.len())?;
                    self.path.push(PathComponent::Key(key));
                    self.default_json(value)?;
                    self.path.pop();
                }
            }
            _ => {}
        }
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_overflow_fails_closed() {
        assert!(array_bytes(&DataType::Utf8, usize::MAX).is_none());
        let mut values = ValueSize::default();
        assert!(values.payload(usize::MAX).is_some());
        assert!(values.payload(1).is_none());
        assert!(values.children(usize::MAX, true).is_none());
    }
}
