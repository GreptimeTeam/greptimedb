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

use greptime_proto::v1::mito::{Mutation, OpType};
use greptime_proto::v1::{Row, Rows, Value};
use store_api::storage::SequenceNumber;

use crate::metadata::{RegionMetadata, SemanticType};

/// Key value view of a mutation.
#[derive(Debug)]
pub struct KeyValues {
    /// Mutation to read.
    ///
    /// This mutation must be a valid mutation and rows in the mutation
    /// must not be `None`.
    mutation: Mutation,
    /// Key value read helper.
    helper: ReadRowHelper,
}

impl KeyValues {
    /// Creates [KeyValues] from specific `mutation`.
    ///
    /// Returns `None` if `rows` of the `mutation` is `None`.
    pub fn new(metadata: &RegionMetadata, mutation: Mutation) -> Option<KeyValues> {
        let rows = mutation.rows.as_ref()?;
        let helper = ReadRowHelper::new(metadata, rows);

        Some(KeyValues { mutation, helper })
    }

    /// Returns a key value iterator.
    pub fn iter(&self) -> impl Iterator<Item = KeyValue> {
        let rows = self.mutation.rows.as_ref().unwrap();
        rows.rows.iter().enumerate().map(|(idx, row)| {
            KeyValue {
                row,
                helper: &self.helper,
                sequence: self.mutation.sequence + idx as u64, // Calculate sequence for each row.
                // Safety: This is a valid mutation.
                op_type: OpType::from_i32(self.mutation.op_type).unwrap(),
            }
        })
    }
}

/// Key value view of a row.
///
/// A key value view divides primary key columns and field (value) columns.
#[derive(Debug)]
pub struct KeyValue<'a> {
    row: &'a Row,
    helper: &'a ReadRowHelper,
    sequence: SequenceNumber,
    op_type: OpType,
}

impl<'a> KeyValue<'a> {
    /// Get primary key columns.
    pub fn primary_keys(&self) -> impl Iterator<Item = &Value> {
        self.helper.indices[..self.helper.num_primary_key_column]
            .iter()
            .map(|idx| &self.row.values[*idx])
    }

    /// Get field columns.
    pub fn fields(&self) -> impl Iterator<Item = &Value> {
        self.helper.indices[self.helper.num_primary_key_column + 1..]
            .iter()
            .map(|idx| &self.row.values[*idx])
    }

    /// Get sequence.
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Get op type.
    pub fn op_type(&self) -> OpType {
        self.op_type
    }
}

/// Helper to read rows in key, value order.
#[derive(Debug)]
struct ReadRowHelper {
    /// Key and value column indices.
    ///
    /// `indices[..num_primary_key_column]` are primary key columns, `indices[num_primary_key_column]`
    /// is the timestamp column and remainings are field columns.
    indices: Vec<usize>,
    /// Number of primary key columns.
    num_primary_key_column: usize,
}

impl ReadRowHelper {
    /// Creates a [ReadRowHelper] for specific `rows`.
    ///
    /// # Panics
    /// The `rows` must fill their missing columns first and have same columns with `metadata`.
    /// Otherwise this method will panic.
    fn new(metadata: &RegionMetadata, rows: &Rows) -> ReadRowHelper {
        assert_eq!(
            metadata.column_metadatas.len(),
            rows.schema.len(),
            "Length mismatch, column_metas: {:?}, rows_schema: {:?}",
            metadata.column_metadatas,
            rows.schema
        );

        // Build a name to index mapping for rows.
        let name_to_index: HashMap<_, _> = rows
            .schema
            .iter()
            .enumerate()
            .map(|(index, col)| (&col.column_name, index))
            .collect();
        let mut indices = Vec::with_capacity(metadata.column_metadatas.len());

        // Get primary key indices.
        for pk_id in &metadata.primary_key {
            // Safety: Id comes from primary key.
            let column = metadata.column_by_id(*pk_id).unwrap();
            let index = name_to_index.get(&column.column_schema.name).unwrap();
            indices.push(*index);
        }
        // Get timestamp index.
        let ts_index = name_to_index
            .get(&metadata.time_index_column().column_schema.name)
            .unwrap();
        indices.push(*ts_index);
        // Iterate columns and find field columns.
        for column in metadata.column_metadatas.iter() {
            if column.semantic_type == SemanticType::Field {
                // Get index in request for each field column.
                let index = name_to_index.get(&column.column_schema.name).unwrap();
                indices.push(*index);
            }
        }

        ReadRowHelper {
            indices,
            num_primary_key_column: metadata.primary_key.len(),
        }
    }
}

// TODO(yingwen): Test key values.
#[cfg(test)]
mod tests {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use greptime_proto::v1;
    use greptime_proto::v1::{value, ColumnDataType, Value};
    use store_api::storage::RegionId;

    use super::*;
    use crate::metadata::{ColumnMetadata, RegionMetadataBuilder};

    const TS_NAME: &str = "ts";

    /// Creates a region: `ts, k0, k1, ..., v0, v1, ...`
    fn new_region_metadata(num_tag: usize, num_field: usize) -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1), 1);
        let mut column_id = 0;
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                TS_NAME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id,
        });
        // For simplicity, we use the same data type for tag/field columns.
        let mut primary_key = Vec::with_capacity(num_tag);
        for i in 0..num_tag {
            column_id += 1;
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    format!("k{i}"),
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id,
            });
            primary_key.push(i as u32 + 1);
        }
        for i in 0..num_field {
            column_id += 1;
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    format!("v{i}"),
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id,
            });
        }
        builder.primary_key(primary_key);
        builder.build().unwrap()
    }

    /// Creates rows `[ 0, 1, ..., n ] x num_rows`
    fn new_rows(column_names: &[&str], num_rows: usize) -> Rows {
        let mut rows = Vec::with_capacity(num_rows);
        for _ in 0..num_rows {
            // For simplicity, we use i64 for timestamp millisecond type. This violates the column schema
            // but it's acceptable for tests.
            let values: Vec<_> = (0..column_names.len())
                .into_iter()
                .map(|idx| Value {
                    value: Some(value::Value::I64Value(idx as i64)),
                })
                .collect();
            rows.push(Row { values });
        }

        let schema = column_names
            .iter()
            .map(|column_name| {
                let datatype = if *column_name == TS_NAME {
                    ColumnDataType::TimestampMillisecond as i32
                } else {
                    ColumnDataType::Int64 as i32
                };
                let semantic_type = if column_name.starts_with("k") {
                    SemanticType::Tag as i32
                } else if column_name.starts_with("v") {
                    SemanticType::Field as i32
                } else {
                    SemanticType::Timestamp as i32
                };
                v1::ColumnSchema {
                    column_name: column_name.to_string(),
                    datatype,
                    semantic_type,
                }
            })
            .collect();

        Rows { rows, schema }
    }

    fn new_mutation(column_names: &[&str], num_rows: usize) -> Mutation {
        let rows = new_rows(column_names, num_rows);
        Mutation {
            op_type: OpType::Put as i32,
            sequence: 100,
            rows: Some(rows),
        }
    }

    #[test]
    fn test_empty_key_values() {
        let meta = new_region_metadata(1, 1);
        let mutation = Mutation {
            op_type: OpType::Put as i32,
            sequence: 100,
            rows: None,
        };
        let key_values = KeyValues::new(&meta, mutation);
        assert!(key_values.is_none());
    }
}
