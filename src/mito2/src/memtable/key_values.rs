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

use api::v1::{ColumnSchema, Mutation, OpType, Row, Rows};
use datatypes::value::ValueRef;
use store_api::metadata::RegionMetadata;
use store_api::storage::SequenceNumber;

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
        let schema = &rows.schema;
        rows.rows.iter().enumerate().map(|(idx, row)| {
            KeyValue {
                row,
                schema,
                helper: &self.helper,
                sequence: self.mutation.sequence + idx as u64, // Calculate sequence for each row.
                // Safety: This is a valid mutation.
                op_type: OpType::try_from(self.mutation.op_type).unwrap(),
            }
        })
    }

    /// Returns number of rows.
    pub fn num_rows(&self) -> usize {
        // Safety: rows is not None.
        self.mutation.rows.as_ref().unwrap().rows.len()
    }
}

/// Key value view of a row.
///
/// A key value view divides primary key columns and field (value) columns.
/// Primary key columns have the same order as region's primary key. Field
/// columns are ordered by their position in the region schema (The same order
/// as users defined while creating the region).
#[derive(Debug)]
pub struct KeyValue<'a> {
    row: &'a Row,
    schema: &'a Vec<ColumnSchema>,
    helper: &'a ReadRowHelper,
    sequence: SequenceNumber,
    op_type: OpType,
}

impl<'a> KeyValue<'a> {
    /// Get primary key columns.
    pub fn primary_keys(&self) -> impl Iterator<Item = ValueRef> {
        self.helper.indices[..self.helper.num_primary_key_column]
            .iter()
            .map(|idx| {
                api::helper::pb_value_to_value_ref(
                    &self.row.values[*idx],
                    &self.schema[*idx].datatype_extension,
                )
            })
    }

    /// Get field columns.
    pub fn fields(&self) -> impl Iterator<Item = ValueRef> {
        self.helper.indices[self.helper.num_primary_key_column + 1..]
            .iter()
            .map(|idx| {
                api::helper::pb_value_to_value_ref(
                    &self.row.values[*idx],
                    &self.schema[*idx].datatype_extension,
                )
            })
    }

    /// Get timestamp.
    pub fn timestamp(&self) -> ValueRef {
        // Timestamp is primitive, we clone it.
        let index = self.helper.indices[self.helper.num_primary_key_column];
        api::helper::pb_value_to_value_ref(
            &self.row.values[index],
            &self.schema[index].datatype_extension,
        )
    }

    /// Get number of primary key columns.
    pub fn num_primary_keys(&self) -> usize {
        self.helper.num_primary_key_column
    }

    /// Get number of field columns.
    pub fn num_fields(&self) -> usize {
        self.row.values.len() - self.helper.num_primary_key_column - 1
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
        for column in metadata.field_columns() {
            // Get index in request for each field column.
            let index = name_to_index.get(&column.column_schema.name).unwrap();
            indices.push(*index);
        }

        ReadRowHelper {
            indices,
            num_primary_key_column: metadata.primary_key.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::{self, ColumnDataType, SemanticType};

    use super::*;
    use crate::test_util::i64_value;
    use crate::test_util::meta_util::TestRegionMetadataBuilder;

    const TS_NAME: &str = "ts";
    const START_SEQ: SequenceNumber = 100;

    /// Creates a region: `ts, k0, k1, ..., v0, v1, ...`
    fn new_region_metadata(num_tags: usize, num_fields: usize) -> RegionMetadata {
        TestRegionMetadataBuilder::default()
            .ts_name(TS_NAME)
            .num_tags(num_tags)
            .num_fields(num_fields)
            .build()
    }

    /// Creates rows `[ 0, 1, ..., n ] x num_rows`
    fn new_rows(column_names: &[&str], num_rows: usize) -> Rows {
        let mut rows = Vec::with_capacity(num_rows);
        for _ in 0..num_rows {
            // For simplicity, we use i64 for timestamp millisecond type. This violates the column schema
            // but it's acceptable for tests.
            let values: Vec<_> = (0..column_names.len())
                .map(|idx| i64_value(idx as i64))
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
                let semantic_type = if column_name.starts_with('k') {
                    SemanticType::Tag as i32
                } else if column_name.starts_with('v') {
                    SemanticType::Field as i32
                } else {
                    SemanticType::Timestamp as i32
                };
                v1::ColumnSchema {
                    column_name: column_name.to_string(),
                    datatype,
                    semantic_type,
                    ..Default::default()
                }
            })
            .collect();

        Rows { rows, schema }
    }

    fn new_mutation(column_names: &[&str], num_rows: usize) -> Mutation {
        let rows = new_rows(column_names, num_rows);
        Mutation {
            op_type: OpType::Put as i32,
            sequence: START_SEQ,
            rows: Some(rows),
        }
    }

    fn check_key_values(kvs: &KeyValues, num_rows: usize, keys: &[i64], ts: i64, values: &[i64]) {
        assert_eq!(num_rows, kvs.num_rows());
        let mut expect_seq = START_SEQ;
        let expect_ts = ValueRef::Int64(ts);
        for kv in kvs.iter() {
            assert_eq!(expect_seq, kv.sequence());
            expect_seq += 1;
            assert_eq!(OpType::Put, kv.op_type);
            assert_eq!(keys.len(), kv.num_primary_keys());
            assert_eq!(values.len(), kv.num_fields());

            assert_eq!(expect_ts, kv.timestamp());
            let expect_keys: Vec<_> = keys.iter().map(|k| ValueRef::Int64(*k)).collect();
            let actual_keys: Vec<_> = kv.primary_keys().collect();
            assert_eq!(expect_keys, actual_keys);
            let expect_values: Vec<_> = values.iter().map(|v| ValueRef::Int64(*v)).collect();
            let actual_values: Vec<_> = kv.fields().collect();
            assert_eq!(expect_values, actual_values);
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
        let kvs = KeyValues::new(&meta, mutation);
        assert!(kvs.is_none());
    }

    #[test]
    fn test_ts_only() {
        let meta = new_region_metadata(0, 0);
        let mutation = new_mutation(&["ts"], 2);
        let kvs = KeyValues::new(&meta, mutation).unwrap();
        check_key_values(&kvs, 2, &[], 0, &[]);
    }

    #[test]
    fn test_no_field() {
        let meta = new_region_metadata(2, 0);
        // The value of each row:
        // k1=0, ts=1, k0=2,
        let mutation = new_mutation(&["k1", "ts", "k0"], 3);
        let kvs = KeyValues::new(&meta, mutation).unwrap();
        // KeyValues
        // keys: [k0=2, k1=0]
        // ts: 1,
        check_key_values(&kvs, 3, &[2, 0], 1, &[]);
    }

    #[test]
    fn test_no_tag() {
        let meta = new_region_metadata(0, 2);
        // The value of each row:
        // v1=0, v0=1, ts=2,
        let mutation = new_mutation(&["v1", "v0", "ts"], 3);
        let kvs = KeyValues::new(&meta, mutation).unwrap();
        // KeyValues (note that v0 is in front of v1 in region schema)
        // ts: 2,
        // fields: [v0=1, v1=0]
        check_key_values(&kvs, 3, &[], 2, &[1, 0]);
    }

    #[test]
    fn test_tag_field() {
        let meta = new_region_metadata(2, 2);
        // The value of each row:
        // k0=0, v0=1, ts=2, k1=3, v1=4,
        let mutation = new_mutation(&["k0", "v0", "ts", "k1", "v1"], 3);
        let kvs = KeyValues::new(&meta, mutation).unwrap();
        // KeyValues
        // keys: [k0=0, k1=3]
        // ts: 2,
        // fields: [v0=1, v1=4]
        check_key_values(&kvs, 3, &[0, 3], 2, &[1, 4]);
    }
}
