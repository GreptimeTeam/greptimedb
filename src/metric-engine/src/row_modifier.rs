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

use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
use datatypes::value::ValueRef;
use fxhash::FxHasher;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::storage::consts::{PRIMARY_KEY_COLUMN_NAME, ReservedColumnId};
use store_api::storage::{ColumnId, TableId};

use crate::error::{EncodePrimaryKeySnafu, Result};

/// A row modifier modifies [`Rows`].
///
/// - For [`PrimaryKeyEncoding::Sparse`] encoding,
///   it replaces the primary key columns with the encoded primary key column(`__primary_key`).
///
/// - For [`PrimaryKeyEncoding::Dense`] encoding,
///   it adds two columns(`__table_id`, `__tsid`) to the row.
pub struct RowModifier {
    codec: SparsePrimaryKeyCodec,
}

impl Default for RowModifier {
    fn default() -> Self {
        Self {
            codec: SparsePrimaryKeyCodec::schemaless(),
        }
    }
}

impl RowModifier {
    /// Modify rows with the given primary key encoding.
    pub(crate) fn modify_rows(
        &self,
        iter: RowsIter,
        table_id: TableId,
        encoding: PrimaryKeyEncoding,
    ) -> Result<Rows> {
        match encoding {
            PrimaryKeyEncoding::Sparse => self.modify_rows_sparse(iter, table_id),
            PrimaryKeyEncoding::Dense => self.modify_rows_dense(iter, table_id),
        }
    }

    /// Modifies rows with sparse primary key encoding.
    /// It replaces the primary key columns with the encoded primary key column(`__primary_key`).
    fn modify_rows_sparse(&self, mut iter: RowsIter, table_id: TableId) -> Result<Rows> {
        let num_column = iter.rows.schema.len();
        let num_primary_key_column = iter.index.num_primary_key_column;
        // num_output_column = remaining columns(fields columns + timestamp column) + 1 (encoded primary key column)
        let num_output_column = num_column - num_primary_key_column + 1;

        let mut buffer = vec![];

        for mut iter in iter.iter_mut() {
            let (table_id, tsid) = Self::fill_internal_columns(table_id, &iter);
            let mut values = Vec::with_capacity(num_output_column);
            buffer.clear();
            let internal_columns = [
                (
                    ReservedColumnId::table_id(),
                    api::helper::pb_value_to_value_ref(&table_id, None),
                ),
                (
                    ReservedColumnId::tsid(),
                    api::helper::pb_value_to_value_ref(&tsid, None),
                ),
            ];
            self.codec
                .encode_to_vec(internal_columns.into_iter(), &mut buffer)
                .context(EncodePrimaryKeySnafu)?;
            self.codec
                .encode_to_vec(iter.primary_keys(), &mut buffer)
                .context(EncodePrimaryKeySnafu)?;

            values.push(ValueData::BinaryValue(buffer.clone()).into());
            values.extend(iter.remaining());
            // Replace the row with the encoded row
            *iter.row = Row { values };
        }

        // Update the schema
        let mut schema = Vec::with_capacity(num_output_column);
        schema.push(ColumnSchema {
            column_name: PRIMARY_KEY_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Binary as i32,
            semantic_type: SemanticType::Tag as _,
            datatype_extension: None,
            options: None,
        });
        schema.extend(iter.remaining_columns());
        iter.rows.schema = schema;

        Ok(iter.rows)
    }

    /// Modifies rows with dense primary key encoding.
    /// It adds two columns(`__table_id`, `__tsid`) to the row.
    fn modify_rows_dense(&self, mut iter: RowsIter, table_id: TableId) -> Result<Rows> {
        // add table_name column
        iter.rows.schema.push(ColumnSchema {
            column_name: DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Uint32 as i32,
            semantic_type: SemanticType::Tag as _,
            datatype_extension: None,
            options: None,
        });
        // add tsid column
        iter.rows.schema.push(ColumnSchema {
            column_name: DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Uint64 as i32,
            semantic_type: SemanticType::Tag as _,
            datatype_extension: None,
            options: None,
        });
        for iter in iter.iter_mut() {
            let (table_id, tsid) = Self::fill_internal_columns(table_id, &iter);
            iter.row.values.push(table_id);
            iter.row.values.push(tsid);
        }

        Ok(iter.rows)
    }

    /// Fills internal columns of a row with table name and a hash of tag values.
    pub fn fill_internal_columns(table_id: TableId, iter: &RowIter<'_>) -> (Value, Value) {
        let ts_id = if !iter.has_null_labels() {
            // No null labels in row, we can safely reuse the precomputed label name hash.
            let mut ts_id_gen = TsidGenerator::new(iter.index.label_name_hash);
            for (_, value) in iter.primary_keys_with_name() {
                // The type is checked before. So only null is ignored.
                if let Some(ValueData::StringValue(string)) = &value.value_data {
                    ts_id_gen.write_str(string);
                } else {
                    unreachable!(
                        "Should not contain null or non-string value: {:?}, table id: {}",
                        value, table_id
                    );
                }
            }
            ts_id_gen.finish()
        } else {
            // Slow path: row contains null, recompute label hash
            let mut hasher = TsidGenerator::default();
            // 1. Find out label names with non-null values and get the hash.
            for (name, value) in iter.primary_keys_with_name() {
                // The type is checked before. So only null is ignored.
                if let Some(ValueData::StringValue(_)) = &value.value_data {
                    hasher.write_str(name);
                }
            }
            let label_name_hash = hasher.finish();

            // 2. Use label name hash as seed and continue with label values.
            let mut final_hasher = TsidGenerator::new(label_name_hash);
            for (_, value) in iter.primary_keys_with_name() {
                if let Some(ValueData::StringValue(value)) = &value.value_data {
                    final_hasher.write_str(value);
                }
            }
            final_hasher.finish()
        };

        (
            ValueData::U32Value(table_id).into(),
            ValueData::U64Value(ts_id).into(),
        )
    }
}

/// Tsid generator.
pub struct TsidGenerator {
    hasher: FxHasher,
}

impl Default for TsidGenerator {
    fn default() -> Self {
        Self {
            hasher: FxHasher::default(),
        }
    }
}

impl TsidGenerator {
    pub fn new(label_name_hash: u64) -> Self {
        let mut hasher = FxHasher::default();
        hasher.write_u64(label_name_hash);
        Self { hasher }
    }

    /// Writes a label pair to the generator.
    pub fn write_str(&mut self, value: &str) {
        self.hasher.write(value.as_bytes());
        self.hasher.write_u8(0xff);
    }

    /// Generates a new TSID.
    pub fn finish(&mut self) -> u64 {
        self.hasher.finish()
    }
}

/// Index of a value.
#[derive(Debug, Clone, Copy)]
struct ValueIndex {
    column_id: ColumnId,
    index: usize,
}

/// Index of a row.
struct IterIndex {
    indices: Vec<ValueIndex>,
    num_primary_key_column: usize,
    /// Precomputed hash for label names.
    label_name_hash: u64,
}

impl IterIndex {
    fn new(row_schema: &[ColumnSchema], name_to_column_id: &HashMap<String, ColumnId>) -> Self {
        let mut reserved_indices = SmallVec::<[ValueIndex; 2]>::new();
        // Uses BTreeMap to keep the primary key column name order (lexicographical)
        let mut primary_key_indices = BTreeMap::new();
        let mut field_indices = SmallVec::<[ValueIndex; 1]>::new();
        let mut ts_index = None;
        for (idx, col) in row_schema.iter().enumerate() {
            match col.semantic_type() {
                SemanticType::Tag => match col.column_name.as_str() {
                    DATA_SCHEMA_TABLE_ID_COLUMN_NAME => {
                        reserved_indices.push(ValueIndex {
                            column_id: ReservedColumnId::table_id(),
                            index: idx,
                        });
                    }
                    DATA_SCHEMA_TSID_COLUMN_NAME => {
                        reserved_indices.push(ValueIndex {
                            column_id: ReservedColumnId::tsid(),
                            index: idx,
                        });
                    }
                    _ => {
                        // Inserts primary key column name follower the column name order (lexicographical)
                        primary_key_indices.insert(
                            col.column_name.as_str(),
                            ValueIndex {
                                column_id: *name_to_column_id.get(&col.column_name).unwrap(),
                                index: idx,
                            },
                        );
                    }
                },
                SemanticType::Field => {
                    field_indices.push(ValueIndex {
                        column_id: *name_to_column_id.get(&col.column_name).unwrap(),
                        index: idx,
                    });
                }
                SemanticType::Timestamp => {
                    ts_index = Some(ValueIndex {
                        column_id: *name_to_column_id.get(&col.column_name).unwrap(),
                        index: idx,
                    });
                }
            }
        }
        let num_primary_key_column = primary_key_indices.len() + reserved_indices.len();
        let mut indices = Vec::with_capacity(num_primary_key_column + 2);
        indices.extend(reserved_indices.into_iter());
        let mut label_name_hasher = TsidGenerator::default();
        for (pk_name, pk_index) in primary_key_indices {
            // primary_key_indices already sorted.
            label_name_hasher.write_str(pk_name);
            indices.push(pk_index);
        }
        let label_name_hash = label_name_hasher.finish();

        indices.extend(ts_index);
        indices.extend(field_indices);
        IterIndex {
            indices,
            num_primary_key_column,
            label_name_hash,
        }
    }
}

/// Iterator of rows.
pub struct RowsIter {
    rows: Rows,
    index: IterIndex,
}

impl RowsIter {
    pub fn new(rows: Rows, name_to_column_id: &HashMap<String, ColumnId>) -> Self {
        let index: IterIndex = IterIndex::new(&rows.schema, name_to_column_id);
        Self { rows, index }
    }

    /// Returns the iterator of rows.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = RowIter<'_>> {
        self.rows.rows.iter_mut().map(|row| RowIter {
            row,
            index: &self.index,
            schema: &self.rows.schema,
        })
    }

    /// Returns the remaining columns.
    fn remaining_columns(&mut self) -> impl Iterator<Item = ColumnSchema> + '_ {
        self.index.indices[self.index.num_primary_key_column..]
            .iter()
            .map(|idx| std::mem::take(&mut self.rows.schema[idx.index]))
    }
}

/// Iterator of a row.
pub struct RowIter<'a> {
    row: &'a mut Row,
    index: &'a IterIndex,
    schema: &'a Vec<ColumnSchema>,
}

impl RowIter<'_> {
    /// Returns the primary keys with their names.
    fn primary_keys_with_name(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.index.indices[..self.index.num_primary_key_column]
            .iter()
            .map(|idx| {
                (
                    &self.schema[idx.index].column_name,
                    &self.row.values[idx.index],
                )
            })
    }

    /// Returns true if any label in current row is null.
    fn has_null_labels(&self) -> bool {
        self.index.indices[..self.index.num_primary_key_column]
            .iter()
            .any(|idx| self.row.values[idx.index].value_data.is_none())
    }

    /// Returns the primary keys.
    pub fn primary_keys(&self) -> impl Iterator<Item = (ColumnId, ValueRef<'_>)> {
        self.index.indices[..self.index.num_primary_key_column]
            .iter()
            .map(|idx| {
                (
                    idx.column_id,
                    api::helper::pb_value_to_value_ref(
                        &self.row.values[idx.index],
                        self.schema[idx.index].datatype_extension.as_ref(),
                    ),
                )
            })
    }

    /// Returns the remaining columns.
    fn remaining(&mut self) -> impl Iterator<Item = Value> + '_ {
        self.index.indices[self.index.num_primary_key_column..]
            .iter()
            .map(|idx| std::mem::take(&mut self.row.values[idx.index]))
    }

    /// Returns value at given offset.
    /// # Panics
    /// Panics if offset out-of-bound
    pub fn value_at(&self, idx: usize) -> &Value {
        &self.row.values[idx]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::{Row, Rows};

    use super::*;

    fn test_schema() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                column_name: "namespace".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as _,
                datatype_extension: None,
                options: None,
            },
            ColumnSchema {
                column_name: "host".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as _,
                datatype_extension: None,
                options: None,
            },
        ]
    }

    fn test_row(v1: &str, v2: &str) -> Row {
        Row {
            values: vec![
                ValueData::StringValue(v1.to_string()).into(),
                ValueData::StringValue(v2.to_string()).into(),
            ],
        }
    }

    fn test_name_to_column_id() -> HashMap<String, ColumnId> {
        HashMap::from([("namespace".to_string(), 1), ("host".to_string(), 2)])
    }

    #[test]
    fn test_encode_sparse() {
        let name_to_column_id = test_name_to_column_id();
        let encoder = RowModifier::default();
        let table_id = 1025;
        let schema = test_schema();
        let row = test_row("greptimedb", "127.0.0.1");
        let rows = Rows {
            schema,
            rows: vec![row],
        };
        let rows_iter = RowsIter::new(rows, &name_to_column_id);
        let result = encoder.modify_rows_sparse(rows_iter, table_id).unwrap();
        assert_eq!(result.rows[0].values.len(), 1);
        let encoded_primary_key = vec![
            128, 0, 0, 4, 1, 0, 0, 4, 1, 128, 0, 0, 3, 1, 37, 196, 242, 181, 117, 224, 7, 137, 0,
            0, 0, 2, 1, 1, 49, 50, 55, 46, 48, 46, 48, 46, 9, 49, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
            1, 1, 1, 103, 114, 101, 112, 116, 105, 109, 101, 9, 100, 98, 0, 0, 0, 0, 0, 0, 2,
        ];
        assert_eq!(
            result.rows[0].values[0],
            ValueData::BinaryValue(encoded_primary_key).into()
        );
        assert_eq!(result.schema, expected_sparse_schema());
    }

    fn expected_sparse_schema() -> Vec<ColumnSchema> {
        vec![ColumnSchema {
            column_name: PRIMARY_KEY_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Binary as i32,
            semantic_type: SemanticType::Tag as _,
            datatype_extension: None,
            options: None,
        }]
    }

    fn expected_dense_schema() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                column_name: "namespace".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as _,
                datatype_extension: None,
                options: None,
            },
            ColumnSchema {
                column_name: "host".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as _,
                datatype_extension: None,
                options: None,
            },
            ColumnSchema {
                column_name: DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint32 as i32,
                semantic_type: SemanticType::Tag as _,
                datatype_extension: None,
                options: None,
            },
            ColumnSchema {
                column_name: DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64 as i32,
                semantic_type: SemanticType::Tag as _,
                datatype_extension: None,
                options: None,
            },
        ]
    }

    #[test]
    fn test_encode_dense() {
        let name_to_column_id = test_name_to_column_id();
        let encoder = RowModifier::default();
        let table_id = 1025;
        let schema = test_schema();
        let row = test_row("greptimedb", "127.0.0.1");
        let rows = Rows {
            schema,
            rows: vec![row],
        };
        let rows_iter = RowsIter::new(rows, &name_to_column_id);
        let result = encoder.modify_rows_dense(rows_iter, table_id).unwrap();
        assert_eq!(
            result.rows[0].values[0],
            ValueData::StringValue("greptimedb".to_string()).into()
        );
        assert_eq!(
            result.rows[0].values[1],
            ValueData::StringValue("127.0.0.1".to_string()).into()
        );
        assert_eq!(result.rows[0].values[2], ValueData::U32Value(1025).into());
        assert_eq!(
            result.rows[0].values[3],
            ValueData::U64Value(2721566936019240841).into()
        );
        assert_eq!(result.schema, expected_dense_schema());
    }

    #[test]
    fn test_fill_internal_columns() {
        let name_to_column_id = test_name_to_column_id();
        let table_id = 1025;
        let schema = test_schema();
        let row = test_row("greptimedb", "127.0.0.1");
        let rows = Rows {
            schema,
            rows: vec![row],
        };
        let mut rows_iter = RowsIter::new(rows, &name_to_column_id);
        let row_iter = rows_iter.iter_mut().next().unwrap();
        let (encoded_table_id, tsid) = RowModifier::fill_internal_columns(table_id, &row_iter);
        assert_eq!(encoded_table_id, ValueData::U32Value(1025).into());
        assert_eq!(tsid, ValueData::U64Value(2721566936019240841).into());

        // Change the column order
        let schema = vec![
            ColumnSchema {
                column_name: "host".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as _,
                datatype_extension: None,
                options: None,
            },
            ColumnSchema {
                column_name: "namespace".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as _,
                datatype_extension: None,
                options: None,
            },
        ];
        let row = test_row("127.0.0.1", "greptimedb");
        let rows = Rows {
            schema,
            rows: vec![row],
        };
        let mut rows_iter = RowsIter::new(rows, &name_to_column_id);
        let row_iter = rows_iter.iter_mut().next().unwrap();
        let (encoded_table_id, tsid) = RowModifier::fill_internal_columns(table_id, &row_iter);
        assert_eq!(encoded_table_id, ValueData::U32Value(1025).into());
        assert_eq!(tsid, ValueData::U64Value(2721566936019240841).into());
    }

    /// Helper function to create a schema with multiple label columns
    fn create_multi_label_schema(labels: &[&str]) -> Vec<ColumnSchema> {
        labels
            .iter()
            .map(|name| ColumnSchema {
                column_name: name.to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as _,
                datatype_extension: None,
                options: None,
            })
            .collect()
    }

    /// Helper function to create a name_to_column_id map
    fn create_name_to_column_id(labels: &[&str]) -> HashMap<String, ColumnId> {
        labels
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.to_string(), idx as ColumnId + 1))
            .collect()
    }

    /// Helper function to create a row with string values
    fn create_row_with_values(values: &[&str]) -> Row {
        Row {
            values: values
                .iter()
                .map(|v| ValueData::StringValue(v.to_string()).into())
                .collect(),
        }
    }

    /// Helper function to create a row with some null values
    fn create_row_with_nulls(values: &[Option<&str>]) -> Row {
        Row {
            values: values
                .iter()
                .map(|v| {
                    v.map(|s| ValueData::StringValue(s.to_string()).into())
                        .unwrap_or_else(|| Value { value_data: None })
                })
                .collect(),
        }
    }

    /// Helper function to extract TSID from a row
    fn extract_tsid(
        schema: Vec<ColumnSchema>,
        row: Row,
        name_to_column_id: &HashMap<String, ColumnId>,
        table_id: TableId,
    ) -> u64 {
        let rows = Rows {
            schema,
            rows: vec![row],
        };
        let mut rows_iter = RowsIter::new(rows, name_to_column_id);
        let row_iter = rows_iter.iter_mut().next().unwrap();
        let (_, tsid_value) = RowModifier::fill_internal_columns(table_id, &row_iter);
        match tsid_value.value_data {
            Some(ValueData::U64Value(tsid)) => tsid,
            _ => panic!("Expected U64Value for TSID"),
        }
    }

    #[test]
    fn test_tsid_same_for_different_label_orders() {
        // Test that rows with the same label name-value pairs but in different orders
        // produce the same TSID
        let table_id = 1025;

        // Schema 1: a, b, c
        let schema1 = create_multi_label_schema(&["a", "b", "c"]);
        let name_to_column_id1 = create_name_to_column_id(&["a", "b", "c"]);
        let row1 = create_row_with_values(&["A", "B", "C"]);
        let tsid1 = extract_tsid(schema1, row1, &name_to_column_id1, table_id);

        // Schema 2: b, a, c (different order)
        let schema2 = create_multi_label_schema(&["b", "a", "c"]);
        let name_to_column_id2 = create_name_to_column_id(&["a", "b", "c"]);
        let row2 = create_row_with_values(&["B", "A", "C"]);
        let tsid2 = extract_tsid(schema2, row2, &name_to_column_id2, table_id);

        // Schema 3: c, b, a (another different order)
        let schema3 = create_multi_label_schema(&["c", "b", "a"]);
        let name_to_column_id3 = create_name_to_column_id(&["a", "b", "c"]);
        let row3 = create_row_with_values(&["C", "B", "A"]);
        let tsid3 = extract_tsid(schema3, row3, &name_to_column_id3, table_id);

        // All should have the same TSID since label names are sorted lexicographically
        // and we're using the same label name-value pairs
        assert_eq!(
            tsid1, tsid2,
            "TSID should be same for different column orders"
        );
        assert_eq!(
            tsid2, tsid3,
            "TSID should be same for different column orders"
        );
    }

    #[test]
    fn test_tsid_same_with_null_labels() {
        // Test that rows that differ only by null label values produce the same TSID
        let table_id = 1025;

        // Row 1: a=A, b=B (no nulls, fast path)
        let schema1 = create_multi_label_schema(&["a", "b"]);
        let name_to_column_id1 = create_name_to_column_id(&["a", "b"]);
        let row1 = create_row_with_values(&["A", "B"]);
        let tsid1 = extract_tsid(schema1, row1, &name_to_column_id1, table_id);

        // Row 2: a=A, b=B, c=null (has null, slow path)
        let schema2 = create_multi_label_schema(&["a", "b", "c"]);
        let name_to_column_id2 = create_name_to_column_id(&["a", "b", "c"]);
        let row2 = create_row_with_nulls(&[Some("A"), Some("B"), None]);
        let tsid2 = extract_tsid(schema2, row2, &name_to_column_id2, table_id);

        // Both should have the same TSID since null labels are ignored
        assert_eq!(
            tsid1, tsid2,
            "TSID should be same when only difference is null label values"
        );
    }

    #[test]
    fn test_tsid_same_with_multiple_null_labels() {
        // Test with multiple null labels
        let table_id = 1025;

        // Row 1: a=A, b=B (no nulls)
        let schema1 = create_multi_label_schema(&["a", "b"]);
        let name_to_column_id1 = create_name_to_column_id(&["a", "b"]);
        let row1 = create_row_with_values(&["A", "B"]);
        let tsid1 = extract_tsid(schema1, row1, &name_to_column_id1, table_id);

        // Row 2: a=A, b=B, c=null, d=null (multiple nulls)
        let schema2 = create_multi_label_schema(&["a", "b", "c", "d"]);
        let name_to_column_id2 = create_name_to_column_id(&["a", "b", "c", "d"]);
        let row2 = create_row_with_nulls(&[Some("A"), Some("B"), None, None]);
        let tsid2 = extract_tsid(schema2, row2, &name_to_column_id2, table_id);

        assert_eq!(
            tsid1, tsid2,
            "TSID should be same when only difference is multiple null label values"
        );
    }

    #[test]
    fn test_tsid_different_with_different_non_null_values() {
        // Test that rows with different non-null values produce different TSIDs
        let table_id = 1025;

        // Row 1: a=A, b=B
        let schema1 = create_multi_label_schema(&["a", "b"]);
        let name_to_column_id1 = create_name_to_column_id(&["a", "b"]);
        let row1 = create_row_with_values(&["A", "B"]);
        let tsid1 = extract_tsid(schema1, row1, &name_to_column_id1, table_id);

        // Row 2: a=A, b=C (different value for b)
        let schema2 = create_multi_label_schema(&["a", "b"]);
        let name_to_column_id2 = create_name_to_column_id(&["a", "b"]);
        let row2 = create_row_with_values(&["A", "C"]);
        let tsid2 = extract_tsid(schema2, row2, &name_to_column_id2, table_id);

        assert_ne!(
            tsid1, tsid2,
            "TSID should be different when label values differ"
        );
    }

    #[test]
    fn test_tsid_fast_path_vs_slow_path_consistency() {
        // Test that fast path (no nulls) and slow path (with nulls) produce
        // the same TSID for the same non-null label values
        let table_id = 1025;

        // Fast path: a=A, b=B (no nulls)
        let schema_fast = create_multi_label_schema(&["a", "b"]);
        let name_to_column_id_fast = create_name_to_column_id(&["a", "b"]);
        let row_fast = create_row_with_values(&["A", "B"]);
        let tsid_fast = extract_tsid(schema_fast, row_fast, &name_to_column_id_fast, table_id);

        // Slow path: a=A, b=B, c=null (has null, triggers slow path)
        let schema_slow = create_multi_label_schema(&["a", "b", "c"]);
        let name_to_column_id_slow = create_name_to_column_id(&["a", "b", "c"]);
        let row_slow = create_row_with_nulls(&[Some("A"), Some("B"), None]);
        let tsid_slow = extract_tsid(schema_slow, row_slow, &name_to_column_id_slow, table_id);

        assert_eq!(
            tsid_fast, tsid_slow,
            "Fast path and slow path should produce same TSID for same non-null values"
        );
    }

    #[test]
    fn test_tsid_with_null_in_middle() {
        // Test with null in the middle of labels
        let table_id = 1025;

        // Row 1: a=A, b=B, c=C
        let schema1 = create_multi_label_schema(&["a", "b", "c"]);
        let name_to_column_id1 = create_name_to_column_id(&["a", "b", "c"]);
        let row1 = create_row_with_values(&["A", "B", "C"]);
        let tsid1 = extract_tsid(schema1, row1, &name_to_column_id1, table_id);

        // Row 2: a=A, b=null, c=C (null in middle)
        let schema2 = create_multi_label_schema(&["a", "b", "c"]);
        let name_to_column_id2 = create_name_to_column_id(&["a", "b", "c"]);
        let row2 = create_row_with_nulls(&[Some("A"), None, Some("C")]);
        let tsid2 = extract_tsid(schema2, row2, &name_to_column_id2, table_id);

        // Should be different because b is null in row2 but B in row1
        // Actually wait, let me reconsider - if b is null, it should be ignored
        // So row2 should be equivalent to a=A, c=C
        // But row1 is a=A, b=B, c=C, so they should be different
        assert_ne!(
            tsid1, tsid2,
            "TSID should be different when a non-null value becomes null"
        );

        // Row 3: a=A, c=C (no b at all, equivalent to row2)
        let schema3 = create_multi_label_schema(&["a", "c"]);
        let name_to_column_id3 = create_name_to_column_id(&["a", "c"]);
        let row3 = create_row_with_values(&["A", "C"]);
        let tsid3 = extract_tsid(schema3, row3, &name_to_column_id3, table_id);

        // Row2 (a=A, b=null, c=C) should be same as row3 (a=A, c=C)
        assert_eq!(
            tsid2, tsid3,
            "TSID should be same when null label is ignored"
        );
    }

    #[test]
    fn test_tsid_all_null_labels() {
        // Test with all labels being null
        let table_id = 1025;

        // Row with all nulls
        let schema = create_multi_label_schema(&["a", "b", "c"]);
        let name_to_column_id = create_name_to_column_id(&["a", "b", "c"]);
        let row = create_row_with_nulls(&[None, None, None]);
        let tsid = extract_tsid(schema.clone(), row, &name_to_column_id, table_id);

        // Should still produce a TSID (based on label names only when all values are null)
        // This tests that the slow path handles the case where all values are null
        // The TSID will be based on the label name hash only
        // Test that it's consistent - same schema with all nulls should produce same TSID
        let row2 = create_row_with_nulls(&[None, None, None]);
        let tsid2 = extract_tsid(schema, row2, &name_to_column_id, table_id);
        assert_eq!(
            tsid, tsid2,
            "TSID should be consistent when all label values are null"
        );
    }
}
