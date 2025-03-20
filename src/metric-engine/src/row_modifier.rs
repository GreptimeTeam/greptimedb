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
use std::hash::Hash;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
use datatypes::value::ValueRef;
use mito2::row_converter::SparsePrimaryKeyCodec;
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::storage::consts::{ReservedColumnId, PRIMARY_KEY_COLUMN_NAME};
use store_api::storage::{ColumnId, TableId};

use crate::error::{EncodePrimaryKeySnafu, Result};

// A random number
const TSID_HASH_SEED: u32 = 846793005;

/// A row modifier modifies [`Rows`].
///
/// - For [`PrimaryKeyEncoding::Sparse`] encoding,
///   it replaces the primary key columns with the encoded primary key column(`__primary_key`).
///
/// - For [`PrimaryKeyEncoding::Dense`] encoding,
///   it adds two columns(`__table_id`, `__tsid`) to the row.
pub(crate) struct RowModifier {
    codec: SparsePrimaryKeyCodec,
}

impl RowModifier {
    pub fn new() -> Self {
        Self {
            codec: SparsePrimaryKeyCodec::schemaless(),
        }
    }

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
            let (table_id, tsid) = self.fill_internal_columns(table_id, &iter);
            let mut values = Vec::with_capacity(num_output_column);
            buffer.clear();
            let internal_columns = [
                (
                    ReservedColumnId::table_id(),
                    api::helper::pb_value_to_value_ref(&table_id, &None),
                ),
                (
                    ReservedColumnId::tsid(),
                    api::helper::pb_value_to_value_ref(&tsid, &None),
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
            let (table_id, tsid) = self.fill_internal_columns(table_id, &iter);
            iter.row.values.push(table_id);
            iter.row.values.push(tsid);
        }

        Ok(iter.rows)
    }

    /// Fills internal columns of a row with table name and a hash of tag values.
    fn fill_internal_columns(&self, table_id: TableId, iter: &RowIter<'_>) -> (Value, Value) {
        let mut hasher = TsidGenerator::default();
        for (name, value) in iter.primary_keys_with_name() {
            // The type is checked before. So only null is ignored.
            if let Some(ValueData::StringValue(string)) = &value.value_data {
                hasher.write_label(name, string);
            }
        }
        let hash = hasher.finish();

        (
            ValueData::U32Value(table_id).into(),
            ValueData::U64Value(hash).into(),
        )
    }
}

/// Tsid generator.
pub struct TsidGenerator {
    hasher: mur3::Hasher128,
}

impl Default for TsidGenerator {
    fn default() -> Self {
        Self {
            hasher: mur3::Hasher128::with_seed(TSID_HASH_SEED),
        }
    }
}

impl TsidGenerator {
    /// Writes a label pair to the generator.
    pub fn write_label(&mut self, name: &str, value: &str) {
        name.hash(&mut self.hasher);
        value.hash(&mut self.hasher);
    }

    /// Generates a new TSID.
    pub fn finish(&mut self) -> u64 {
        // TSID is 64 bits, simply truncate the 128 bits hash
        let (hash, _) = self.hasher.finish128();
        hash
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
        let indices = reserved_indices
            .into_iter()
            .chain(primary_key_indices.values().cloned())
            .chain(ts_index)
            .chain(field_indices)
            .collect();
        IterIndex {
            indices,
            num_primary_key_column,
        }
    }
}

/// Iterator of rows.
pub(crate) struct RowsIter {
    rows: Rows,
    index: IterIndex,
}

impl RowsIter {
    pub fn new(rows: Rows, name_to_column_id: &HashMap<String, ColumnId>) -> Self {
        let index: IterIndex = IterIndex::new(&rows.schema, name_to_column_id);
        Self { rows, index }
    }

    /// Returns the iterator of rows.
    fn iter_mut(&mut self) -> impl Iterator<Item = RowIter> {
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
struct RowIter<'a> {
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

    /// Returns the primary keys.
    fn primary_keys(&self) -> impl Iterator<Item = (ColumnId, ValueRef)> {
        self.index.indices[..self.index.num_primary_key_column]
            .iter()
            .map(|idx| {
                (
                    idx.column_id,
                    api::helper::pb_value_to_value_ref(
                        &self.row.values[idx.index],
                        &self.schema[idx.index].datatype_extension,
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
        let encoder = RowModifier::new();
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
            128, 0, 0, 4, 1, 0, 0, 4, 1, 128, 0, 0, 3, 1, 131, 9, 166, 190, 173, 37, 39, 240, 0, 0,
            0, 2, 1, 1, 49, 50, 55, 46, 48, 46, 48, 46, 9, 49, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1,
            1, 1, 103, 114, 101, 112, 116, 105, 109, 101, 9, 100, 98, 0, 0, 0, 0, 0, 0, 2,
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
        let encoder = RowModifier::new();
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
            ValueData::U64Value(9442261431637846000).into()
        );
        assert_eq!(result.schema, expected_dense_schema());
    }

    #[test]
    fn test_fill_internal_columns() {
        let name_to_column_id = test_name_to_column_id();
        let encoder = RowModifier::new();
        let table_id = 1025;
        let schema = test_schema();
        let row = test_row("greptimedb", "127.0.0.1");
        let rows = Rows {
            schema,
            rows: vec![row],
        };
        let mut rows_iter = RowsIter::new(rows, &name_to_column_id);
        let row_iter = rows_iter.iter_mut().next().unwrap();
        let (encoded_table_id, tsid) = encoder.fill_internal_columns(table_id, &row_iter);
        assert_eq!(encoded_table_id, ValueData::U32Value(1025).into());
        assert_eq!(tsid, ValueData::U64Value(9442261431637846000).into());

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
        let (encoded_table_id, tsid) = encoder.fill_internal_columns(table_id, &row_iter);
        assert_eq!(encoded_table_id, ValueData::U32Value(1025).into());
        assert_eq!(tsid, ValueData::U64Value(9442261431637846000).into());
    }
}
