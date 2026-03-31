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

use std::hash::Hasher;
use std::sync::Arc;

use datatypes::arrow::array::{Array, BinaryBuilder, StringArray, UInt64Array};
use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datatypes::arrow::record_batch::RecordBatch;
use fxhash::FxHasher;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use snafu::ResultExt;
use store_api::storage::ColumnId;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::error::{EncodePrimaryKeySnafu, Result, UnexpectedRequestSnafu};

/// Info about a tag column for TSID computation and sparse primary key encoding.
#[allow(dead_code)]
pub struct TagColumnInfo {
    /// Column name (used for label-name hash).
    pub name: String,
    /// Column index in the RecordBatch.
    pub index: usize,
    /// Column ID in the physical region.
    pub column_id: ColumnId,
}

/// Computes `__tsid` values for each row.
pub fn compute_tsid_array(
    batch: &RecordBatch,
    sorted_tag_columns: &[TagColumnInfo],
    tag_arrays: &[&StringArray],
) -> UInt64Array {
    let num_rows = batch.num_rows();

    let label_name_hash = {
        let mut hasher = FxHasher::default();
        for tag_col in sorted_tag_columns {
            hasher.write(tag_col.name.as_bytes());
            hasher.write_u8(0xff);
        }
        hasher.finish()
    };

    let mut tsid_values = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        let has_null = tag_arrays.iter().any(|arr| arr.is_null(row));

        let tsid = if !has_null {
            let mut hasher = FxHasher::default();
            hasher.write_u64(label_name_hash);
            for arr in tag_arrays {
                hasher.write(arr.value(row).as_bytes());
                hasher.write_u8(0xff);
            }
            hasher.finish()
        } else {
            let mut name_hasher = FxHasher::default();
            for (tc, arr) in sorted_tag_columns.iter().zip(tag_arrays.iter()) {
                if !arr.is_null(row) {
                    name_hasher.write(tc.name.as_bytes());
                    name_hasher.write_u8(0xff);
                }
            }
            let row_label_hash = name_hasher.finish();

            let mut val_hasher = FxHasher::default();
            val_hasher.write_u64(row_label_hash);
            for arr in tag_arrays {
                if !arr.is_null(row) {
                    val_hasher.write(arr.value(row).as_bytes());
                    val_hasher.write_u8(0xff);
                }
            }
            val_hasher.finish()
        };

        tsid_values.push(tsid);
    }

    UInt64Array::from(tsid_values)
}

fn build_tag_arrays<'a>(
    batch: &'a RecordBatch,
    sorted_tag_columns: &[TagColumnInfo],
) -> Vec<&'a StringArray> {
    sorted_tag_columns
        .iter()
        .map(|tc| {
            batch
                .column(tc.index)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("tag column must be utf8")
        })
        .collect()
}

/// Modifies a RecordBatch for sparse primary key encoding.
pub fn modify_batch_sparse(
    batch: RecordBatch,
    table_id: u32,
    sorted_tag_columns: &[TagColumnInfo],
    non_tag_column_indices: &[usize],
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let codec = SparsePrimaryKeyCodec::schemaless();
    let tag_arrays: Vec<&StringArray> = build_tag_arrays(&batch, sorted_tag_columns);
    let tsid_array = compute_tsid_array(&batch, sorted_tag_columns, &tag_arrays);

    let mut pk_builder = BinaryBuilder::with_capacity(num_rows, 0);
    let mut buffer = Vec::new();
    for row in 0..num_rows {
        buffer.clear();
        codec
            .encode_internal(table_id, tsid_array.value(row), &mut buffer)
            .context(EncodePrimaryKeySnafu)?;

        let tags = sorted_tag_columns
            .iter()
            .zip(tag_arrays.iter())
            .filter(|(_, arr)| !arr.is_null(row))
            .map(|(tc, arr)| (tc.column_id, arr.value(row).as_bytes()));
        codec
            .encode_raw_tag_value(tags, &mut buffer)
            .context(EncodePrimaryKeySnafu)?;

        pk_builder.append_value(&buffer);
    }

    let pk_array = pk_builder.finish();

    let mut fields = vec![Arc::new(Field::new(
        PRIMARY_KEY_COLUMN_NAME,
        DataType::Binary,
        false,
    ))];
    let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(pk_array)];

    for &idx in non_tag_column_indices {
        fields.push(batch.schema().fields()[idx].clone());
        columns.push(batch.column(idx).clone());
    }

    let new_schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(new_schema, columns).map_err(|e| {
        UnexpectedRequestSnafu {
            reason: format!("Failed to build modified sparse RecordBatch: {e}"),
        }
        .build()
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
    use datatypes::arrow::array::{BinaryArray, Int64Array, StringArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datatypes::arrow::record_batch::RecordBatch;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

    use super::*;
    use crate::row_modifier::{RowModifier, RowsIter, TableIdInput};

    fn build_sparse_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("greptime_timestamp", DataType::Int64, false),
            Field::new("greptime_value", DataType::Float64, true),
            Field::new("namespace", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1000])),
                Arc::new(datatypes::arrow::array::Float64Array::from(vec![42.0])),
                Arc::new(StringArray::from(vec!["greptimedb"])),
                Arc::new(StringArray::from(vec!["127.0.0.1"])),
            ],
        )
        .unwrap()
    }

    fn sparse_tag_columns() -> Vec<TagColumnInfo> {
        vec![
            TagColumnInfo {
                name: "host".to_string(),
                index: 3,
                column_id: 3,
            },
            TagColumnInfo {
                name: "namespace".to_string(),
                index: 2,
                column_id: 2,
            },
        ]
    }

    #[test]
    fn test_compute_tsid_basic() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("namespace", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["greptimedb"])),
                Arc::new(StringArray::from(vec!["127.0.0.1"])),
            ],
        )
        .unwrap();

        let tag_columns: Vec<TagColumnInfo> = vec![
            TagColumnInfo {
                name: "host".to_string(),
                index: 1,
                column_id: 2,
            },
            TagColumnInfo {
                name: "namespace".to_string(),
                index: 0,
                column_id: 1,
            },
        ];
        let tag_arrays = build_tag_arrays(&batch, &tag_columns);
        let tsid_array = compute_tsid_array(&batch, &tag_columns, &tag_arrays);

        assert_eq!(tsid_array.value(0), 2721566936019240841);
    }

    #[test]
    fn test_compute_tsid_with_nulls() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch_no_null = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["B"])),
            ],
        )
        .unwrap();
        let tag_cols_2: Vec<TagColumnInfo> = vec![
            TagColumnInfo {
                name: "a".to_string(),
                index: 0,
                column_id: 1,
            },
            TagColumnInfo {
                name: "b".to_string(),
                index: 1,
                column_id: 2,
            },
        ];
        let tag_arrays_2 = build_tag_arrays(&batch_no_null, &tag_cols_2);
        let tsid_no_null = compute_tsid_array(&batch_no_null, &tag_cols_2, &tag_arrays_2);

        let schema3 = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]));
        let batch_with_null = RecordBatch::try_new(
            schema3,
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["B"])),
                Arc::new(StringArray::from(vec![None as Option<&str>])),
            ],
        )
        .unwrap();
        let tag_cols_3: Vec<TagColumnInfo> = vec![
            TagColumnInfo {
                name: "a".to_string(),
                index: 0,
                column_id: 1,
            },
            TagColumnInfo {
                name: "b".to_string(),
                index: 1,
                column_id: 2,
            },
            TagColumnInfo {
                name: "c".to_string(),
                index: 2,
                column_id: 3,
            },
        ];
        let tag_arrays_3 = build_tag_arrays(&batch_with_null, &tag_cols_3);
        let tsid_with_null = compute_tsid_array(&batch_with_null, &tag_cols_3, &tag_arrays_3);

        assert_eq!(tsid_no_null.value(0), tsid_with_null.value(0));
    }

    #[test]
    fn test_modify_batch_sparse() {
        let batch = build_sparse_test_batch();
        let tag_columns = sparse_tag_columns();
        let non_tag_indices = vec![0, 1];
        let table_id: u32 = 1025;

        let modified =
            modify_batch_sparse(batch, table_id, &tag_columns, &non_tag_indices).unwrap();

        assert_eq!(modified.num_columns(), 3);
        assert_eq!(modified.schema().field(0).name(), PRIMARY_KEY_COLUMN_NAME);
        assert_eq!(modified.schema().field(1).name(), "greptime_timestamp");
        assert_eq!(modified.schema().field(2).name(), "greptime_value");
    }

    #[test]
    fn test_modify_batch_sparse_matches_row_modifier() {
        let batch = build_sparse_test_batch();
        let tag_columns = sparse_tag_columns();
        let non_tag_indices = vec![0, 1];
        let table_id: u32 = 1025;
        let modified =
            modify_batch_sparse(batch, table_id, &tag_columns, &non_tag_indices).unwrap();

        let name_to_column_id: HashMap<String, ColumnId> = [
            ("greptime_timestamp".to_string(), 0),
            ("greptime_value".to_string(), 1),
            ("namespace".to_string(), 2),
            ("host".to_string(), 3),
        ]
        .into_iter()
        .collect();

        let rows = Rows {
            schema: vec![
                ColumnSchema {
                    column_name: "greptime_timestamp".to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    semantic_type: SemanticType::Timestamp as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "greptime_value".to_string(),
                    datatype: ColumnDataType::Float64 as i32,
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "namespace".to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "host".to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
            ],
            rows: vec![Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(1000)),
                    },
                    Value {
                        value_data: Some(ValueData::F64Value(42.0)),
                    },
                    Value {
                        value_data: Some(ValueData::StringValue("greptimedb".to_string())),
                    },
                    Value {
                        value_data: Some(ValueData::StringValue("127.0.0.1".to_string())),
                    },
                ],
            }],
        };

        let row_iter = RowsIter::new(rows, &name_to_column_id);
        let rows = RowModifier::default()
            .modify_rows(
                row_iter,
                TableIdInput::Single(table_id),
                PrimaryKeyEncoding::Sparse,
            )
            .unwrap();
        let ValueData::BinaryValue(expected_pk) =
            rows.rows[0].values[0].value_data.clone().unwrap()
        else {
            panic!("expected binary primary key");
        };

        let actual_array = modified
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(actual_array.value(0), expected_pk.as_slice());
    }
}
