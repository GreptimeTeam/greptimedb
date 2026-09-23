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

use api::v1::SemanticType;
use datatypes::arrow::array::{Array, ArrayRef, StructArray, make_array, new_null_array};
use datatypes::arrow::buffer::NullBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::vectors::{Helper, VectorRef};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::error::{ConvertVectorSnafu, InvalidRecordBatchSnafu, Result};
use crate::sst::parquet::format::PrimaryKeyArray;

/// Extracts a materialized JSON hint, propagating nulls from all ancestor objects.
/// Missing physical fields and type mismatches abort index creation rather than
/// indexing remainder data or silently producing an incomplete index.
pub(crate) fn json_index_leaf(
    root: &ArrayRef,
    path: &[String],
    data_type: &ConcreteDataType,
) -> Result<VectorRef> {
    let mut array = root.clone();
    let mut nulls = None;
    for segment in path {
        if array.data_type() == &datatypes::arrow::datatypes::DataType::Null {
            array = new_null_array(&data_type.as_arrow_type(), root.len());
            break;
        }
        let object = array
            .as_any()
            .downcast_ref::<StructArray>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("JSON index path {path:?} traverses a non-object"),
            })?;
        nulls = NullBuffer::union(nulls.as_ref(), object.nulls());
        array = object
            .column_by_name(segment)
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("JSON index path {path:?} has no materialized field {segment}"),
            })?
            .clone();
    }
    if array.data_type() == &datatypes::arrow::datatypes::DataType::Null {
        array = new_null_array(&data_type.as_arrow_type(), root.len());
    }
    nulls = NullBuffer::union(nulls.as_ref(), array.nulls());
    let data = array
        .to_data()
        .into_builder()
        .nulls(nulls)
        .build()
        .map_err(|err| {
            InvalidRecordBatchSnafu {
                reason: err.to_string(),
            }
            .build()
        })?;
    let vector = Helper::try_into_vector(make_array(data)).context(ConvertVectorSnafu)?;
    // JSON2 uses Utf8View physically; all Arrow string layouts use the same
    // UTF-8 index encoding. Keep the declared hint type in the target key.
    ensure!(
        &vector.data_type() == data_type
            || (vector.data_type().is_string() && data_type.is_string()),
        InvalidRecordBatchSnafu {
            reason: format!(
                "JSON index path {path:?} expected {data_type}, found {}",
                vector.data_type()
            ),
        }
    );
    Ok(vector)
}

/// Yields (first row, row count) for indexing a materialized column.
/// Tags are constant within consecutive equal PK dictionary keys. Fields and
/// timestamps must still be visited row by row. Distinct dictionary entries may
/// contain equal PK bytes; keeping those runs separate is correct and avoids decoding.
/// Inputs without a non-null PK dictionary fall back to visiting individual rows.
pub(crate) fn column_index_rows(
    batch: &RecordBatch,
    semantic_type: SemanticType,
) -> impl Iterator<Item = (usize, usize)> + '_ {
    let keys = (semantic_type == SemanticType::Tag)
        .then(|| batch.column_by_name(PRIMARY_KEY_COLUMN_NAME))
        .flatten()
        .and_then(|array| array.as_any().downcast_ref::<PrimaryKeyArray>())
        .filter(|array| array.null_count() == 0 && array.values().null_count() == 0)
        .map(|array| array.keys().values());
    let mut row = 0;
    std::iter::from_fn(move || {
        if row == batch.num_rows() {
            return None;
        }
        let start = row;
        row += 1;
        if let Some(keys) = keys {
            while row < keys.len() && keys[row] == keys[start] {
                row += 1;
            }
        }
        Some((start, row - start))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::Int64Array;
    use datatypes::arrow::datatypes::Field;
    use datatypes::value::Value;

    use super::*;

    #[test]
    fn test_json_index_leaf_parent_nulls_and_slices() {
        let child = Arc::new(Int64Array::from(vec![
            Some(11),
            Some(22),
            Some(33),
            None,
            Some(55),
        ]));
        let nested = StructArray::new(
            vec![Field::new("a.b", child.data_type().clone(), true)].into(),
            vec![child],
            Some(NullBuffer::from(vec![true, false, true, true, true])),
        );
        let root: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("nested", nested.data_type().clone(), true)].into(),
            vec![Arc::new(nested)],
            Some(NullBuffer::from(vec![true, true, false, true, true])),
        ));
        let path = vec!["nested".into(), "a.b".into()];
        let leaf = json_index_leaf(&root, &path, &ConcreteDataType::int64_datatype()).unwrap();
        assert_eq!(
            (0..leaf.len()).map(|i| leaf.get(i)).collect::<Vec<_>>(),
            vec![
                Value::Int64(11),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int64(55)
            ]
        );
        let slice = json_index_leaf(
            &root.slice(1, 4),
            &path,
            &ConcreteDataType::int64_datatype(),
        )
        .unwrap();
        assert_eq!(
            (0..slice.len()).map(|i| slice.get(i)).collect::<Vec<_>>(),
            vec![Value::Null, Value::Null, Value::Null, Value::Int64(55)]
        );
        assert!(json_index_leaf(&root, &path, &ConcreteDataType::string_datatype()).is_err());
        assert!(
            json_index_leaf(
                &root,
                &["missing".into()],
                &ConcreteDataType::int64_datatype()
            )
            .is_err()
        );
        assert!(
            json_index_leaf(
                &root,
                &["nested".into(), "a.b".into(), "child".into()],
                &ConcreteDataType::int64_datatype()
            )
            .is_err()
        );
    }
}
