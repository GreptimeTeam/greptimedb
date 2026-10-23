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
use datatypes::arrow::array::{Array, ArrayRef, StructArray};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::{Helper, VectorRef};
use snafu::{OptionExt, ResultExt};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::error::{ConvertVectorSnafu, InvalidRecordBatchSnafu, Result};
use crate::sst::parquet::format::PrimaryKeyArray;

/// Extracts leaf values for a JSON type hint at the given path.
///
/// Returns an error if the path does not exist, traverses a non-object,
/// or the leaf array cannot be converted to a vector.
///
/// # Details
///
/// The caller guarantees that the materialized leaf type matches the type hint.
///
/// JSON2 builders propagate ancestor nulls to child arrays. SST reads and schema
/// adaptation preserve this invariant, so no ancestor null bitmap merging is needed.
pub(crate) fn extract_json_type_hint_values(root: &ArrayRef, path: &[String]) -> Result<VectorRef> {
    let mut array = root.clone();
    for segment in path {
        let object = array
            .as_any()
            .downcast_ref::<StructArray>()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("JSON index path {path:?} traverses a non-object"),
            })?;
        array = object
            .column_by_name(segment)
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("JSON index path {path:?} has no materialized field {segment}"),
            })?
            .clone();
    }
    Helper::try_into_vector(array).context(ConvertVectorSnafu)
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

    use datatypes::arrow::array::{Int64Array, NullArray};
    use datatypes::arrow::buffer::NullBuffer;
    use datatypes::arrow::datatypes::Field;
    use datatypes::value::Value;

    use super::*;

    #[test]
    fn test_extract_json_type_hint_values_null_type() {
        let nulls: ArrayRef = Arc::new(NullArray::new(2));
        let root: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("a", nulls.data_type().clone(), true)].into(),
            vec![nulls],
            None,
        ));
        // A null leaf can be extracted, but cannot be traversed as an object.
        assert!(extract_json_type_hint_values(&root, &["a".into(), "b".into()]).is_err());
        let leaf = extract_json_type_hint_values(&root, &["a".into()]).unwrap();
        assert_eq!(leaf.len(), 2);
        assert_eq!(leaf.get(0), Value::Null);
        assert_eq!(leaf.get(1), Value::Null);

        let values: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
        let root: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("a", values.data_type().clone(), true)].into(),
            vec![values],
            None,
        ));
        let leaf = extract_json_type_hint_values(&root, &["a".into()]).unwrap();
        assert_eq!(leaf.len(), 2);
        assert_eq!(leaf.get(0), Value::Null);
        assert_eq!(leaf.get(1), Value::Null);
    }

    #[test]
    fn test_extract_json_type_hint_values_nulls_and_slices() {
        let child = Arc::new(Int64Array::from(vec![Some(11), None, None, None, Some(55)]));
        let nested = StructArray::new(
            vec![Field::new("a.b", child.data_type().clone(), true)].into(),
            vec![child],
            Some(NullBuffer::from(vec![true, false, false, true, true])),
        );
        let root: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("nested", nested.data_type().clone(), true)].into(),
            vec![Arc::new(nested)],
            Some(NullBuffer::from(vec![true, true, false, true, true])),
        ));
        let path = vec!["nested".into(), "a.b".into()];
        let leaf = extract_json_type_hint_values(&root, &path).unwrap();
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
        let slice = extract_json_type_hint_values(&root.slice(1, 4), &path).unwrap();
        assert_eq!(
            (0..slice.len()).map(|i| slice.get(i)).collect::<Vec<_>>(),
            vec![Value::Null, Value::Null, Value::Null, Value::Int64(55)]
        );
        assert!(extract_json_type_hint_values(&root, &["missing".into()]).is_err());
        assert!(
            extract_json_type_hint_values(&root, &["nested".into(), "a.b".into(), "child".into()])
                .is_err()
        );
    }
}
