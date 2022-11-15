// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub use prost::DecodeError;
use prost::Message;

use crate::v1::codec::{InsertBatch, PhysicalPlanNode, RegionNumber, SelectResult};
use crate::v1::meta::TableRouteValue;

macro_rules! impl_convert_with_bytes {
    ($data_type: ty) => {
        impl From<$data_type> for Vec<u8> {
            fn from(entity: $data_type) -> Self {
                entity.encode_to_vec()
            }
        }

        impl TryFrom<&[u8]> for $data_type {
            type Error = DecodeError;

            fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
                <$data_type>::decode(value.as_ref())
            }
        }
    };
}

impl_convert_with_bytes!(InsertBatch);
impl_convert_with_bytes!(SelectResult);
impl_convert_with_bytes!(PhysicalPlanNode);
impl_convert_with_bytes!(RegionNumber);
impl_convert_with_bytes!(TableRouteValue);

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use crate::v1::codec::*;
    use crate::v1::{column, Column};

    const SEMANTIC_TAG: i32 = 0;

    #[test]
    fn test_convert_insert_batch() {
        let insert_batch = mock_insert_batch();

        let bytes: Vec<u8> = insert_batch.into();
        let insert: InsertBatch = bytes.deref().try_into().unwrap();

        assert_eq!(8, insert.row_count);
        assert_eq!(1, insert.columns.len());

        let column = &insert.columns[0];
        assert_eq!("foo", column.column_name);
        assert_eq!(SEMANTIC_TAG, column.semantic_type);
        assert_eq!(vec![1], column.null_mask);
        assert_eq!(
            vec![2, 3, 4, 5, 6, 7, 8],
            column.values.as_ref().unwrap().i32_values
        );
    }

    #[should_panic]
    #[test]
    fn test_convert_insert_batch_wrong() {
        let insert_batch = mock_insert_batch();

        let mut bytes: Vec<u8> = insert_batch.into();

        // modify some bytes
        bytes[0] = 0b1;
        bytes[1] = 0b1;

        let insert: InsertBatch = bytes.deref().try_into().unwrap();

        assert_eq!(8, insert.row_count);
        assert_eq!(1, insert.columns.len());

        let column = &insert.columns[0];
        assert_eq!("foo", column.column_name);
        assert_eq!(SEMANTIC_TAG, column.semantic_type);
        assert_eq!(vec![1], column.null_mask);
        assert_eq!(
            vec![2, 3, 4, 5, 6, 7, 8],
            column.values.as_ref().unwrap().i32_values
        );
    }

    #[test]
    fn test_convert_select_result() {
        let select_result = mock_select_result();

        let bytes: Vec<u8> = select_result.into();
        let result: SelectResult = bytes.deref().try_into().unwrap();

        assert_eq!(8, result.row_count);
        assert_eq!(1, result.columns.len());

        let column = &result.columns[0];
        assert_eq!("foo", column.column_name);
        assert_eq!(SEMANTIC_TAG, column.semantic_type);
        assert_eq!(vec![1], column.null_mask);
        assert_eq!(
            vec![2, 3, 4, 5, 6, 7, 8],
            column.values.as_ref().unwrap().i32_values
        );
    }

    #[should_panic]
    #[test]
    fn test_convert_select_result_wrong() {
        let select_result = mock_select_result();

        let mut bytes: Vec<u8> = select_result.into();

        // modify some bytes
        bytes[0] = 0b1;
        bytes[1] = 0b1;

        let result: SelectResult = bytes.deref().try_into().unwrap();

        assert_eq!(8, result.row_count);
        assert_eq!(1, result.columns.len());

        let column = &result.columns[0];
        assert_eq!("foo", column.column_name);
        assert_eq!(SEMANTIC_TAG, column.semantic_type);
        assert_eq!(vec![1], column.null_mask);
        assert_eq!(
            vec![2, 3, 4, 5, 6, 7, 8],
            column.values.as_ref().unwrap().i32_values
        );
    }

    #[test]
    fn test_convert_region_id() {
        let region_id = RegionNumber { id: 12 };

        let bytes: Vec<u8> = region_id.into();
        let region_id: RegionNumber = bytes.deref().try_into().unwrap();

        assert_eq!(12, region_id.id);
    }

    fn mock_insert_batch() -> InsertBatch {
        let values = column::Values {
            i32_values: vec![2, 3, 4, 5, 6, 7, 8],
            ..Default::default()
        };
        let null_mask = vec![1];
        let column = Column {
            column_name: "foo".to_string(),
            semantic_type: SEMANTIC_TAG,
            values: Some(values),
            null_mask,
            ..Default::default()
        };
        InsertBatch {
            columns: vec![column],
            row_count: 8,
        }
    }

    fn mock_select_result() -> SelectResult {
        let values = column::Values {
            i32_values: vec![2, 3, 4, 5, 6, 7, 8],
            ..Default::default()
        };
        let null_mask = vec![1];
        let column = Column {
            column_name: "foo".to_string(),
            semantic_type: SEMANTIC_TAG,
            values: Some(values),
            null_mask,
            ..Default::default()
        };
        SelectResult {
            columns: vec![column],
            row_count: 8,
        }
    }
}
