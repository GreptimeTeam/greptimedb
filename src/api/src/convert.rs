pub use prost::DecodeError;
use prost::Message;

use crate::v1::InsertBatch;

impl From<InsertBatch> for Vec<u8> {
    fn from(insert: InsertBatch) -> Self {
        insert.encode_length_delimited_to_vec()
    }
}

impl TryFrom<Vec<u8>> for InsertBatch {
    type Error = DecodeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        InsertBatch::decode_length_delimited(value.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use crate::v1::*;

    const SEMANTIC_TAG: i32 = 0;

    #[test]
    fn test_convert_insert_batch() {
        let insert_batch = mock_insert_batch();

        let bytes: Vec<u8> = insert_batch.into();
        let insert: InsertBatch = bytes.try_into().unwrap();

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

        let insert: InsertBatch = bytes.try_into().unwrap();

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
        };
        InsertBatch {
            columns: vec![column],
            row_count: 8,
        }
    }
}
