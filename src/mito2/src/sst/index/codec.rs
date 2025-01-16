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

use datatypes::data_type::ConcreteDataType;
use datatypes::value::{Value, ValueRef};
use memcomparable::Serializer;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::storage::ColumnId;

use crate::error::{FieldTypeMismatchSnafu, IndexEncodeNullSnafu, Result};
use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodec, SortField};

/// Encodes index values according to their data types for sorting and storage use.
pub struct IndexValueCodec;

impl IndexValueCodec {
    /// Serializes a non-null `ValueRef` using the data type defined in `SortField` and writes
    /// the result into a buffer.
    ///
    /// For `String` data types, we don't serialize it via memcomparable, but directly write the
    /// bytes into the buffer, since we have to keep the original string for searching with regex.
    ///
    /// # Arguments
    /// * `value` - The value to be encoded.
    /// * `field` - Contains data type to guide serialization.
    /// * `buffer` - Destination buffer for the serialized value.
    pub fn encode_nonnull_value(
        value: ValueRef,
        field: &SortField,
        buffer: &mut Vec<u8>,
    ) -> Result<()> {
        ensure!(!value.is_null(), IndexEncodeNullSnafu);

        if matches!(field.data_type, ConcreteDataType::String(_)) {
            let value = value
                .as_string()
                .context(FieldTypeMismatchSnafu)?
                .context(IndexEncodeNullSnafu)?;
            buffer.extend_from_slice(value.as_bytes());
            Ok(())
        } else {
            buffer.reserve(field.estimated_size());
            let mut serializer = Serializer::new(buffer);
            field.serialize(&mut serializer, &value)
        }
    }
}

/// Decodes primary key values into their corresponding column ids, data types and values.
pub struct IndexValuesCodec {
    /// Tuples containing column id and its corresponding index_name (result of `to_string` on ColumnId),
    /// to minimize redundant `to_string` calls.
    column_ids: Vec<(ColumnId, String)>,
    /// The data types of tag columns.
    fields: Vec<SortField>,
    /// The decoder for the primary key.
    decoder: DensePrimaryKeyCodec,
}

impl IndexValuesCodec {
    /// Creates a new `IndexValuesCodec` from a list of `ColumnMetadata` of tag columns.
    pub fn from_tag_columns<'a>(tag_columns: impl Iterator<Item = &'a ColumnMetadata>) -> Self {
        let (column_ids, fields): (Vec<_>, Vec<_>) = tag_columns
            .map(|column| {
                (
                    (column.column_id, column.column_id.to_string()),
                    SortField::new(column.column_schema.data_type.clone()),
                )
            })
            .unzip();

        let decoder = DensePrimaryKeyCodec::with_fields(fields.clone());
        Self {
            column_ids,
            fields,
            decoder,
        }
    }

    /// Decodes a primary key into its corresponding column ids, data types and values.
    pub fn decode(
        &self,
        primary_key: &[u8],
    ) -> Result<impl Iterator<Item = (&(ColumnId, String), &SortField, Option<Value>)>> {
        let values = self.decoder.decode_dense(primary_key)?;

        let iter = values
            .into_iter()
            .zip(&self.column_ids)
            .zip(&self.fields)
            .map(|((value, column_id), encoder)| {
                if value.is_null() {
                    (column_id, encoder, None)
                } else {
                    (column_id, encoder, Some(value))
                }
            });

        Ok(iter)
    }
}

#[cfg(test)]
mod tests {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;

    use super::*;
    use crate::error::Error;
    use crate::row_converter::{PrimaryKeyCodecExt, SortField};

    #[test]
    fn test_encode_value_basic() {
        let value = ValueRef::from("hello");
        let field = SortField::new(ConcreteDataType::string_datatype());

        let mut buffer = Vec::new();
        IndexValueCodec::encode_nonnull_value(value, &field, &mut buffer).unwrap();
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_encode_value_type_mismatch() {
        let value = ValueRef::from("hello");
        let field = SortField::new(ConcreteDataType::int64_datatype());

        let mut buffer = Vec::new();
        let res = IndexValueCodec::encode_nonnull_value(value, &field, &mut buffer);
        assert!(matches!(res, Err(Error::FieldTypeMismatch { .. })));
    }

    #[test]
    fn test_encode_null_value() {
        let value = ValueRef::Null;
        let field = SortField::new(ConcreteDataType::string_datatype());

        let mut buffer = Vec::new();
        let res = IndexValueCodec::encode_nonnull_value(value, &field, &mut buffer);
        assert!(matches!(res, Err(Error::IndexEncodeNull { .. })));
    }

    #[test]
    fn test_decode_primary_key_basic() {
        let tag_columns = vec![
            ColumnMetadata {
                column_schema: ColumnSchema::new("tag0", ConcreteDataType::string_datatype(), true),
                semantic_type: api::v1::SemanticType::Tag,
                column_id: 1,
            },
            ColumnMetadata {
                column_schema: ColumnSchema::new("tag1", ConcreteDataType::int64_datatype(), false),
                semantic_type: api::v1::SemanticType::Tag,
                column_id: 2,
            },
        ];

        let primary_key = DensePrimaryKeyCodec::with_fields(vec![
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::int64_datatype()),
        ])
        .encode([ValueRef::Null, ValueRef::Int64(10)].into_iter())
        .unwrap();

        let codec = IndexValuesCodec::from_tag_columns(tag_columns.iter());
        let mut iter = codec.decode(&primary_key).unwrap();

        let ((column_id, col_id_str), field, value) = iter.next().unwrap();
        assert_eq!(*column_id, 1);
        assert_eq!(col_id_str, "1");
        assert_eq!(field, &SortField::new(ConcreteDataType::string_datatype()));
        assert_eq!(value, None);

        let ((column_id, col_id_str), field, value) = iter.next().unwrap();
        assert_eq!(*column_id, 2);
        assert_eq!(col_id_str, "2");
        assert_eq!(field, &SortField::new(ConcreteDataType::int64_datatype()));
        assert_eq!(value, Some(Value::Int64(10)));

        assert!(iter.next().is_none());
    }
}
