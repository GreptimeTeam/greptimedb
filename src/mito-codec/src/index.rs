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

//! Index codec utilities.

use std::collections::HashMap;
use std::sync::Arc;

use datatypes::value::ValueRef;
use memcomparable::Serializer;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::ColumnMetadata;
use store_api::storage::ColumnId;

use crate::error::{FieldTypeMismatchSnafu, IndexEncodeNullSnafu, Result};
use crate::row_converter::{PrimaryKeyCodec, SortField, build_primary_key_codec_with_fields};

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

        if field.data_type().is_string() {
            let value = value
                .try_into_string()
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

pub struct PkColInfo {
    pub idx: usize,
    pub field: SortField,
}

impl PkColInfo {
    pub fn new(idx: usize, field: SortField) -> Self {
        Self { idx, field }
    }
}

/// Decodes primary key values into their corresponding column ids, data types and values.
pub struct IndexValuesCodec {
    /// Column ids -> column info mapping.
    columns_mapping: HashMap<ColumnId, PkColInfo>,
    /// The decoder for the primary key.
    decoder: Arc<dyn PrimaryKeyCodec>,
}

impl IndexValuesCodec {
    /// Creates a new `IndexValuesCodec` from a list of `ColumnMetadata` of tag columns.
    pub fn from_tag_columns<'a>(
        primary_key_encoding: PrimaryKeyEncoding,
        tag_columns: impl Iterator<Item = &'a ColumnMetadata>,
    ) -> Self {
        let (columns_mapping, fields): (HashMap<ColumnId, PkColInfo>, Vec<(ColumnId, SortField)>) =
            tag_columns
                .enumerate()
                .map(|(idx, column)| {
                    let col_id = column.column_id;
                    let field = SortField::new(column.column_schema.data_type.clone());
                    let pk_col_info = PkColInfo::new(idx, field.clone());
                    ((col_id, pk_col_info), (col_id, field))
                })
                .unzip();

        let decoder = build_primary_key_codec_with_fields(primary_key_encoding, fields.into_iter());

        Self {
            columns_mapping,
            decoder,
        }
    }

    pub fn pk_col_info(&self, column_id: ColumnId) -> Option<&PkColInfo> {
        self.columns_mapping.get(&column_id)
    }

    pub fn decoder(&self) -> &dyn PrimaryKeyCodec {
        self.decoder.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::Value;
    use store_api::metadata::ColumnMetadata;

    use super::*;
    use crate::error::Error;
    use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};

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
        let tag_columns = [
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
            (0, SortField::new(ConcreteDataType::string_datatype())),
            (1, SortField::new(ConcreteDataType::int64_datatype())),
        ])
        .encode([ValueRef::Null, ValueRef::Int64(10)].into_iter())
        .unwrap();

        let codec =
            IndexValuesCodec::from_tag_columns(PrimaryKeyEncoding::Dense, tag_columns.iter());
        let values = codec.decoder().decode(&primary_key).unwrap().into_dense();

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Null);
        assert_eq!(values[1], Value::Int64(10));
    }
}
