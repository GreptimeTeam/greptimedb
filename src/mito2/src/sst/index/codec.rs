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

use datatypes::value::{Value, ValueRef};
use memcomparable::Serializer;
use store_api::metadata::ColumnMetadata;

use crate::error::Result;
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

type ColumnName = String;

/// Encodes index values according to their data types for sorting and storage use.
pub struct IndexValueCodec;

impl IndexValueCodec {
    /// Serializes a `ValueRef` using the data type defined in `SortField` and writes
    /// the result into a buffer.
    ///
    /// # Arguments
    /// * `value` - The value to be encoded.
    /// * `field` - Contains data type to guide serialization.
    /// * `buffer` - Destination buffer for the serialized value.
    pub fn encode_value(value: ValueRef, field: &SortField, buffer: &mut Vec<u8>) -> Result<()> {
        buffer.reserve(field.estimated_size());
        let mut serializer = Serializer::new(buffer);
        field.serialize(&mut serializer, &value)
    }
}

pub struct IndexValuesCodec {
    column_names: Vec<ColumnName>,
    fields: Vec<SortField>,
    decoder: McmpRowCodec,
}

impl IndexValuesCodec {
    pub fn from_tag_columns<'a>(tag_columns: impl Iterator<Item = &'a ColumnMetadata>) -> Self {
        let (column_names, fields): (Vec<_>, Vec<_>) = tag_columns
            .map(|column| {
                (
                    column.column_schema.name.clone(),
                    SortField::new(column.column_schema.data_type.clone()),
                )
            })
            .unzip();

        let decoder = McmpRowCodec::new(fields.clone());
        Self {
            column_names,
            fields,
            decoder,
        }
    }

    pub fn decode(
        &self,
        primary_key: &[u8],
    ) -> Result<impl Iterator<Item = (&ColumnName, &SortField, Option<Value>)>> {
        let values = self.decoder.decode(primary_key)?;

        let iter = values
            .into_iter()
            .zip(&self.column_names)
            .zip(&self.fields)
            .map(|((value, column_name), encoder)| {
                if value.is_null() {
                    (column_name, encoder, None)
                } else {
                    (column_name, encoder, Some(value))
                }
            });

        Ok(iter)
    }
}

#[cfg(test)]
mod tests {
    use datatypes::data_type::ConcreteDataType;

    use super::*;
    use crate::error::Error;

    #[test]
    fn test_encode_value_basic() {
        let value = ValueRef::from("hello");
        let field = SortField::new(ConcreteDataType::string_datatype());

        let mut buffer = Vec::new();
        IndexValueCodec::encode_value(value, &field, &mut buffer).unwrap();
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_encode_value_type_mismatch() {
        let value = ValueRef::from("hello");
        let field = SortField::new(ConcreteDataType::int64_datatype());

        let mut buffer = Vec::new();
        let res = IndexValueCodec::encode_value(value, &field, &mut buffer);
        assert!(matches!(res, Err(Error::FieldTypeMismatch { .. })));
    }
}
