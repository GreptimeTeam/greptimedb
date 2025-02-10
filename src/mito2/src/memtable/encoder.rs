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

//! Sparse primary key encoder;

use datatypes::prelude::ValueRef;
use memcomparable::Serializer;
use serde::Serialize;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::SerializeFieldSnafu;
use crate::row_converter::SortField;

pub(crate) struct FieldWithId {
    pub(crate) field: SortField,
    pub(crate) column_id: ColumnId,
}

pub(crate) struct SparseEncoder {
    pub(crate) columns: Vec<FieldWithId>,
    #[cfg(test)]
    pub(crate) column_id_to_field: std::collections::HashMap<ColumnId, (SortField, usize)>,
}

impl SparseEncoder {
    pub(crate) fn new(metadata: &RegionMetadataRef) -> Self {
        let mut columns = Vec::with_capacity(metadata.primary_key.len());
        #[cfg(test)]
        let mut column_id_to_field =
            std::collections::HashMap::with_capacity(metadata.primary_key.len());
        for (_idx, c) in metadata.primary_key_columns().enumerate() {
            let sort_field = SortField::new(c.column_schema.data_type.clone());

            let field = FieldWithId {
                field: sort_field.clone(),
                column_id: c.column_id,
            };
            columns.push(field);
            #[cfg(test)]
            column_id_to_field.insert(c.column_id, (sort_field, _idx));
        }
        Self {
            columns,
            #[cfg(test)]
            column_id_to_field,
        }
    }

    pub fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> crate::error::Result<()>
    where
        I: Iterator<Item = ValueRef<'a>>,
    {
        let mut serializer = Serializer::new(buffer);
        for (value, field) in row.zip(self.columns.iter()) {
            if !value.is_null() {
                field
                    .column_id
                    .serialize(&mut serializer)
                    .context(SerializeFieldSnafu)?;
                field.field.serialize(&mut serializer, &value)?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn decode(&self, bytes: &[u8]) -> crate::error::Result<Vec<datatypes::value::Value>> {
        use serde::Deserialize;
        let mut deserializer = memcomparable::Deserializer::new(bytes);
        let mut values = vec![datatypes::value::Value::Null; self.columns.len()];

        while deserializer.has_remaining() {
            let column_id =
                u32::deserialize(&mut deserializer).context(crate::error::DeserializeFieldSnafu)?;
            let (field, idx) = self.column_id_to_field.get(&column_id).unwrap();
            let value = field.deserialize(&mut deserializer)?;
            values[*idx] = value;
        }
        Ok(values)
    }
}
