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
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use mito2::engine::MitoEngine;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{
    DecodeColumnValueSnafu, DeserializeSemanticTypeSnafu, Result, TableAlreadyExistsSnafu,
};
use crate::utils;

/// The other two fields key and value will be used as a k-v storage.
/// It contains two group of key:
/// - `__table_<TABLE_NAME>` is used for marking table existence. It doesn't have value.
/// - `__column_<TABLE_NAME>_<COLUMN_NAME>` is used for marking column existence,
///   the value is column's semantic type. To avoid the key conflict, this column key
///   will be encoded by base64([STANDARD_NO_PAD]).
///
/// This is a generic handler like [MetricEngine](crate::engine::MetricEngine). It
/// will handle all the metadata related operations across physical tables. Thus
/// every operation should be associated to a [RegionId], which is the physical
/// table id + region sequence. This handler will transform the region group by
/// itself.
pub struct MetadataRegion {
    mito: MitoEngine,
}

impl MetadataRegion {
    /// Add a new table key to metadata.
    ///
    /// This method will check if the table key already exists, if so, it will return
    /// a [TableAlreadyExistsSnafu] error.
    pub fn add_table(&self, region_id: RegionId, table_name: &str) -> Result<()> {
        let region_id = utils::to_metadata_region_id(region_id);
        let table_key = Self::concat_table_key(table_name);

        let put_success = self.put_conditionally(region_id, table_key, String::new())?;

        if !put_success {
            TableAlreadyExistsSnafu { table_name }.fail()
        } else {
            Ok(())
        }
    }

    /// Add a new column key to metadata.
    ///
    /// This method won't check if the column already exists.
    pub fn add_column(
        &self,
        region_id: RegionId,
        table_name: &str,
        column_name: &str,
        semantic_type: SemanticType,
    ) -> Result<()> {
        let region_id = utils::to_metadata_region_id(region_id);
        let column_key = Self::concat_column_key(table_name, column_name);

        self.put_conditionally(
            region_id,
            column_key,
            Self::serialize_semantic_type(semantic_type),
        )?;
        Ok(())
    }
}

// utils to concat and parse key/value
impl MetadataRegion {
    pub fn concat_table_key(table_name: &str) -> String {
        format!("__table_{}", table_name)
    }

    pub fn concat_column_key(table_name: &str, column_name: &str) -> String {
        let encoded_table_name = STANDARD_NO_PAD.encode(table_name);
        let encoded_column_name = STANDARD_NO_PAD.encode(column_name);
        format!("__column_{}_{}", encoded_table_name, encoded_column_name)
    }

    pub fn parse_table_key(key: &str) -> Option<&str> {
        key.strip_prefix("__table_")
    }

    /// Parse column key to (table_name, column_name)
    pub fn parse_column_key(key: &str) -> Result<Option<(String, String)>> {
        if let Some(stripped) = key.strip_prefix("__column_") {
            let mut iter = stripped.split('_');
            let encoded_table_name = iter.next().unwrap();
            let encoded_column_name = iter.next().unwrap();

            let table_name = STANDARD_NO_PAD
                .decode(encoded_table_name)
                .context(DecodeColumnValueSnafu)?;
            let column_name = STANDARD_NO_PAD
                .decode(encoded_column_name)
                .context(DecodeColumnValueSnafu)?;

            Ok(Some((
                String::from_utf8(table_name).unwrap(),
                String::from_utf8(column_name).unwrap(),
            )))
        } else {
            Ok(None)
        }
    }

    pub fn serialize_semantic_type(semantic_type: SemanticType) -> String {
        serde_json::to_string(&semantic_type).unwrap()
    }

    pub fn deserialize_semantic_type(semantic_type: &str) -> Result<SemanticType> {
        serde_json::from_str(semantic_type)
            .with_context(|_| DeserializeSemanticTypeSnafu { raw: semantic_type })
    }
}

// simulate to `KvBackend`
//
// methods in this block assume the given region id is transformed.
#[allow(unused_variables)]
impl MetadataRegion {
    /// Put if not exist, return if this put operation is successful (error other
    /// than "key already exist" will be wrapped in [Err]).
    pub fn put_conditionally(
        &self,
        region_id: RegionId,
        key: String,
        value: String,
    ) -> Result<bool> {
        todo!()
    }

    /// Check if the given key exists.
    pub fn exist(&self, region_id: RegionId, key: &str) -> Result<bool> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_concat_table_key() {
        let table_name = "my_table";
        let expected = "__table_my_table".to_string();
        assert_eq!(MetadataRegion::concat_table_key(table_name), expected);
    }

    #[test]
    fn test_concat_column_key() {
        let table_name = "my_table";
        let column_name = "my_column";
        let expected = "__column_bXlfdGFibGU_bXlfY29sdW1u".to_string();
        assert_eq!(
            MetadataRegion::concat_column_key(table_name, column_name),
            expected
        );
    }

    #[test]
    fn test_parse_table_key() {
        let encoded = MetadataRegion::concat_column_key("my_table", "my_column");
        assert_eq!(encoded, "__column_bXlfdGFibGU_bXlfY29sdW1u");

        let decoded = MetadataRegion::parse_column_key(&encoded).unwrap();
        assert_eq!(
            decoded,
            Some(("my_table".to_string(), "my_column".to_string()))
        );
    }

    #[test]
    fn test_parse_valid_column_key() {
        let encoded = MetadataRegion::concat_column_key("my_table", "my_column");
        assert_eq!(encoded, "__column_bXlfdGFibGU_bXlfY29sdW1u");

        let decoded = MetadataRegion::parse_column_key(&encoded).unwrap();
        assert_eq!(
            decoded,
            Some(("my_table".to_string(), "my_column".to_string()))
        );
    }

    #[test]
    fn test_parse_invalid_column_key() {
        let key = "__column_asdfasd_????";
        let result = MetadataRegion::parse_column_key(key);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_semantic_type() {
        let semantic_type = SemanticType::Tag;
        let expected = "\"Tag\"".to_string();
        assert_eq!(
            MetadataRegion::serialize_semantic_type(semantic_type),
            expected
        );
    }

    #[test]
    fn test_deserialize_semantic_type() {
        let semantic_type = "\"Tag\"";
        let expected = SemanticType::Tag;
        assert_eq!(
            MetadataRegion::deserialize_semantic_type(semantic_type).unwrap(),
            expected
        );

        let semantic_type = "\"InvalidType\"";
        assert!(MetadataRegion::deserialize_semantic_type(semantic_type).is_err());
    }
}
