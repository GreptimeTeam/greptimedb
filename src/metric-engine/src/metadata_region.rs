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

use std::collections::HashMap;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use common_recordbatch::util::collect;
use datafusion::prelude::{col, lit};
use mito2::engine::MitoEngine;
use snafu::ResultExt;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    METADATA_SCHEMA_KEY_COLUMN_INDEX, METADATA_SCHEMA_KEY_COLUMN_NAME,
    METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME, METADATA_SCHEMA_VALUE_COLUMN_INDEX,
    METADATA_SCHEMA_VALUE_COLUMN_NAME,
};
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionPutRequest;
use store_api::storage::{RegionId, ScanRequest};

use crate::error::{
    CollectRecordBatchStreamSnafu, DecodeColumnValueSnafu, DeserializeColumnMetadataSnafu,
    MitoReadOperationSnafu, MitoWriteOperationSnafu, ParseRegionIdSnafu, RegionAlreadyExistsSnafu,
    Result,
};
use crate::utils;

const REGION_PREFIX: &str = "__region_";
const COLUMN_PREFIX: &str = "__column_";

/// The other two fields key and value will be used as a k-v storage.
/// It contains two group of key:
/// - `__region_<LOGICAL_REGION_ID>` is used for marking table existence. It doesn't have value.
/// - `__column_<LOGICAL_REGION_ID>_<COLUMN_NAME>` is used for marking column existence,
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
    pub fn new(mito: MitoEngine) -> Self {
        Self { mito }
    }

    /// Add a new table key to metadata.
    ///
    /// This method will check if the table key already exists, if so, it will return
    /// a [TableAlreadyExistsSnafu] error.
    pub async fn add_logical_region(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<()> {
        let region_id = utils::to_metadata_region_id(physical_region_id);
        let region_key = Self::concat_region_key(logical_region_id);

        let put_success = self
            .put_if_absent(region_id, region_key, String::new())
            .await?;

        if !put_success {
            RegionAlreadyExistsSnafu {
                region_id: logical_region_id,
            }
            .fail()
        } else {
            Ok(())
        }
    }

    /// Add a new column key to metadata.
    ///
    /// This method won't check if the column already exists. But
    /// will return if the column is successfully added.
    pub async fn add_column(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        column_metadata: &ColumnMetadata,
    ) -> Result<bool> {
        let region_id = utils::to_metadata_region_id(physical_region_id);
        let column_key =
            Self::concat_column_key(logical_region_id, &column_metadata.column_schema.name);

        self.put_if_absent(
            region_id,
            column_key,
            Self::serialize_column_metadata(column_metadata),
        )
        .await
    }

    /// Check if the given logical region exists.
    pub async fn is_logical_region_exists(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<bool> {
        let region_id = utils::to_metadata_region_id(physical_region_id);
        let region_key = Self::concat_region_key(logical_region_id);
        self.exists(region_id, &region_key).await
    }

    /// Check if the given column exists. Return the semantic type if exists.
    pub async fn column_semantic_type(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        column_name: &str,
    ) -> Result<Option<SemanticType>> {
        let region_id = utils::to_metadata_region_id(physical_region_id);
        let column_key = Self::concat_column_key(logical_region_id, column_name);
        let semantic_type = self.get(region_id, &column_key).await?;
        semantic_type
            .map(|s| Self::deserialize_column_metadata(&s).map(|c| c.semantic_type))
            .transpose()
    }

    // TODO(ruihang): avoid using `get_all`
    /// Get all the columns of a given logical region.
    /// Return a list of (column_name, semantic_type).
    pub async fn logical_columns(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<Vec<(String, ColumnMetadata)>> {
        let metadata_region_id = utils::to_metadata_region_id(physical_region_id);
        let region_column_prefix = Self::concat_column_key_prefix(logical_region_id);

        let mut columns = vec![];
        for (k, v) in self.get_all(metadata_region_id).await? {
            if !k.starts_with(&region_column_prefix) {
                continue;
            }
            // Safety: we have checked the prefix
            let (_, column_name) = Self::parse_column_key(&k)?.unwrap();
            let column_metadata = Self::deserialize_column_metadata(&v)?;
            columns.push((column_name, column_metadata));
        }

        Ok(columns)
    }

    pub async fn logical_regions(&self, physical_region_id: RegionId) -> Result<Vec<RegionId>> {
        let metadata_region_id = utils::to_metadata_region_id(physical_region_id);

        let mut regions = vec![];
        for (k, _) in self.get_all(metadata_region_id).await? {
            if !k.starts_with(REGION_PREFIX) {
                continue;
            }
            // Safety: we have checked the prefix
            let region_id = Self::parse_region_key(&k).unwrap();
            let region_id = region_id.parse::<u64>().unwrap().into();
            regions.push(region_id);
        }

        Ok(regions)
    }
}

// utils to concat and parse key/value
impl MetadataRegion {
    pub fn concat_region_key(region_id: RegionId) -> String {
        format!("{REGION_PREFIX}{}", region_id.as_u64())
    }

    /// Column name will be encoded by base64([STANDARD_NO_PAD])
    pub fn concat_column_key(region_id: RegionId, column_name: &str) -> String {
        let encoded_column_name = STANDARD_NO_PAD.encode(column_name);
        format!(
            "{COLUMN_PREFIX}{}_{}",
            region_id.as_u64(),
            encoded_column_name
        )
    }

    /// Concat a column key prefix without column name
    pub fn concat_column_key_prefix(region_id: RegionId) -> String {
        format!("{COLUMN_PREFIX}{}_", region_id.as_u64())
    }

    #[allow(dead_code)]
    pub fn parse_region_key(key: &str) -> Option<&str> {
        key.strip_prefix(REGION_PREFIX)
    }

    /// Parse column key to (logical_region_id, column_name)
    #[allow(dead_code)]
    pub fn parse_column_key(key: &str) -> Result<Option<(RegionId, String)>> {
        if let Some(stripped) = key.strip_prefix(COLUMN_PREFIX) {
            let mut iter = stripped.split('_');

            let region_id_raw = iter.next().unwrap();
            let region_id = region_id_raw
                .parse::<u64>()
                .with_context(|_| ParseRegionIdSnafu { raw: region_id_raw })?
                .into();

            let encoded_column_name = iter.next().unwrap();
            let column_name = STANDARD_NO_PAD
                .decode(encoded_column_name)
                .context(DecodeColumnValueSnafu)?;

            Ok(Some((region_id, String::from_utf8(column_name).unwrap())))
        } else {
            Ok(None)
        }
    }

    pub fn serialize_column_metadata(column_metadata: &ColumnMetadata) -> String {
        serde_json::to_string(column_metadata).unwrap()
    }

    pub fn deserialize_column_metadata(column_metadata: &str) -> Result<ColumnMetadata> {
        serde_json::from_str(column_metadata).with_context(|_| DeserializeColumnMetadataSnafu {
            raw: column_metadata,
        })
    }
}

// simulate to `KvBackend`
//
// methods in this block assume the given region id is transformed.
#[allow(unused_variables)]
impl MetadataRegion {
    /// Put if not exist, return if this put operation is successful (error other
    /// than "key already exist" will be wrapped in [Err]).
    pub async fn put_if_absent(
        &self,
        region_id: RegionId,
        key: String,
        value: String,
    ) -> Result<bool> {
        if self.exists(region_id, &key).await? {
            return Ok(false);
        }

        let put_request = Self::build_put_request(&key, &value);
        self.mito
            .handle_request(
                region_id,
                store_api::region_request::RegionRequest::Put(put_request),
            )
            .await
            .context(MitoWriteOperationSnafu)?;
        Ok(true)
    }

    /// Check if the given key exists.
    ///
    /// Notice that due to mito doesn't support transaction, TOCTTOU is possible.
    pub async fn exists(&self, region_id: RegionId, key: &str) -> Result<bool> {
        let scan_req = Self::build_read_request(key);
        let record_batch_stream = self
            .mito
            .handle_query(region_id, scan_req)
            .await
            .context(MitoReadOperationSnafu)?;
        let scan_result = collect(record_batch_stream)
            .await
            .context(CollectRecordBatchStreamSnafu)?;

        let exist = !scan_result.is_empty() && scan_result.first().unwrap().num_rows() != 0;
        Ok(exist)
    }

    /// Retrieves the value associated with the given key in the specified region.
    /// Returns `Ok(None)` if the key is not found.
    pub async fn get(&self, region_id: RegionId, key: &str) -> Result<Option<String>> {
        let scan_req = Self::build_read_request(key);
        let record_batch_stream = self
            .mito
            .handle_query(region_id, scan_req)
            .await
            .context(MitoReadOperationSnafu)?;
        let scan_result = collect(record_batch_stream)
            .await
            .context(CollectRecordBatchStreamSnafu)?;

        let Some(first_batch) = scan_result.first() else {
            return Ok(None);
        };

        let val = first_batch
            .column(0)
            .get_ref(0)
            .as_string()
            .unwrap()
            .map(|s| s.to_string());

        Ok(val)
    }

    /// Load all metadata from a given region.
    pub async fn get_all(&self, region_id: RegionId) -> Result<HashMap<String, String>> {
        let scan_req = ScanRequest {
            projection: Some(vec![
                METADATA_SCHEMA_KEY_COLUMN_INDEX,
                METADATA_SCHEMA_VALUE_COLUMN_INDEX,
            ]),
            filters: vec![],
            output_ordering: None,
            limit: None,
        };
        let record_batch_stream = self
            .mito
            .handle_query(region_id, scan_req)
            .await
            .context(MitoReadOperationSnafu)?;
        let scan_result = collect(record_batch_stream)
            .await
            .context(CollectRecordBatchStreamSnafu)?;

        let mut result = HashMap::new();
        for batch in scan_result {
            let key_col = batch.column(0);
            let val_col = batch.column(1);
            for row_index in 0..batch.num_rows() {
                let key = key_col
                    .get_ref(row_index)
                    .as_string()
                    .unwrap()
                    .map(|s| s.to_string());
                let val = val_col
                    .get_ref(row_index)
                    .as_string()
                    .unwrap()
                    .map(|s| s.to_string());
                result.insert(key.unwrap(), val.unwrap_or_default());
            }
        }
        Ok(result)
    }

    /// Builds a [ScanRequest] to read metadata for a given key.
    /// The request will contains a EQ filter on the key column.
    ///
    /// Only the value column is projected.
    fn build_read_request(key: &str) -> ScanRequest {
        let filter_expr = col(METADATA_SCHEMA_KEY_COLUMN_NAME).eq(lit(key));

        ScanRequest {
            projection: Some(vec![METADATA_SCHEMA_VALUE_COLUMN_INDEX]),
            filters: vec![filter_expr.into()],
            output_ordering: None,
            limit: None,
        }
    }

    fn build_put_request(key: &str, value: &str) -> RegionPutRequest {
        let cols = vec![
            ColumnSchema {
                column_name: METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::TimestampMillisecond as _,
                semantic_type: SemanticType::Timestamp as _,
                ..Default::default()
            },
            ColumnSchema {
                column_name: METADATA_SCHEMA_KEY_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String as _,
                semantic_type: SemanticType::Tag as _,
                ..Default::default()
            },
            ColumnSchema {
                column_name: METADATA_SCHEMA_VALUE_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String as _,
                semantic_type: SemanticType::Field as _,
                ..Default::default()
            },
        ];
        let rows = Rows {
            schema: cols,
            rows: vec![Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(0)),
                    },
                    Value {
                        value_data: Some(ValueData::StringValue(key.to_string())),
                    },
                    Value {
                        value_data: Some(ValueData::StringValue(value.to_string())),
                    },
                ],
            }],
        };

        RegionPutRequest { rows }
    }
}

#[cfg(test)]
mod test {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::region_request::RegionRequest;

    use super::*;
    use crate::test_util::TestEnv;
    use crate::utils::to_metadata_region_id;

    #[test]
    fn test_concat_table_key() {
        let region_id = RegionId::new(1234, 7844);
        let expected = "__region_5299989651108".to_string();
        assert_eq!(MetadataRegion::concat_region_key(region_id), expected);
    }

    #[test]
    fn test_concat_column_key() {
        let region_id = RegionId::new(8489, 9184);
        let column_name = "my_column";
        let expected = "__column_36459977384928_bXlfY29sdW1u".to_string();
        assert_eq!(
            MetadataRegion::concat_column_key(region_id, column_name),
            expected
        );
    }

    #[test]
    fn test_parse_table_key() {
        let region_id = RegionId::new(87474, 10607);
        let encoded = MetadataRegion::concat_column_key(region_id, "my_column");
        assert_eq!(encoded, "__column_375697969260911_bXlfY29sdW1u");

        let decoded = MetadataRegion::parse_column_key(&encoded).unwrap();
        assert_eq!(decoded, Some((region_id, "my_column".to_string())));
    }

    #[test]
    fn test_parse_valid_column_key() {
        let region_id = RegionId::new(176, 910);
        let encoded = MetadataRegion::concat_column_key(region_id, "my_column");
        assert_eq!(encoded, "__column_755914245006_bXlfY29sdW1u");

        let decoded = MetadataRegion::parse_column_key(&encoded).unwrap();
        assert_eq!(decoded, Some((region_id, "my_column".to_string())));
    }

    #[test]
    fn test_parse_invalid_column_key() {
        let key = "__column_asdfasd_????";
        let result = MetadataRegion::parse_column_key(key);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_column_metadata() {
        let semantic_type = SemanticType::Tag;
        let column_metadata = ColumnMetadata {
            column_schema: ColumnSchema::new("blabla", ConcreteDataType::string_datatype(), false),
            semantic_type,
            column_id: 5,
        };
        let expected = "{\"column_schema\":{\"name\":\"blabla\",\"data_type\":{\"String\":null},\"is_nullable\":false,\"is_time_index\":false,\"default_constraint\":null,\"metadata\":{}},\"semantic_type\":\"Tag\",\"column_id\":5}".to_string();
        assert_eq!(
            MetadataRegion::serialize_column_metadata(&column_metadata),
            expected
        );

        let semantic_type = "\"Invalid Column Metadata\"";
        assert!(MetadataRegion::deserialize_column_metadata(semantic_type).is_err());
    }

    #[test]
    fn test_build_read_request() {
        let key = "test_key";
        let expected_filter_expr = col(METADATA_SCHEMA_KEY_COLUMN_NAME).eq(lit(key));
        let expected_scan_request = ScanRequest {
            projection: Some(vec![METADATA_SCHEMA_VALUE_COLUMN_INDEX]),
            filters: vec![expected_filter_expr.into()],
            output_ordering: None,
            limit: None,
        };
        let actual_scan_request = MetadataRegion::build_read_request(key);
        assert_eq!(actual_scan_request, expected_scan_request);
    }

    #[tokio::test]
    async fn test_put_conditionally() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let metadata_region = env.metadata_region();
        let region_id = to_metadata_region_id(env.default_physical_region_id());

        // Test inserting a new key-value pair
        let key = "test_key".to_string();
        let value = "test_value".to_string();
        let result = metadata_region
            .put_if_absent(region_id, key.clone(), value.clone())
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Verify that the key-value pair was actually inserted
        let scan_req = MetadataRegion::build_read_request("test_key");
        let record_batch_stream = metadata_region
            .mito
            .handle_query(region_id, scan_req)
            .await
            .unwrap();
        let scan_result = collect(record_batch_stream).await.unwrap();
        assert_eq!(scan_result.len(), 1);

        // Test inserting the same key-value pair again
        let result = metadata_region
            .put_if_absent(region_id, key.clone(), value.clone())
            .await;
        assert!(result.is_ok());
        assert!(!result.unwrap(),);
    }

    #[tokio::test]
    async fn test_exist() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let metadata_region = env.metadata_region();
        let region_id = to_metadata_region_id(env.default_physical_region_id());

        // Test checking for a non-existent key
        let key = "test_key".to_string();
        let result = metadata_region.exists(region_id, &key).await;
        assert!(result.is_ok());
        assert!(!result.unwrap());

        // Test inserting a key and then checking for its existence
        let value = "test_value".to_string();
        let put_request = MetadataRegion::build_put_request(&key, &value);
        metadata_region
            .mito
            .handle_request(region_id, RegionRequest::Put(put_request))
            .await
            .unwrap();
        let result = metadata_region.exists(region_id, &key).await;
        assert!(result.is_ok());
        assert!(result.unwrap(),);
    }

    #[tokio::test]
    async fn test_get() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let metadata_region = env.metadata_region();
        let region_id = to_metadata_region_id(env.default_physical_region_id());

        // Test getting a non-existent key
        let key = "test_key".to_string();
        let result = metadata_region.get(region_id, &key).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        // Test inserting a key and then getting its value
        let value = "test_value".to_string();
        let put_request = MetadataRegion::build_put_request(&key, &value);
        metadata_region
            .mito
            .handle_request(region_id, RegionRequest::Put(put_request))
            .await
            .unwrap();
        let result = metadata_region.get(region_id, &key).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(value));
    }

    #[tokio::test]
    async fn test_add_logical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let metadata_region = env.metadata_region();
        let physical_region_id = to_metadata_region_id(env.default_physical_region_id());

        // add one table
        let logical_region_id = RegionId::new(196, 2333);
        metadata_region
            .add_logical_region(physical_region_id, logical_region_id)
            .await
            .unwrap();
        assert!(metadata_region
            .is_logical_region_exists(physical_region_id, logical_region_id)
            .await
            .unwrap());

        // add it again
        assert!(metadata_region
            .add_logical_region(physical_region_id, logical_region_id)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_add_column() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let metadata_region = env.metadata_region();
        let physical_region_id = to_metadata_region_id(env.default_physical_region_id());

        let logical_region_id = RegionId::new(868, 8390);
        let column_name = "column1";
        let semantic_type = SemanticType::Tag;
        let column_metadata = ColumnMetadata {
            column_schema: ColumnSchema::new(
                column_name,
                ConcreteDataType::string_datatype(),
                false,
            ),
            semantic_type,
            column_id: 5,
        };
        metadata_region
            .add_column(physical_region_id, logical_region_id, &column_metadata)
            .await
            .unwrap();
        let actual_semantic_type = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, column_name)
            .await
            .unwrap();
        assert_eq!(actual_semantic_type, Some(semantic_type));

        // duplicate column won't be updated
        let is_updated = metadata_region
            .add_column(physical_region_id, logical_region_id, &column_metadata)
            .await
            .unwrap();
        assert!(!is_updated);
        let actual_semantic_type = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, column_name)
            .await
            .unwrap();
        assert_eq!(actual_semantic_type, Some(semantic_type));
    }
}
