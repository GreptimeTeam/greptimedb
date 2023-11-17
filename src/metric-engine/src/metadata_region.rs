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

use api::v1::value::ValueData;
use api::v1::{self, ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use common_recordbatch::util::collect;
use datafusion::prelude::{col, lit, Expr};
use datatypes::vectors::StringVector;
use mito2::engine::MitoEngine;
use snafu::ResultExt;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionPutRequest, RegionReadRequest};
use store_api::storage::{RegionId, ScanRequest, TableId};

use crate::consts::{
    METADATA_SCHEMA_KEY_COLUMN_NAME, METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME,
    METADATA_SCHEMA_VALUE_COLUMN_INDEX, METADATA_SCHEMA_VALUE_COLUMN_NAME,
};
use crate::error::{
    CollectRecordBatchStreamSnafu, DecodeColumnValueSnafu, DeserializeSemanticTypeSnafu,
    MitoReadOperationSnafu, MitoWriteOperationSnafu, ParseRegionIdSnafu, RegionAlreadyExistsSnafu,
    Result,
};
use crate::utils;

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
        column_name: &str,
        semantic_type: SemanticType,
    ) -> Result<bool> {
        let region_id = utils::to_metadata_region_id(physical_region_id);
        let column_key = Self::concat_column_key(logical_region_id, column_name);

        self.put_if_absent(
            region_id,
            column_key,
            Self::serialize_semantic_type(semantic_type),
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
            .map(|s| Self::deserialize_semantic_type(&s))
            .transpose()
    }
}

// utils to concat and parse key/value
impl MetadataRegion {
    pub fn concat_region_key(region_id: RegionId) -> String {
        format!("__region_{}", region_id.as_u64())
    }

    /// Column name will be encoded by base64([STANDARD_NO_PAD])
    pub fn concat_column_key(region_id: RegionId, column_name: &str) -> String {
        let encoded_column_name = STANDARD_NO_PAD.encode(column_name);
        format!("__column_{}_{}", region_id.as_u64(), encoded_column_name)
    }

    pub fn parse_region_key(key: &str) -> Option<&str> {
        key.strip_prefix("__region_")
    }

    /// Parse column key to (logical_region_id, column_name)
    pub fn parse_column_key(key: &str) -> Result<Option<(RegionId, String)>> {
        if let Some(stripped) = key.strip_prefix("__column_") {
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
        let mut scan_result = collect(record_batch_stream)
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

    /// Builds a [ScanRequest] to read metadata for a given key.
    /// The request will contains a EQ filter on the key column.
    ///
    /// Only the value column is projected.
    fn build_read_request(key: &str) -> ScanRequest {
        let filter_expr = col(METADATA_SCHEMA_KEY_COLUMN_NAME).eq(lit(key));

        ScanRequest {
            sequence: None,
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
            },
            ColumnSchema {
                column_name: METADATA_SCHEMA_KEY_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String as _,
                semantic_type: SemanticType::Tag as _,
            },
            ColumnSchema {
                column_name: METADATA_SCHEMA_VALUE_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String as _,
                semantic_type: SemanticType::Field as _,
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

    #[test]
    fn test_build_read_request() {
        let key = "test_key";
        let expected_filter_expr = col(METADATA_SCHEMA_KEY_COLUMN_NAME).eq(lit(key));
        let expected_scan_request = ScanRequest {
            sequence: None,
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
        metadata_region
            .add_column(
                physical_region_id,
                logical_region_id,
                column_name,
                semantic_type,
            )
            .await
            .unwrap();
        let actual_semantic_type = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, column_name)
            .await
            .unwrap();
        assert_eq!(actual_semantic_type, Some(semantic_type));

        // duplicate column won't be updated
        let is_updated = metadata_region
            .add_column(
                physical_region_id,
                logical_region_id,
                column_name,
                SemanticType::Field,
            )
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
