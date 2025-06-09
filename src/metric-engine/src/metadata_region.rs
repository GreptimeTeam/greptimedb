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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
use async_stream::try_stream;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::prelude::{col, lit};
use futures_util::stream::BoxStream;
use futures_util::TryStreamExt;
use mito2::engine::MitoEngine;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    METADATA_SCHEMA_KEY_COLUMN_INDEX, METADATA_SCHEMA_KEY_COLUMN_NAME,
    METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME, METADATA_SCHEMA_VALUE_COLUMN_INDEX,
    METADATA_SCHEMA_VALUE_COLUMN_NAME,
};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionDeleteRequest, RegionPutRequest};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::error::{
    CollectRecordBatchStreamSnafu, DecodeColumnValueSnafu, DeserializeColumnMetadataSnafu,
    LogicalRegionNotFoundSnafu, MitoReadOperationSnafu, MitoWriteOperationSnafu,
    ParseRegionIdSnafu, Result,
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
    pub(crate) mito: MitoEngine,
    /// Logical lock for operations that need to be serialized. Like update & read region columns.
    ///
    /// Region entry will be registered on creating and opening logical region, and deregistered on
    /// removing logical region.
    logical_region_lock: RwLock<HashMap<RegionId, Arc<RwLock<()>>>>,
}

impl MetadataRegion {
    pub fn new(mito: MitoEngine) -> Self {
        Self {
            mito,
            logical_region_lock: RwLock::new(HashMap::new()),
        }
    }

    /// Open a logical region.
    ///
    /// Returns true if the logical region is opened for the first time.
    pub async fn open_logical_region(&self, logical_region_id: RegionId) -> bool {
        match self
            .logical_region_lock
            .write()
            .await
            .entry(logical_region_id)
        {
            Entry::Occupied(_) => false,
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(Arc::new(RwLock::new(())));
                true
            }
        }
    }

    /// Retrieve a read lock guard of given logical region id.
    pub async fn read_lock_logical_region(
        &self,
        logical_region_id: RegionId,
    ) -> Result<OwnedRwLockReadGuard<()>> {
        let lock = self
            .logical_region_lock
            .read()
            .await
            .get(&logical_region_id)
            .context(LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            })?
            .clone();
        Ok(RwLock::read_owned(lock).await)
    }

    /// Retrieve a write lock guard of given logical region id.
    pub async fn write_lock_logical_region(
        &self,
        logical_region_id: RegionId,
    ) -> Result<OwnedRwLockWriteGuard<()>> {
        let lock = self
            .logical_region_lock
            .read()
            .await
            .get(&logical_region_id)
            .context(LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            })?
            .clone();
        Ok(RwLock::write_owned(lock).await)
    }

    /// Remove a registered logical region from metadata.
    ///
    /// This method doesn't check if the previous key exists.
    pub async fn remove_logical_region(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<()> {
        // concat region key
        let region_id = utils::to_metadata_region_id(physical_region_id);
        let region_key = Self::concat_region_key(logical_region_id);

        // concat column keys
        let logical_columns = self
            .logical_columns(physical_region_id, logical_region_id)
            .await?;
        let mut column_keys = logical_columns
            .into_iter()
            .map(|(col, _)| Self::concat_column_key(logical_region_id, &col))
            .collect::<Vec<_>>();

        // remove region key and column keys
        column_keys.push(region_key);
        self.delete(region_id, &column_keys).await?;

        self.logical_region_lock
            .write()
            .await
            .remove(&logical_region_id);

        Ok(())
    }

    // TODO(ruihang): avoid using `get_all`
    /// Get all the columns of a given logical region.
    /// Return a list of (column_name, column_metadata).
    pub async fn logical_columns(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<Vec<(String, ColumnMetadata)>> {
        let metadata_region_id = utils::to_metadata_region_id(physical_region_id);
        let region_column_prefix = Self::concat_column_key_prefix(logical_region_id);

        let mut columns = vec![];
        for (k, v) in self
            .get_all_with_prefix(metadata_region_id, &region_column_prefix)
            .await?
        {
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

    /// Return all logical regions associated with the physical region.
    pub async fn logical_regions(&self, physical_region_id: RegionId) -> Result<Vec<RegionId>> {
        let metadata_region_id = utils::to_metadata_region_id(physical_region_id);

        let mut regions = vec![];
        for k in self
            .get_all_key_with_prefix(metadata_region_id, REGION_PREFIX)
            .await?
        {
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

    pub fn parse_region_key(key: &str) -> Option<&str> {
        key.strip_prefix(REGION_PREFIX)
    }

    /// Parse column key to (logical_region_id, column_name)
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

/// Decode a record batch stream to a stream of items.
pub fn decode_batch_stream<T: Send + 'static>(
    mut record_batch_stream: SendableRecordBatchStream,
    decode: fn(RecordBatch) -> Vec<T>,
) -> BoxStream<'static, Result<T>> {
    let stream = try_stream! {
        while let Some(batch) = record_batch_stream.try_next().await.context(CollectRecordBatchStreamSnafu)? {
            for item in decode(batch) {
                yield item;
            }
        }
    };
    Box::pin(stream)
}

/// Decode a record batch to a list of key and value.
fn decode_record_batch_to_key_and_value(batch: RecordBatch) -> Vec<(String, String)> {
    let key_col = batch.column(0);
    let val_col = batch.column(1);

    (0..batch.num_rows())
        .flat_map(move |row_index| {
            let key = key_col
                .get_ref(row_index)
                .as_string()
                .unwrap()
                .map(|s| s.to_string());

            key.map(|k| {
                (
                    k,
                    val_col
                        .get_ref(row_index)
                        .as_string()
                        .unwrap()
                        .map(|s| s.to_string())
                        .unwrap_or_default(),
                )
            })
        })
        .collect()
}

/// Decode a record batch to a list of key.
fn decode_record_batch_to_key(batch: RecordBatch) -> Vec<String> {
    let key_col = batch.column(0);

    (0..batch.num_rows())
        .flat_map(move |row_index| {
            let key = key_col
                .get_ref(row_index)
                .as_string()
                .unwrap()
                .map(|s| s.to_string());
            key
        })
        .collect()
}

// simulate to `KvBackend`
//
// methods in this block assume the given region id is transformed.
impl MetadataRegion {
    fn build_prefix_read_request(prefix: &str, key_only: bool) -> ScanRequest {
        let filter_expr = col(METADATA_SCHEMA_KEY_COLUMN_NAME).like(lit(prefix));

        let projection = if key_only {
            vec![METADATA_SCHEMA_KEY_COLUMN_INDEX]
        } else {
            vec![
                METADATA_SCHEMA_KEY_COLUMN_INDEX,
                METADATA_SCHEMA_VALUE_COLUMN_INDEX,
            ]
        };
        ScanRequest {
            projection: Some(projection),
            filters: vec![filter_expr],
            ..Default::default()
        }
    }

    pub async fn get_all_with_prefix(
        &self,
        region_id: RegionId,
        prefix: &str,
    ) -> Result<HashMap<String, String>> {
        let scan_req = MetadataRegion::build_prefix_read_request(prefix, false);
        let record_batch_stream = self
            .mito
            .scan_to_stream(region_id, scan_req)
            .await
            .context(MitoReadOperationSnafu)?;

        decode_batch_stream(record_batch_stream, decode_record_batch_to_key_and_value)
            .try_collect::<HashMap<_, _>>()
            .await
    }

    pub async fn get_all_key_with_prefix(
        &self,
        region_id: RegionId,
        prefix: &str,
    ) -> Result<Vec<String>> {
        let scan_req = MetadataRegion::build_prefix_read_request(prefix, true);
        let record_batch_stream = self
            .mito
            .scan_to_stream(region_id, scan_req)
            .await
            .context(MitoReadOperationSnafu)?;

        decode_batch_stream(record_batch_stream, decode_record_batch_to_key)
            .try_collect::<Vec<_>>()
            .await
    }

    /// Delete the given keys. For performance consideration, this method
    /// doesn't check if those keys exist or not.
    async fn delete(&self, region_id: RegionId, keys: &[String]) -> Result<()> {
        let delete_request = Self::build_delete_request(keys);
        self.mito
            .handle_request(
                region_id,
                store_api::region_request::RegionRequest::Delete(delete_request),
            )
            .await
            .context(MitoWriteOperationSnafu)?;
        Ok(())
    }

    pub(crate) fn build_put_request_from_iter(
        kv: impl Iterator<Item = (String, String)>,
    ) -> RegionPutRequest {
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
            rows: kv
                .into_iter()
                .map(|(key, value)| Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(0)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue(key)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue(value)),
                        },
                    ],
                })
                .collect(),
        };

        RegionPutRequest { rows, hint: None }
    }

    fn build_delete_request(keys: &[String]) -> RegionDeleteRequest {
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
        ];
        let rows = keys
            .iter()
            .map(|key| Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(0)),
                    },
                    Value {
                        value_data: Some(ValueData::StringValue(key.to_string())),
                    },
                ],
            })
            .collect();
        let rows = Rows { schema: cols, rows };

        RegionDeleteRequest { rows }
    }

    /// Add logical regions to the metadata region.
    pub async fn add_logical_regions(
        &self,
        physical_region_id: RegionId,
        write_region_id: bool,
        logical_regions: impl Iterator<Item = (RegionId, HashMap<&str, &ColumnMetadata>)>,
    ) -> Result<()> {
        let region_id = utils::to_metadata_region_id(physical_region_id);
        let iter = logical_regions
            .into_iter()
            .flat_map(|(logical_region_id, column_metadatas)| {
                if write_region_id {
                    Some((
                        MetadataRegion::concat_region_key(logical_region_id),
                        String::new(),
                    ))
                } else {
                    None
                }
                .into_iter()
                .chain(column_metadatas.into_iter().map(
                    move |(name, column_metadata)| {
                        (
                            MetadataRegion::concat_column_key(logical_region_id, name),
                            MetadataRegion::serialize_column_metadata(column_metadata),
                        )
                    },
                ))
            })
            .collect::<Vec<_>>();

        let put_request = MetadataRegion::build_put_request_from_iter(iter.into_iter());
        self.mito
            .handle_request(
                region_id,
                store_api::region_request::RegionRequest::Put(put_request),
            )
            .await
            .context(MitoWriteOperationSnafu)?;

        Ok(())
    }
}

#[cfg(test)]
impl MetadataRegion {
    /// Retrieves the value associated with the given key in the specified region.
    /// Returns `Ok(None)` if the key is not found.
    pub async fn get(&self, region_id: RegionId, key: &str) -> Result<Option<String>> {
        let filter_expr = datafusion::prelude::col(METADATA_SCHEMA_KEY_COLUMN_NAME)
            .eq(datafusion::prelude::lit(key));

        let scan_req = ScanRequest {
            projection: Some(vec![METADATA_SCHEMA_VALUE_COLUMN_INDEX]),
            filters: vec![filter_expr],
            ..Default::default()
        };
        let record_batch_stream = self
            .mito
            .scan_to_stream(region_id, scan_req)
            .await
            .context(MitoReadOperationSnafu)?;
        let scan_result = common_recordbatch::util::collect(record_batch_stream)
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
}

#[cfg(test)]
mod test {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;

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

    fn test_column_metadatas() -> HashMap<String, ColumnMetadata> {
        HashMap::from([
            (
                "label1".to_string(),
                ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "label1".to_string(),
                        ConcreteDataType::string_datatype(),
                        false,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 5,
                },
            ),
            (
                "label2".to_string(),
                ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "label2".to_string(),
                        ConcreteDataType::string_datatype(),
                        false,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 5,
                },
            ),
        ])
    }

    #[tokio::test]
    async fn add_logical_regions_to_meta_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let metadata_region = env.metadata_region();
        let physical_region_id = to_metadata_region_id(env.default_physical_region_id());
        let column_metadatas = test_column_metadatas();
        let logical_region_id = RegionId::new(1024, 1);

        let iter = vec![(
            logical_region_id,
            column_metadatas
                .iter()
                .map(|(k, v)| (k.as_str(), v))
                .collect::<HashMap<_, _>>(),
        )];
        metadata_region
            .add_logical_regions(physical_region_id, true, iter.into_iter())
            .await
            .unwrap();
        // Add logical region again.
        let iter = vec![(
            logical_region_id,
            column_metadatas
                .iter()
                .map(|(k, v)| (k.as_str(), v))
                .collect::<HashMap<_, _>>(),
        )];
        metadata_region
            .add_logical_regions(physical_region_id, true, iter.into_iter())
            .await
            .unwrap();

        // Check if the logical region is added.
        let logical_regions = metadata_region
            .logical_regions(physical_region_id)
            .await
            .unwrap();
        assert_eq!(logical_regions.len(), 2);

        // Check if the logical region columns are added.
        let logical_columns = metadata_region
            .logical_columns(physical_region_id, logical_region_id)
            .await
            .unwrap()
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(logical_columns.len(), 2);
        assert_eq!(column_metadatas, logical_columns);
    }
}
