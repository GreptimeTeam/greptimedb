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
use serde::{Deserialize, Serialize};
use store_api::region_engine::RawLabelSource;
use store_api::storage::{ColumnId, RegionId};

/// Reserved internal control for a FE-authorized raw-series region read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSeriesRead {
    /// Version of this internal control format.
    pub version: u32,
    /// Expected destination region.
    pub region_id: RegionId,
    /// Metric name bound by the authorized logical table.
    pub metric_name: String,
    /// Complete ordered physical primary-key identity from the authorized table.
    pub primary_key: Vec<RawSeriesReadColumn>,
    /// Physical time-index identity from the authorized table.
    pub time_index: RawSeriesReadColumn,
    /// Logical primary-key tags in output order.
    pub tags: Vec<RawSeriesReadColumn>,
    /// Requested physical fields in output order.
    pub fields: Vec<RawSeriesReadColumn>,
}

/// FE-authorized physical-column identity carried to the region scanner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSeriesReadColumn {
    /// Physical region column id.
    pub column_id: ColumnId,
    /// Name resolved from the authorized logical schema.
    pub name: String,
    /// Expected physical datatype and nullability from the authorized schema.
    pub data_type: ConcreteDataType,
    /// Whether the logical column permits nulls.
    pub nullable: bool,
}

impl RawSeriesRead {
    /// Reserved QueryContext extension key and encoded-control bound.
    pub const EXTENSION_KEY: &'static str = "greptime.internal.raw_series_read.v1";
    pub const MAX_BYTES: usize = 16 * 1024;

    /// Validates this control against metadata opened from the actual scanner.
    pub fn bind(
        &self,
        actual: store_api::metadata::RegionMetadataRef,
    ) -> Result<RawLabelSource, String> {
        if actual.primary_key_encoding != store_api::codec::PrimaryKeyEncoding::Dense {
            return Err("raw-series reads require dense primary-key encoding".into());
        }
        if self.version != 1
            || self.region_id != actual.region_id
            || self.metric_name.is_empty()
            || self.fields.is_empty()
        {
            return Err("invalid raw-series binding identity".into());
        }
        let mut primary_key_seen = std::collections::HashSet::new();
        let mut tag_field_seen = std::collections::HashSet::new();
        let primary_key_ids = self
            .primary_key
            .iter()
            .map(|column| column.column_id)
            .collect::<std::collections::HashSet<_>>();
        let mut tags = Vec::with_capacity(self.tags.len());
        if actual.primary_key.as_slice()
            != self
                .primary_key
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>()
        {
            return Err("raw-series primary-key identity mismatch".into());
        }
        for key in &self.primary_key {
            let column = actual
                .column_by_id(key.column_id)
                .ok_or("raw-series primary-key column missing")?;
            if column.column_schema.name != key.name
                || column.column_schema.data_type != key.data_type
                || column.column_schema.is_nullable() != key.nullable
                || !primary_key_seen.insert(key.column_id)
            {
                return Err("raw-series primary-key binding mismatch".into());
            }
        }
        let actual_time = actual.time_index_column();
        if actual_time.column_id != self.time_index.column_id
            || actual_time.column_schema.name != self.time_index.name
            || actual_time.column_schema.data_type != self.time_index.data_type
            || actual_time.column_schema.is_nullable() != self.time_index.nullable
        {
            return Err("raw-series time-index binding mismatch".into());
        }
        for tag in &self.tags {
            let column = actual
                .column_by_id(tag.column_id)
                .ok_or("raw-series tag column missing")?;
            if column.column_schema.name != tag.name
                || column.column_schema.data_type != tag.data_type
                || column.column_schema.is_nullable() != tag.nullable
                || !primary_key_ids.contains(&tag.column_id)
                || column.semantic_type != api::v1::SemanticType::Tag
                || !tag_field_seen.insert(tag.column_id)
            {
                return Err("raw-series tag binding mismatch".into());
            }
            tags.push(store_api::region_engine::BoundTag {
                column_id: tag.column_id,
                name: tag.name.as_str().into(),
            });
        }
        for field in &self.fields {
            let column = actual
                .column_by_id(field.column_id)
                .ok_or("raw-series field column missing")?;
            if column.column_schema.name != field.name
                || column.column_schema.data_type != field.data_type
                || column.column_schema.is_nullable() != field.nullable
                || column.semantic_type != api::v1::SemanticType::Field
                || !tag_field_seen.insert(field.column_id)
            {
                return Err("raw-series field binding mismatch".into());
            }
        }
        Ok(RawLabelSource::PrimaryKey {
            metric_name: self.metric_name.as_str().into(),
            physical_metadata: actual,
            logical_tags: tags.into(),
        })
    }

    /// Encodes this control within the internal size limit.
    pub fn encode(&self) -> Result<String, serde_json::Error> {
        let encoded = serde_json::to_string(self)?;
        if encoded.len() > Self::MAX_BYTES {
            return Err(<serde_json::Error as serde::ser::Error>::custom(
                "raw-series control exceeds size limit",
            ));
        }
        Ok(encoded)
    }

    /// Decodes an optional reserved extension, rejecting malformed or unsupported controls.
    pub fn decode(
        extensions: &std::collections::HashMap<String, String>,
    ) -> Result<Option<Self>, serde_json::Error> {
        let Some(encoded) = extensions.get(Self::EXTENSION_KEY) else {
            return Ok(None);
        };
        if encoded.len() > Self::MAX_BYTES {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "raw-series control exceeds size limit",
            ));
        }
        let request: Self = serde_json::from_str(encoded)?;
        if request.version != 1 || request.metric_name.is_empty() || request.fields.is_empty() {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "invalid raw-series control",
            ));
        }
        Ok(Some(request))
    }
}

#[cfg(test)]
mod tests {
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    fn fixture() -> (store_api::metadata::RegionMetadataRef, RawSeriesRead) {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "host",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: api::v1::SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "rack",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: api::v1::SemanticType::Tag,
                column_id: 4,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "value",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: api::v1::SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: api::v1::SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1, 4]);
        let metadata = std::sync::Arc::new(builder.build().unwrap());
        let column = |column_id, name: &str, data_type| RawSeriesReadColumn {
            column_id,
            name: name.into(),
            data_type,
            nullable: false,
        };
        let read = RawSeriesRead {
            version: 1,
            region_id: RegionId::new(1, 2),
            metric_name: "cpu".into(),
            primary_key: vec![
                column(1, "host", ConcreteDataType::string_datatype()),
                column(4, "rack", ConcreteDataType::string_datatype()),
            ],
            time_index: column(3, "ts", ConcreteDataType::timestamp_millisecond_datatype()),
            tags: vec![
                column(1, "host", ConcreteDataType::string_datatype()),
                column(4, "rack", ConcreteDataType::string_datatype()),
            ],
            fields: vec![column(2, "value", ConcreteDataType::int64_datatype())],
        };
        (metadata, read)
    }

    #[test]
    fn decodes_frozen_v1_wire_fixture_and_binds_to_metadata() {
        let (metadata, expected) = fixture();
        let encoded = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../tests/compatibility/wire/raw_series_read/v1.json"
        ));
        let mut extensions = std::collections::HashMap::new();
        extensions.insert(RawSeriesRead::EXTENSION_KEY.into(), encoded.into());

        let decoded = RawSeriesRead::decode(&extensions).unwrap().unwrap();
        assert_eq!(decoded, expected);
        assert!(decoded.bind(metadata).is_ok());
    }

    #[test]
    fn binds_tag_that_is_also_primary_key() {
        let (metadata, read) = fixture();
        assert!(read.bind(metadata).is_ok());
    }

    #[test]
    fn binds_and_decodes_tagless_read() {
        let (metadata, mut read) = fixture();
        let mut builder = RegionMetadataBuilder::from_existing((*metadata).clone());
        builder.primary_key(vec![]);
        let metadata = std::sync::Arc::new(builder.build().unwrap());
        read.primary_key.clear();
        read.tags.clear();

        assert!(read.bind(metadata).is_ok());
        let mut extensions = std::collections::HashMap::new();
        extensions.insert(RawSeriesRead::EXTENSION_KEY.into(), read.encode().unwrap());
        assert_eq!(RawSeriesRead::decode(&extensions).unwrap(), Some(read));
    }

    #[test]
    fn rejects_duplicate_field_tag_and_wrong_identity() {
        let (metadata, mut read) = fixture();
        read.fields[0] = read.tags[0].clone();
        assert!(read.bind(metadata.clone()).is_err());
        let mut read = read;
        read.fields[0] = RawSeriesReadColumn {
            column_id: 2,
            name: "wrong".into(),
            data_type: ConcreteDataType::int64_datatype(),
            nullable: false,
        };
        assert!(read.bind(metadata).is_err());
    }

    #[test]
    fn rejects_primary_key_order_missing_names_types_nullability_and_wrong_time() {
        let (metadata, read) = fixture();
        let mut bad = read.clone();
        bad.primary_key.reverse();
        assert!(bad.bind(metadata.clone()).is_err());
        let mut bad = read.clone();
        bad.primary_key[0].name = "missing".into();
        assert!(bad.bind(metadata.clone()).is_err());
        let mut bad = read.clone();
        bad.primary_key[0].data_type = ConcreteDataType::int64_datatype();
        assert!(bad.bind(metadata.clone()).is_err());
        let mut bad = read.clone();
        bad.primary_key[0].nullable = true;
        assert!(bad.bind(metadata.clone()).is_err());
        let mut bad = read.clone();
        bad.time_index.column_id = 2;
        assert!(bad.bind(metadata.clone()).is_err());
        let mut bad = read.clone();
        bad.tags[0].name = "missing".into();
        assert!(bad.bind(metadata.clone()).is_err());
        let mut bad = read.clone();
        bad.tags.push(bad.tags[0].clone());
        assert!(bad.bind(metadata.clone()).is_err());
        let mut bad = read;
        bad.primary_key.push(bad.primary_key[0].clone());
        assert!(bad.bind(metadata).is_err());
    }

    #[test]
    fn rejects_sparse_primary_key_encoding() {
        let (metadata, read) = fixture();
        let mut builder = RegionMetadataBuilder::from_existing((*metadata).clone());
        builder.primary_key_encoding(store_api::codec::PrimaryKeyEncoding::Sparse);
        assert!(
            read.bind(std::sync::Arc::new(builder.build().unwrap()))
                .is_err()
        );
    }

    #[test]
    fn codec_rejects_invalid_extensions() {
        let (_, read) = fixture();
        let mut extensions = std::collections::HashMap::new();
        assert!(RawSeriesRead::decode(&extensions).unwrap().is_none());
        extensions.insert(RawSeriesRead::EXTENSION_KEY.into(), "{".into());
        assert!(RawSeriesRead::decode(&extensions).is_err());
        extensions.insert(RawSeriesRead::EXTENSION_KEY.into(), "{}".into());
        assert!(RawSeriesRead::decode(&extensions).is_err());
        let mut unsupported_version = serde_json::to_value(&read).unwrap();
        unsupported_version["version"] = serde_json::json!(2);
        extensions.insert(
            RawSeriesRead::EXTENSION_KEY.into(),
            unsupported_version.to_string(),
        );
        assert!(RawSeriesRead::decode(&extensions).is_err());
        let mut unknown = serde_json::to_value(read).unwrap();
        unknown["unexpected"] = serde_json::json!(true);
        extensions.insert(RawSeriesRead::EXTENSION_KEY.into(), unknown.to_string());
        assert!(RawSeriesRead::decode(&extensions).is_err());
        extensions.insert(
            RawSeriesRead::EXTENSION_KEY.into(),
            "x".repeat(RawSeriesRead::MAX_BYTES + 1),
        );
        assert!(RawSeriesRead::decode(&extensions).is_err());
    }
}
