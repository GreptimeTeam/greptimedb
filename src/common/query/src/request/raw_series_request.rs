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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::SemanticType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use serde::{Deserialize, Serialize};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::region_engine::{BoundTag, RawLabelSource};
use store_api::storage::ColumnId;

/// A logical label mapped to its physical region column.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSeriesTag {
    /// Physical region column identifier.
    pub column_id: ColumnId,
    /// Logical tag name.
    pub name: String,
}

/// Explicit versioned binding for a single-region raw-series request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSeriesRequest {
    /// Control format version (currently 1).
    pub version: u32,
    /// Expected physical metadata snapshot.
    pub physical_metadata: RegionMetadata,
    /// Logical metric name.
    pub metric_name: String,
    /// Logical tags in output order.
    pub logical_tags: Vec<RawSeriesTag>,
    /// Physical fields in requested output order.
    pub field_ids: Vec<ColumnId>,
}

impl RawSeriesRequest {
    /// Binds expected output fields and labels to the actual opened scanner metadata.
    pub fn bind(
        &self,
        actual: RegionMetadataRef,
    ) -> DataFusionResult<(Arc<SeriesSchema>, RawLabelSource)> {
        self.validate()?;
        if !same_encoding_identity(&self.physical_metadata, &actual) {
            return Err(DataFusionError::Plan("Raw-series metadata mismatch".into()));
        }
        let fields = self
            .field_ids
            .iter()
            .map(|id| {
                actual
                    .column_by_id(*id)
                    .filter(|column| column.semantic_type == SemanticType::Field)
                    .map(|column| FieldSpec {
                        name: Arc::from(column.column_schema.name.as_str()),
                    })
                    .ok_or_else(|| DataFusionError::Plan("Invalid raw-series fields".into()))
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let schema = Arc::new(SeriesSchema {
            fields: fields.into(),
            eval_grid: None,
        });
        let tags = self
            .logical_tags
            .iter()
            .map(|tag| BoundTag {
                column_id: tag.column_id,
                name: Arc::from(tag.name.as_str()),
            })
            .collect::<Vec<_>>();
        let source = RawLabelSource::PrimaryKey {
            metric_name: Arc::from(self.metric_name.as_str()),
            physical_metadata: actual,
            logical_tags: tags.into(),
        };
        Ok((schema, source))
    }

    fn validate(&self) -> DataFusionResult<()> {
        if self.version != 1 || self.field_ids.is_empty() {
            return Err(DataFusionError::Plan("Invalid raw-series request".into()));
        }
        let mut ids = HashSet::new();
        for id in &self.field_ids {
            let Some(column) = self.physical_metadata.column_by_id(*id) else {
                return Err(DataFusionError::Plan("Invalid raw-series fields".into()));
            };
            if column.semantic_type != SemanticType::Field || !ids.insert(*id) {
                return Err(DataFusionError::Plan("Invalid raw-series fields".into()));
            }
        }
        Ok(())
    }
}

fn same_encoding_identity(left: &RegionMetadata, right: &RegionMetadata) -> bool {
    left.region_id == right.region_id
        && left.schema_version == right.schema_version
        && left.column_metadatas == right.column_metadatas
        && left.primary_key == right.primary_key
        && left.primary_key_encoding == right.primary_key_encoding
        && left.time_index_column().column_id == right.time_index_column().column_id
        && left.time_index_type() == right.time_index_type()
}

#[cfg(test)]
mod tests {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    /// Builds the physical metadata of a one-tag, two-field region.
    fn fixture_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "host",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "value",
                    ConcreteDataType::int32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("aux", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 4,
            })
            .primary_key(vec![1]);
        builder.build().unwrap()
    }

    /// Builds a request asking for fields in descending column order.
    fn fixture_request() -> RawSeriesRequest {
        RawSeriesRequest {
            version: 1,
            physical_metadata: fixture_metadata(),
            metric_name: "cpu".to_string(),
            logical_tags: vec![RawSeriesTag {
                column_id: 1,
                name: "host".to_string(),
            }],
            field_ids: vec![3, 2],
        }
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut request = fixture_request();
        request.version = 2;
        assert!(request.validate().is_err());
    }

    #[test]
    fn rejects_empty_duplicate_and_non_field_ids() {
        let mut request = fixture_request();
        request.field_ids.clear();
        assert!(request.validate().is_err());
        let mut request = fixture_request();
        request.field_ids.push(3);
        assert!(request.validate().is_err());
        let mut request = fixture_request();
        request.field_ids = vec![1];
        assert!(request.validate().is_err());
    }

    #[test]
    fn binds_schema_order_and_actual_metadata_arc() {
        let request = fixture_request();
        let mut reordered = request.clone();
        reordered.field_ids.reverse();
        let actual = Arc::new(reordered.physical_metadata.clone());
        let (schema, source) = reordered.bind(actual.clone()).unwrap();
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .collect::<Vec<_>>(),
            ["value", "aux"]
        );
        let RawLabelSource::PrimaryKey {
            physical_metadata, ..
        } = source
        else {
            panic!("expected key labels")
        };
        assert!(Arc::ptr_eq(&actual, &physical_metadata));
        let mut mismatch = request.clone();
        mismatch.physical_metadata.schema_version += 1;
        assert!(mismatch.bind(actual.clone()).is_err());
        let mut mismatch = request.clone();
        mismatch.physical_metadata.region_id = RegionId::new(1, 2);
        assert!(mismatch.bind(actual.clone()).is_err());
        let mut mismatch = request.clone();
        mismatch.physical_metadata.primary_key.push(2);
        assert!(mismatch.bind(actual.clone()).is_err());
        let mut mismatch = request.clone();
        mismatch.physical_metadata.column_metadatas.swap(0, 1);
        assert!(mismatch.bind(actual.clone()).is_err());
        let mut mismatch = request.clone();
        mismatch.physical_metadata.column_metadatas[1].column_schema =
            ColumnSchema::new("value", ConcreteDataType::int64_datatype(), false);
        assert!(mismatch.bind(actual.clone()).is_err());
        let mut partition_only = request;
        partition_only
            .physical_metadata
            .set_partition_expr(Some("ignored".into()));
        assert!(partition_only.bind(actual).is_ok());
    }
}
