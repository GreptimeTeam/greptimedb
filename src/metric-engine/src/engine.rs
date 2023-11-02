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
use std::sync::Arc;

use api::v1::SemanticType;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_query::Output;
use common_recordbatch::SendableRecordBatchStream;
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use mito2::engine::{MitoEngine, MITO_ENGINE_NAME};
use object_store::util::join_dir;
use snafu::ResultExt;
use store_api::metadata::{ColumnMetadata, RegionMetadataRef};
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{RegionCreateRequest, RegionRequest};
use store_api::storage::{RegionGroup, RegionId, ScanRequest};

use crate::error::{CreateMitoRegionSnafu, Result};
use crate::utils;

/// region group value for data region inside a metric region
pub const METRIC_DATA_REGION_GROUP: RegionGroup = 0;

/// region group value for metadata region inside a metric region
pub const METRIC_METADATA_REGION_GROUP: RegionGroup = 1;

const METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME: &str = "ts";
const METADATA_SCHEMA_KEY_COLUMN_NAME: &str = "key";
const METADATA_SCHEMA_VALUE_COLUMN_NAME: &str = "val";

const METADATA_REGION_SUBDIR: &str = "metadata";
const DATA_REGION_SUBDIR: &str = "data";

pub struct MetricEngine {
    inner: Arc<MetricEngineInner>,
}

#[async_trait]
impl RegionEngine for MetricEngine {
    /// Name of this engine
    fn name(&self) -> &str {
        "metric"
    }

    /// Handles request to the region.
    ///
    /// Only query is not included, which is handled in `handle_query`
    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> std::result::Result<Output, BoxedError> {
        match request {
            RegionRequest::Put(_) => todo!(),
            RegionRequest::Delete(_) => todo!(),
            RegionRequest::Create(create) => todo!(),
            RegionRequest::Drop(_) => todo!(),
            RegionRequest::Open(_) => todo!(),
            RegionRequest::Close(_) => todo!(),
            RegionRequest::Alter(_) => todo!(),
            RegionRequest::Flush(_) => todo!(),
            RegionRequest::Compact(_) => todo!(),
            RegionRequest::Truncate(_) => todo!(),
        }
        todo!()
    }

    /// Handles substrait query and return a stream of record batches
    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        todo!()
    }

    /// Retrieves region's metadata.
    async fn get_metadata(
        &self,
        region_id: RegionId,
    ) -> std::result::Result<RegionMetadataRef, BoxedError> {
        todo!()
    }

    /// Retrieves region's disk usage.
    async fn region_disk_usage(&self, region_id: RegionId) -> Option<i64> {
        todo!()
    }

    /// Stops the engine
    async fn stop(&self) -> std::result::Result<(), BoxedError> {
        todo!()
    }

    fn set_writable(
        &self,
        region_id: RegionId,
        writable: bool,
    ) -> std::result::Result<(), BoxedError> {
        todo!()
    }

    fn role(&self, region_id: RegionId) -> Option<RegionRole> {
        todo!()
    }
}

struct MetricEngineInner {
    mito: MitoEngine,
}

impl MetricEngineInner {
    pub async fn create_region(
        &self,
        region_id: RegionId,
        request: RegionCreateRequest,
    ) -> Result<()> {
        self.verify_region_create_request(&request)?;

        let (data_region_id, metadata_region_id) = Self::transform_region_id(region_id);
        let create_data_region_request = self.create_request_for_data_region(&request);
        let create_metadata_region_request =
            self.create_request_for_metadata_region(&request.region_dir);

        self.mito
            .handle_request(
                data_region_id,
                RegionRequest::Create(create_data_region_request),
            )
            .await
            .with_context(|_| CreateMitoRegionSnafu {
                region_type: DATA_REGION_SUBDIR,
            })?;
        self.mito
            .handle_request(
                metadata_region_id,
                RegionRequest::Create(create_metadata_region_request),
            )
            .await
            .with_context(|_| CreateMitoRegionSnafu {
                region_type: METADATA_REGION_SUBDIR,
            })?;

        Ok(())
    }

    /// Check if
    /// - internal columns are present
    fn verify_region_create_request(&self, request: &RegionCreateRequest) -> Result<()> {
        let name_to_index = request
            .column_metadatas
            .iter()
            .enumerate()
            .map(|(idx, metadata)| (metadata.column_schema.name.clone(), idx))
            .collect::<HashMap<String, usize>>();

        Ok(())
    }

    /// Build data region id and metadata region id from the given region id.
    ///
    /// Return value: (data_region_id, metadata_region_id)
    fn transform_region_id(region_id: RegionId) -> (RegionId, RegionId) {
        (
            utils::to_data_region_id(region_id),
            utils::to_metadata_region_id(region_id),
        )
    }

    /// Build [RegionCreateRequest] for metadata region
    ///
    /// This method will append [METADATA_REGION_SUBDIR] to the given `region_dir`.
    pub fn create_request_for_metadata_region(&self, region_dir: &str) -> RegionCreateRequest {
        // ts TIME INDEX DEFAULT 0
        let timestamp_column_metadata = ColumnMetadata {
            column_id: 0,
            semantic_type: SemanticType::Timestamp,
            column_schema: ColumnSchema::new(
                METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME,
                ConcreteDataType::time_millisecond_datatype(),
                false,
            )
            .with_default_constraint(Some(datatypes::schema::ColumnDefaultConstraint::Value(
                Value::Timestamp(Timestamp::new_millisecond(0)),
            )))
            .unwrap(),
        };
        // key STRING PRIMARY KEY
        let key_column_metadata = ColumnMetadata {
            column_id: 1,
            semantic_type: SemanticType::Tag,
            column_schema: ColumnSchema::new(
                METADATA_SCHEMA_KEY_COLUMN_NAME,
                ConcreteDataType::string_datatype(),
                false,
            ),
        };
        // val STRING
        let value_column_metadata = ColumnMetadata {
            column_id: 2,
            semantic_type: SemanticType::Field,
            column_schema: ColumnSchema::new(
                METADATA_SCHEMA_VALUE_COLUMN_NAME,
                ConcreteDataType::string_datatype(),
                true,
            ),
        };

        let metadata_region_dir = join_dir(region_dir, METADATA_REGION_SUBDIR);

        RegionCreateRequest {
            engine: MITO_ENGINE_NAME.to_string(),
            column_metadatas: vec![
                timestamp_column_metadata,
                key_column_metadata,
                value_column_metadata,
            ],
            primary_key: vec![1],
            options: HashMap::new(),
            region_dir: metadata_region_dir,
        }
    }

    // todo: register "tag columns" to metadata
    pub fn create_request_for_data_region(
        &self,
        request: &RegionCreateRequest,
    ) -> RegionCreateRequest {
        let mut data_region_request = request.clone();

        // concat region dir
        data_region_request.region_dir = join_dir(&request.region_dir, DATA_REGION_SUBDIR);

        // todo: change semantic type and primary key

        // todo: add internal column

        data_region_request
    }
}
