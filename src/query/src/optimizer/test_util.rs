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

//! Utils for testing the optimizer.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::SemanticType;
use async_trait::async_trait;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use datatypes::schema::ColumnSchema;
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::region_engine::{
    RegionEngine, RegionManifestInfo, RegionRole, RegionScannerRef, RegionStatistic,
    RemapManifestsRequest, RemapManifestsResponse, SetRegionRoleStateResponse,
    SettableRegionRoleState, SyncManifestResponse,
};
use store_api::region_request::RegionRequest;
use store_api::storage::{ConcreteDataType, RegionId, ScanRequest, SequenceNumber};

use crate::dummy_catalog::DummyTableProvider;

/// A mock region engine that can be used for testing the optimizer.
pub(crate) struct MetaRegionEngine {
    metadatas: HashMap<RegionId, RegionMetadataRef>,
}

impl MetaRegionEngine {
    /// Creates a engine with the given metadata.
    pub(crate) fn with_metadata(metadata: RegionMetadataRef) -> Self {
        let mut metadatas = HashMap::new();
        metadatas.insert(metadata.region_id, metadata);

        Self { metadatas }
    }
}

#[async_trait]
impl RegionEngine for MetaRegionEngine {
    fn name(&self) -> &str {
        "MetaRegionEngine"
    }

    async fn handle_request(
        &self,
        _region_id: RegionId,
        _request: RegionRequest,
    ) -> Result<RegionResponse, BoxedError> {
        unimplemented!()
    }

    async fn handle_query(
        &self,
        _region_id: RegionId,
        _request: ScanRequest,
    ) -> Result<RegionScannerRef, BoxedError> {
        unimplemented!()
    }

    async fn get_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef, BoxedError> {
        self.metadatas.get(&region_id).cloned().ok_or_else(|| {
            BoxedError::new(PlainError::new(
                "Region not found".to_string(),
                StatusCode::RegionNotFound,
            ))
        })
    }

    fn region_statistic(&self, _region_id: RegionId) -> Option<RegionStatistic> {
        None
    }

    async fn get_committed_sequence(
        &self,
        _region_id: RegionId,
    ) -> Result<SequenceNumber, BoxedError> {
        Ok(SequenceNumber::default())
    }

    async fn stop(&self) -> Result<(), BoxedError> {
        Ok(())
    }

    fn set_region_role(&self, _region_id: RegionId, _role: RegionRole) -> Result<(), BoxedError> {
        unimplemented!()
    }

    async fn set_region_role_state_gracefully(
        &self,
        _region_id: RegionId,
        _region_role_state: SettableRegionRoleState,
    ) -> Result<SetRegionRoleStateResponse, BoxedError> {
        unimplemented!()
    }

    async fn sync_region(
        &self,
        _region_id: RegionId,
        _manifest_info: RegionManifestInfo,
    ) -> Result<SyncManifestResponse, BoxedError> {
        unimplemented!()
    }

    async fn remap_manifests(
        &self,
        _request: RemapManifestsRequest,
    ) -> Result<RemapManifestsResponse, BoxedError> {
        unimplemented!()
    }

    fn role(&self, _region_id: RegionId) -> Option<RegionRole> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Mock a [DummyTableProvider] with a single region.
/// The schema is: `k0: string tag, ts: timestamp, v0: float64 field`
pub(crate) fn mock_table_provider(region_id: RegionId) -> DummyTableProvider {
    let metadata = Arc::new(mock_region_metadata(region_id));
    let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
    DummyTableProvider::new(region_id, engine, metadata)
}

/// Returns a mock region metadata.
/// The schema is: `k0: string tag, ts: timestamp, v0: float64 field`
fn mock_region_metadata(region_id: RegionId) -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(region_id);
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 3,
        })
        .primary_key(vec![1]);
    builder.build().unwrap()
}
