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

//! Utilities for testing.

use api::v1::SemanticType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use mito2::test_util::TestEnv as MitoTestEnv;
use object_store::util::join_dir;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCreateRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::data_region::DataRegion;
use crate::engine::{MetricEngine, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY};
use crate::metadata_region::MetadataRegion;

/// Env to test metric engine.
pub struct TestEnv {
    mito_env: MitoTestEnv,
    mito: MitoEngine,
}

impl TestEnv {
    /// Returns a new env with empty prefix for test.
    pub async fn new() -> Self {
        Self::with_prefix("").await
    }

    /// Returns a new env with specific `prefix` for test.
    pub async fn with_prefix(prefix: &str) -> Self {
        let mut mito_env = MitoTestEnv::with_prefix(prefix);
        let mito = mito_env.create_engine(MitoConfig::default()).await;
        Self { mito_env, mito }
    }

    pub fn data_home(&self) -> String {
        let env_root = self.mito_env.data_home().to_string_lossy().to_string();
        join_dir(&env_root, "data")
    }

    /// Returns a reference to the engine.
    pub fn mito(&self) -> MitoEngine {
        self.mito.clone()
    }

    pub fn metric(&self) -> MetricEngine {
        MetricEngine::new(self.mito())
    }

    /// Create regions in [MetricEngine] under [`default_region_id`](TestEnv::default_region_id)
    /// and region dir `"test_metric_region"`.
    pub async fn init_metric_region(&self) {
        let region_id = self.default_region_id();
        let region_create_request = RegionCreateRequest {
            engine: METRIC_ENGINE_NAME.to_string(),
            column_metadatas: vec![ColumnMetadata {
                column_id: 0,
                semantic_type: SemanticType::Timestamp,
                column_schema: ColumnSchema::new(
                    "greptime_timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
            }],
            primary_key: vec![],
            options: [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
                .into_iter()
                .collect(),
            region_dir: "test_metric_region".to_string(),
        };

        // create regions
        self.metric()
            .handle_request(region_id, RegionRequest::Create(region_create_request))
            .await
            .unwrap();
    }

    pub fn metadata_region(&self) -> MetadataRegion {
        MetadataRegion::new(self.mito())
    }

    pub fn data_region(&self) -> DataRegion {
        DataRegion::new(self.mito())
    }

    /// `RegionId::new(1, 2)`
    pub fn default_region_id(&self) -> RegionId {
        RegionId::new(1, 2)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::engine::{DATA_REGION_SUBDIR, METADATA_REGION_SUBDIR};
    use crate::utils::{self, to_metadata_region_id};

    #[tokio::test]
    async fn create_metadata_region() {
        common_telemetry::init_default_ut_logging();

        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let region_id = to_metadata_region_id(env.default_region_id());
        let region_dir = join_dir(&env.data_home(), "test_metric_region");

        // `join_dir` doesn't suit windows path
        #[cfg(not(target_os = "windows"))]
        {
            // assert metadata region's dir
            let metadata_region_dir = join_dir(&region_dir, METADATA_REGION_SUBDIR);
            let exist = tokio::fs::try_exists(metadata_region_dir).await.unwrap();
            assert!(exist);

            // assert data region's dir
            let data_region_dir = join_dir(&region_dir, DATA_REGION_SUBDIR);
            let exist = tokio::fs::try_exists(data_region_dir).await.unwrap();
            assert!(exist);
        }

        // check mito engine
        let metadata_region_id = utils::to_metadata_region_id(region_id);
        let _ = env.mito().get_metadata(metadata_region_id).await.unwrap();
        let data_region_id = utils::to_data_region_id(region_id);
        let _ = env.mito().get_metadata(data_region_id).await.unwrap();
    }
}
