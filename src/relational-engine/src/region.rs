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

use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use store_api::path_utils::region_name;
use store_api::region_request::{RegionCreateRequest, RegionOpenRequest};
use store_api::storage::RegionId;

use crate::error::{RegionExistsSnafu, Result};
use crate::manifest::RelationalRegionManifest;

#[derive(Debug)]
pub struct RelationalRegion {
    pub(crate) region_dir: String,
    pub(crate) metadata: RegionMetadataRef,
    /// Frozen connection / remote table options. Kept alongside the metadata so an
    /// idempotent `Create` can be validated against the existing definition.
    options: HashMap<String, String>,
}

pub type RelationalRegionRef = Arc<RelationalRegion>;

impl RelationalRegion {
    /// Computes the region directory under a table's data home.
    pub(crate) fn region_dir(table_dir: &str, region_id: RegionId) -> String {
        object_store::util::join_dir(
            table_dir,
            &region_name(region_id.table_id(), region_id.region_sequence()),
        )
    }

    pub async fn create(
        region_id: RegionId,
        request: RegionCreateRequest,
        object_store: &ObjectStore,
    ) -> Result<RelationalRegionRef> {
        let options = request.options.clone();
        let manifest = RelationalRegionManifest {
            region_id,
            column_metadatas: request.column_metadatas.clone(),
            primary_key: request.primary_key.clone(),
            options: request.options,
        };

        let region_dir = Self::region_dir(&request.table_dir, region_id);
        // Validate the options carry the relational metadata (connection + remote table)
        // before persisting, so a malformed region fails fast here rather than on open.
        manifest.relational_options()?;
        let metadata = manifest.metadata()?;

        manifest.store(&region_dir, object_store).await?;

        Ok(Arc::new(Self {
            region_dir,
            metadata,
            options,
        }))
    }

    pub async fn open(
        region_id: RegionId,
        request: RegionOpenRequest,
        object_store: &ObjectStore,
    ) -> Result<RelationalRegionRef> {
        let region_dir = Self::region_dir(&request.table_dir, region_id);
        Self::load(region_id, &region_dir, object_store).await
    }

    /// Loads a region from its persisted manifest. Used both by `open` and to
    /// recover a region whose manifest already exists (idempotent create).
    pub(crate) async fn load(
        region_id: RegionId,
        region_dir: &str,
        object_store: &ObjectStore,
    ) -> Result<RelationalRegionRef> {
        let manifest = RelationalRegionManifest::load(region_id, region_dir, object_store).await?;
        Self::from_manifest(region_dir, manifest)
    }

    /// Builds a region from an already-loaded manifest, validating the
    /// relational options and rebuilding the region metadata.
    pub(crate) fn from_manifest(
        region_dir: &str,
        manifest: RelationalRegionManifest,
    ) -> Result<RelationalRegionRef> {
        manifest.relational_options()?;
        let metadata = manifest.metadata()?;
        let options = manifest.options;

        Ok(Arc::new(Self {
            region_dir: region_dir.to_string(),
            metadata,
            options,
        }))
    }

    pub async fn drop(&self, object_store: &ObjectStore) -> Result<()> {
        RelationalRegionManifest::delete(self.metadata.region_id, &self.region_dir, object_store)
            .await
    }

    pub fn metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    /// Validates an idempotent `Create` against the already-open region, mirroring
    /// mito2's `check_recovered_region`. A create whose definition (region id, schema,
    /// primary key, or options) matches the existing region succeeds; a mismatched
    /// create must fail instead of silently returning success.
    pub(crate) fn check_create_request(
        &self,
        region_id: RegionId,
        request: &RegionCreateRequest,
    ) -> Result<()> {
        if self.metadata.region_id != region_id {
            return RegionExistsSnafu {
                region_id,
                reason: format!(
                    "existing metadata has different region id {}",
                    self.metadata.region_id
                ),
            }
            .fail();
        }
        if self.metadata.column_metadatas != request.column_metadatas {
            return RegionExistsSnafu {
                region_id,
                reason: "existing metadata has different schema".to_string(),
            }
            .fail();
        }
        if self.metadata.primary_key != request.primary_key {
            return RegionExistsSnafu {
                region_id,
                reason: "existing metadata has different primary key".to_string(),
            }
            .fail();
        }
        if self.options != request.options {
            return RegionExistsSnafu {
                region_id,
                reason: "existing metadata has different options".to_string(),
            }
            .fail();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::collections::HashMap;

    use store_api::region_request::{PathType, RegionCreateRequest, RegionOpenRequest};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;
    use crate::test_util::{new_test_column_metadata, new_test_object_store, new_test_options};

    #[tokio::test]
    async fn test_create_region() {
        let (_dir, object_store) = new_test_object_store("test_create_region");

        let request = RegionCreateRequest {
            engine: "relational".to_string(),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
            table_dir: "create_region_dir/".to_string(),
            path_type: PathType::Bare,
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };
        let region_id = RegionId::new(1, 0);

        let region = RelationalRegion::create(region_id, request.clone(), &object_store)
            .await
            .unwrap();

        assert_eq!(region.region_dir, "create_region_dir/1_0000000000/");
        assert_eq!(region.metadata.region_id, region_id);
        assert_eq!(region.metadata.primary_key, vec![1]);
        assert_eq!(region.metadata.column_metadatas.len(), 3);

        assert!(
            object_store
                .exists("create_region_dir/1_0000000000/manifest/_relational_manifest")
                .await
                .unwrap()
        );

        // Creating the same region twice must fail: the manifest already exists.
        let err = RelationalRegion::create(region_id, request, &object_store)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ManifestExists { .. });
    }

    #[tokio::test]
    async fn test_open_region() {
        let (_dir, object_store) = new_test_object_store("test_open_region");

        let region_dir = "open_region_dir/".to_string();
        let request = RegionCreateRequest {
            engine: "relational".to_string(),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
            table_dir: region_dir.clone(),
            path_type: PathType::Bare,
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };
        let region_id = RegionId::new(1, 0);

        RelationalRegion::create(region_id, request, &object_store)
            .await
            .unwrap();

        let request = RegionOpenRequest {
            engine: "relational".to_string(),
            table_dir: region_dir,
            path_type: PathType::Bare,
            options: HashMap::default(),
            skip_wal_replay: false,
            checkpoint: None,
            requirements: Default::default(),
        };

        let region = RelationalRegion::open(region_id, request, &object_store)
            .await
            .unwrap();

        assert_eq!(region.region_dir, "open_region_dir/1_0000000000/");
        assert_eq!(region.metadata.region_id, region_id);
        assert_eq!(region.metadata.primary_key, vec![1]);
        assert_eq!(region.metadata.column_metadatas.len(), 3);
    }

    #[tokio::test]
    async fn test_drop_region() {
        let (_dir, object_store) = new_test_object_store("test_drop_region");

        let region_dir = "drop_region_dir/".to_string();
        let request = RegionCreateRequest {
            engine: "relational".to_string(),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
            table_dir: region_dir.clone(),
            path_type: PathType::Bare,
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };
        let region_id = RegionId::new(1, 0);

        let region = RelationalRegion::create(region_id, request, &object_store)
            .await
            .unwrap();

        assert!(
            object_store
                .exists("drop_region_dir/1_0000000000/manifest/_relational_manifest")
                .await
                .unwrap()
        );

        RelationalRegion::drop(&region, &object_store)
            .await
            .unwrap();

        assert!(
            !object_store
                .exists("drop_region_dir/1_0000000000/manifest/_relational_manifest")
                .await
                .unwrap()
        );

        // Re-opening a dropped region must fail.
        let request = RegionOpenRequest {
            engine: "relational".to_string(),
            table_dir: region_dir,
            path_type: PathType::Bare,
            options: HashMap::default(),
            skip_wal_replay: false,
            checkpoint: None,
            requirements: Default::default(),
        };
        let err = RelationalRegion::open(region_id, request, &object_store)
            .await
            .unwrap_err();
        assert_matches!(err, Error::LoadRegionManifest { .. });
    }
}
