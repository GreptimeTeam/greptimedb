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
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::{ColumnId, RegionId};
use table::requests::{RELATIONAL_META_KEY, RelationalOptions};

use crate::error::{
    CheckObjectSnafu, DecodeJsonSnafu, DeleteRegionManifestSnafu, EncodeJsonSnafu,
    InvalidMetadataSnafu, LoadRegionManifestSnafu, ManifestExistsSnafu, MissingRequiredFieldSnafu,
    Result, StoreRegionManifestSnafu,
};

#[inline]
fn region_manifest_path(region_dir: &str) -> String {
    format!("{region_dir}manifest/_relational_manifest")
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelationalRegionManifest {
    pub region_id: RegionId,
    pub column_metadatas: Vec<ColumnMetadata>,
    pub primary_key: Vec<ColumnId>,
    pub options: HashMap<String, String>,
}

impl RelationalRegionManifest {
    pub async fn store(&self, region_dir: &str, object_store: &ObjectStore) -> Result<()> {
        let path = &region_manifest_path(region_dir);
        let exist = object_store
            .exists(path)
            .await
            .context(CheckObjectSnafu { path })?;
        ensure!(!exist, ManifestExistsSnafu { path });

        let bs = self.encode()?;
        object_store
            .write(path, bs)
            .await
            .context(StoreRegionManifestSnafu {
                region_id: self.region_id,
            })?;

        Ok(())
    }

    /// Checks whether a region manifest has already been persisted under
    /// `region_dir`. This is the source of truth for region existence and is
    /// used to make `CREATE EXTERNAL TABLE` idempotent across restarts.
    pub async fn exists(region_dir: &str, object_store: &ObjectStore) -> Result<bool> {
        let path = &region_manifest_path(region_dir);
        object_store
            .exists(path)
            .await
            .context(CheckObjectSnafu { path })
    }

    pub async fn load(
        region_id: RegionId,
        region_dir: &str,
        object_store: &ObjectStore,
    ) -> Result<Self> {
        let path = &region_manifest_path(region_dir);
        let bs = object_store
            .read(path)
            .await
            .context(LoadRegionManifestSnafu { region_id })?
            .to_vec();
        Self::decode(bs.as_slice())
    }

    pub async fn delete(
        region_id: RegionId,
        region_dir: &str,
        object_store: &ObjectStore,
    ) -> Result<()> {
        let path = &region_manifest_path(region_dir);
        object_store
            .delete(path)
            .await
            .context(DeleteRegionManifestSnafu { region_id })
    }

    pub fn metadata(&self) -> Result<RegionMetadataRef> {
        let mut builder = RegionMetadataBuilder::new(self.region_id);
        for column in &self.column_metadatas {
            builder.push_column_metadata(column.clone());
        }
        builder.primary_key(self.primary_key.clone());
        let metadata = builder
            .build_without_validation()
            .context(InvalidMetadataSnafu)?;

        Ok(Arc::new(metadata))
    }

    pub fn relational_options(&self) -> Result<RelationalOptions> {
        let encoded_opts = self.get_option(RELATIONAL_META_KEY)?;
        serde_json::from_str(&encoded_opts).context(DecodeJsonSnafu)
    }

    fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).context(EncodeJsonSnafu)
    }

    fn decode(src: &[u8]) -> Result<Self> {
        serde_json::from_slice(src).context(DecodeJsonSnafu)
    }

    fn get_option(&self, name: &str) -> Result<String> {
        self.options
            .get(name)
            .cloned()
            .context(MissingRequiredFieldSnafu { name })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;
    use crate::test_util::{new_test_column_metadata, new_test_object_store, new_test_options};

    #[tokio::test]
    async fn test_manifest_round_trip() {
        let (_dir, object_store) = new_test_object_store("test_manifest_round_trip");

        let region_id = RegionId::new(1, 0);
        let manifest = RelationalRegionManifest {
            region_id,
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
        };
        let region_dir = "manifest_dir/1_0000000000/";

        manifest.store(region_dir, &object_store).await.unwrap();

        // Options and schema are persisted verbatim and recovered on load.
        let loaded = RelationalRegionManifest::load(region_id, region_dir, &object_store)
            .await
            .unwrap();
        assert_eq!(loaded.options, manifest.options);
        assert_eq!(loaded.region_id, region_id);
        assert_eq!(loaded.primary_key, vec![1]);
        assert_eq!(loaded.column_metadatas.len(), 3);
        assert_eq!(
            loaded.relational_options().unwrap(),
            manifest.relational_options().unwrap()
        );

        // Storing the same manifest twice must fail.
        let err = manifest.store(region_dir, &object_store).await.unwrap_err();
        assert_matches!(err, Error::ManifestExists { .. });
    }

    #[tokio::test]
    async fn test_metadata_rebuild() {
        let manifest = RelationalRegionManifest {
            region_id: RegionId::new(2, 0),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
        };

        let metadata = manifest.metadata().unwrap();
        assert_eq!(metadata.region_id, RegionId::new(2, 0));
        assert_eq!(metadata.primary_key, vec![1]);
        assert_eq!(metadata.column_metadatas.len(), 3);
        assert_eq!(metadata.column_metadatas[0].column_id, 0);
        assert_eq!(metadata.column_metadatas[1].column_id, 1);
        assert_eq!(metadata.column_metadatas[2].column_id, 2);
    }
}
