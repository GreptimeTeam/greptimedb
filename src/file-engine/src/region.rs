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

use common_datasource::file_format::Format;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::{RegionCreateRequest, RegionOpenRequest};
use store_api::storage::RegionId;

use crate::error::Result;
use crate::manifest::FileRegionManifest;
use crate::FileOptions;

#[derive(Debug)]
pub struct FileRegion {
    pub(crate) region_dir: String,
    pub(crate) file_options: FileOptions,
    pub(crate) url: String,
    pub(crate) format: Format,
    pub(crate) options: HashMap<String, String>,
    pub(crate) metadata: RegionMetadataRef,
}

pub type FileRegionRef = Arc<FileRegion>;

impl FileRegion {
    pub async fn create(
        region_id: RegionId,
        request: RegionCreateRequest,
        object_store: &ObjectStore,
    ) -> Result<FileRegionRef> {
        let manifest = FileRegionManifest {
            region_id,
            column_metadatas: request.column_metadatas.clone(),
            primary_key: request.primary_key.clone(),
            options: request.options,
        };

        let region_dir = request.region_dir;
        let url = manifest.url()?;
        let file_options = manifest.file_options()?;
        let format = manifest.format()?;
        let options = manifest.options.clone();
        let metadata = manifest.metadata()?;

        manifest.store(&region_dir, object_store).await?;

        Ok(Arc::new(Self {
            region_dir,
            url,
            file_options,
            format,
            options,
            metadata,
        }))
    }

    pub async fn open(
        region_id: RegionId,
        request: RegionOpenRequest,
        object_store: &ObjectStore,
    ) -> Result<FileRegionRef> {
        let manifest =
            FileRegionManifest::load(region_id, &request.region_dir, object_store).await?;

        Ok(Arc::new(Self {
            region_dir: request.region_dir,
            url: manifest.url()?,
            file_options: manifest.file_options()?,
            format: manifest.format()?,
            metadata: manifest.metadata()?,
            options: manifest.options,
        }))
    }

    pub async fn drop(&self, object_store: &ObjectStore) -> Result<()> {
        FileRegionManifest::delete(self.metadata.region_id, &self.region_dir, object_store).await
    }

    pub fn metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;
    use crate::error::Error;
    use crate::test_util::{new_test_column_metadata, new_test_object_store, new_test_options};

    #[tokio::test]
    async fn test_create_region() {
        let (_dir, object_store) = new_test_object_store("test_create_region");

        let request = RegionCreateRequest {
            engine: "file".to_string(),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
            region_dir: "create_region_dir/".to_string(),
        };
        let region_id = RegionId::new(1, 0);

        let region = FileRegion::create(region_id, request.clone(), &object_store)
            .await
            .unwrap();

        assert_eq!(region.region_dir, "create_region_dir/");
        assert_eq!(region.url, "test");
        assert_eq!(region.file_options.files, vec!["1.csv"]);
        assert_matches!(region.format, Format::Csv { .. });
        assert_eq!(region.options, new_test_options());
        assert_eq!(region.metadata.region_id, region_id);
        assert_eq!(region.metadata.primary_key, vec![1]);

        assert!(object_store
            .is_exist("create_region_dir/manifest/_file_manifest")
            .await
            .unwrap());

        // Object exists, should fail
        let err = FileRegion::create(region_id, request, &object_store)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ManifestExists { .. });
    }

    #[tokio::test]
    async fn test_open_region() {
        let (_dir, object_store) = new_test_object_store("test_open_region");

        let region_dir = "open_region_dir/".to_string();
        let request = RegionCreateRequest {
            engine: "file".to_string(),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
            region_dir: region_dir.clone(),
        };
        let region_id = RegionId::new(1, 0);

        let _ = FileRegion::create(region_id, request.clone(), &object_store)
            .await
            .unwrap();

        let request = RegionOpenRequest {
            engine: "file".to_string(),
            region_dir,
            options: HashMap::default(),
            skip_wal_replay: false,
        };

        let region = FileRegion::open(region_id, request, &object_store)
            .await
            .unwrap();

        assert_eq!(region.region_dir, "open_region_dir/");
        assert_eq!(region.url, "test");
        assert_eq!(region.file_options.files, vec!["1.csv"]);
        assert_matches!(region.format, Format::Csv { .. });
        assert_eq!(region.options, new_test_options());
        assert_eq!(region.metadata.region_id, region_id);
        assert_eq!(region.metadata.primary_key, vec![1]);
    }

    #[tokio::test]
    async fn test_drop_region() {
        let (_dir, object_store) = new_test_object_store("test_drop_region");

        let region_dir = "drop_region_dir/".to_string();
        let request = RegionCreateRequest {
            engine: "file".to_string(),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
            region_dir: region_dir.clone(),
        };
        let region_id = RegionId::new(1, 0);

        let region = FileRegion::create(region_id, request.clone(), &object_store)
            .await
            .unwrap();

        assert!(object_store
            .is_exist("drop_region_dir/manifest/_file_manifest")
            .await
            .unwrap());

        FileRegion::drop(&region, &object_store).await.unwrap();
        assert!(!object_store
            .is_exist("drop_region_dir/manifest/_file_manifest")
            .await
            .unwrap());

        let request = RegionOpenRequest {
            engine: "file".to_string(),
            region_dir,
            options: HashMap::default(),
            skip_wal_replay: false,
        };
        let err = FileRegion::open(region_id, request, &object_store)
            .await
            .unwrap_err();
        assert_matches!(err, Error::LoadRegionManifest { .. });
    }
}
