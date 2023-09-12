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
use common_datasource::object_store::build_backend;
use common_recordbatch::SendableRecordBatchStream;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::{RegionCreateRequest, RegionOpenRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::error::{BuildBackendSnafu, Result};
use crate::manifest::FileRegionManifest;
use crate::stream::{create_stream, CreateScanPlanContext, ScanPlanConfig};

#[derive(Debug)]
pub struct FileRegion {
    region_dir: String,
    files: Vec<String>,
    url: String,
    format: Format,
    options: HashMap<String, String>,
    metadata: RegionMetadataRef,
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
        let files = manifest.files()?;
        let format = manifest.format()?;
        let options = manifest.options.clone();
        let metadata = manifest.metadata()?;

        manifest.store(&region_dir, object_store).await?;

        Ok(Arc::new(Self {
            region_dir,
            url,
            files,
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
            files: manifest.files()?,
            format: manifest.format()?,
            metadata: manifest.metadata()?,
            options: manifest.options,
        }))
    }

    pub async fn drop(&self, object_store: &ObjectStore) -> Result<()> {
        FileRegionManifest::delete(self.metadata.region_id, &self.region_dir, object_store).await
    }

    pub fn query(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let store = build_backend(&self.url, &self.options).context(BuildBackendSnafu)?;
        create_stream(
            &self.format,
            &CreateScanPlanContext::default(),
            &ScanPlanConfig {
                file_schema: self.metadata.schema.clone(),
                files: &self.files,
                projection: request.projection.as_ref(),
                filters: &request.filters,
                limit: request.limit,
                store,
            },
        )
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
            create_if_not_exists: true,
            options: new_test_options(),
            region_dir: "create_region_dir/".to_string(),
        };
        let region_id = RegionId::new(1, 0);

        let region = FileRegion::create(region_id, request.clone(), &object_store)
            .await
            .unwrap();

        assert_eq!(region.region_dir, "create_region_dir/");
        assert_eq!(region.url, "test");
        assert_eq!(region.files, vec!["1.csv"]);
        assert_matches!(region.format, Format::Csv { .. });
        assert_eq!(region.options, new_test_options());
        assert_eq!(region.metadata.region_id, region_id);
        assert_eq!(region.metadata.primary_key, vec![1]);

        assert!(object_store
            .is_exist("create_region_dir/manifest/_immutable_manifest")
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
            create_if_not_exists: true,
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
        };

        let region = FileRegion::open(region_id, request, &object_store)
            .await
            .unwrap();

        assert_eq!(region.region_dir, "open_region_dir/");
        assert_eq!(region.url, "test");
        assert_eq!(region.files, vec!["1.csv"]);
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
            create_if_not_exists: true,
            options: new_test_options(),
            region_dir: region_dir.clone(),
        };
        let region_id = RegionId::new(1, 0);

        let region = FileRegion::create(region_id, request.clone(), &object_store)
            .await
            .unwrap();

        assert!(object_store
            .is_exist("drop_region_dir/manifest/_immutable_manifest")
            .await
            .unwrap());

        FileRegion::drop(&region, &object_store).await.unwrap();
        assert!(!object_store
            .is_exist("drop_region_dir/manifest/_immutable_manifest")
            .await
            .unwrap());

        let request = RegionOpenRequest {
            engine: "file".to_string(),
            region_dir,
            options: HashMap::default(),
        };
        let err = FileRegion::open(region_id, request, &object_store)
            .await
            .unwrap_err();
        assert_matches!(err, Error::LoadRegionManifest { .. });
    }
}
