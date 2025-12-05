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
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::{ColumnId, RegionId};

use crate::FileOptions;
use crate::error::{
    CheckObjectSnafu, DecodeJsonSnafu, DeleteRegionManifestSnafu, EncodeJsonSnafu,
    InvalidMetadataSnafu, LoadRegionManifestSnafu, ManifestExistsSnafu, MissingRequiredFieldSnafu,
    ParseFileFormatSnafu, Result, StoreRegionManifestSnafu,
};

#[inline]
fn region_manifest_path(region_dir: &str) -> String {
    format!("{region_dir}manifest/_file_manifest")
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileRegionManifest {
    pub region_id: RegionId,
    pub column_metadatas: Vec<ColumnMetadata>,
    pub primary_key: Vec<ColumnId>,
    pub options: HashMap<String, String>,
}

impl FileRegionManifest {
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

    pub fn url(&self) -> Result<String> {
        self.get_option(table::requests::FILE_TABLE_LOCATION_KEY)
    }

    pub fn file_options(&self) -> Result<FileOptions> {
        let encoded_opts = self.get_option(table::requests::FILE_TABLE_META_KEY)?;
        serde_json::from_str(&encoded_opts).context(DecodeJsonSnafu)
    }

    pub fn format(&self) -> Result<Format> {
        Format::try_from(&self.options).context(ParseFileFormatSnafu)
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
    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;

    use super::*;

    #[test]
    fn metadata_allows_internal_column_name() {
        let manifest = FileRegionManifest {
            region_id: RegionId::new(1, 0),
            column_metadatas: vec![
                ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "__primary_key",
                        ConcreteDataType::string_datatype(),
                        false,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 1,
                },
                ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                    semantic_type: SemanticType::Timestamp,
                    column_id: 2,
                },
            ],
            primary_key: vec![1],
            options: HashMap::default(),
        };

        let metadata = manifest.metadata().unwrap();
        assert!(
            metadata
                .column_metadatas
                .iter()
                .any(|c| c.column_schema.name == "__primary_key")
        );
    }
}
