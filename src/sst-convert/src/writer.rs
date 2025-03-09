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

//! Utilities for writing SST files.

use std::sync::Arc;

use mito2::access_layer::{
    AccessLayer, AccessLayerRef, OperationType, SstInfoArray, SstWriteRequest,
};
use mito2::cache::CacheManager;
use mito2::config::MitoConfig;
use mito2::error::Result;
use mito2::read::Source;
use mito2::region::options::RegionOptions;
use mito2::sst::index::intermediate::IntermediateManager;
use mito2::sst::index::puffin_manager::PuffinManagerFactory;
use mito2::sst::parquet::WriteOptions;
use object_store::manager::ObjectStoreManagerRef;
use store_api::metadata::RegionMetadataRef;

/// A writer that can create multiple SST files for a region.
pub struct RegionWriter {
    /// Mito engine config.
    config: Arc<MitoConfig>,
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Options of the region.
    region_options: RegionOptions,
    /// SST access layer.
    access_layer: AccessLayerRef,
}

impl RegionWriter {
    /// Writes data from a source to SST files.
    pub async fn write_sst(
        &self,
        source: Source,
        write_opts: &WriteOptions,
    ) -> Result<SstInfoArray> {
        let request = SstWriteRequest {
            op_type: OperationType::Flush,
            metadata: self.metadata.clone(),
            source,
            cache_manager: Arc::new(CacheManager::default()),
            storage: None,
            max_sequence: None,
            index_options: self.region_options.index_options.clone(),
            inverted_index_config: self.config.inverted_index.clone(),
            fulltext_index_config: self.config.fulltext_index.clone(),
            bloom_filter_index_config: self.config.bloom_filter_index.clone(),
        };

        self.access_layer.write_sst(request, write_opts).await
    }
}

/// Creator to create [`RegionWriter`] for different regions.
pub struct RegionWriterBuilder {
    /// Mito engine config.
    config: Arc<MitoConfig>,
    /// Object stores.
    object_store_manager: ObjectStoreManagerRef,
    puffin_manager_factory: PuffinManagerFactory,
    intermediate_manager: IntermediateManager,
}

impl RegionWriterBuilder {
    /// Create a new [`RegionContextCreator`].
    pub async fn new(
        config: Arc<MitoConfig>,
        object_store_manager: ObjectStoreManagerRef,
    ) -> Result<Self> {
        let puffin_manager_factory = PuffinManagerFactory::new(
            &config.index.aux_path,
            config.index.staging_size.as_bytes(),
            Some(config.index.write_buffer_size.as_bytes() as _),
            config.index.staging_ttl,
        )
        .await?;
        let intermediate_manager = IntermediateManager::init_fs(&config.index.aux_path)
            .await?
            .with_buffer_size(Some(config.index.write_buffer_size.as_bytes() as _));

        Ok(Self {
            config,
            object_store_manager,
            puffin_manager_factory,
            intermediate_manager,
        })
    }

    /// Builds a [`RegionWriter`] for the given region directory.
    pub async fn build(
        &self,
        metadata: RegionMetadataRef,
        region_dir: &str,
        region_options: RegionOptions,
    ) -> Result<RegionWriter> {
        let object_store = mito2::region::opener::get_object_store(
            &region_options.storage,
            &self.object_store_manager,
        )?;
        let access_layer = Arc::new(AccessLayer::new(
            region_dir,
            object_store,
            self.puffin_manager_factory.clone(),
            self.intermediate_manager.clone(),
        ));

        Ok(RegionWriter {
            config: self.config.clone(),
            metadata,
            region_options,
            access_layer,
        })
    }
}
