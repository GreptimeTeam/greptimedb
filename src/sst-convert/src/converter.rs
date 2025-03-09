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

//! SST converter.

use datanode::config::{FileConfig, StorageConfig};
use datanode::datanode::DatanodeBuilder;
use datanode::store::fs::new_fs_object_store;
use meta_client::MetaClientOptions;
use object_store::manager::ObjectStoreManagerRef;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{DatanodeSnafu, Result};

/// SST converter takes a list of source files and converts them to SST files.
pub struct SstConverter {
    /// Object store for input files
    input_store: ObjectStore,
    /// Object store manager for output files.
    output_store_manager: ObjectStoreManagerRef,
}

impl SstConverter {
    //
}

/// Builder to build a SST converter.
pub struct SstConverterBuilder {
    input_path: String,
    meta_options: MetaClientOptions,
    storage_config: StorageConfig,
}

impl SstConverterBuilder {
    /// Creates a new builder.
    pub fn new(input_path: String) -> Self {
        Self {
            input_path,
            meta_options: MetaClientOptions::default(),
            storage_config: StorageConfig::default(),
        }
    }
}

/// A hepler function to create the object store manager.
pub async fn new_object_store_manager(config: &StorageConfig) -> Result<ObjectStoreManagerRef> {
    DatanodeBuilder::build_object_store_manager(config)
        .await
        .context(DatanodeSnafu)
}

/// Creates a input store from a path.
pub async fn new_input_store(path: &str) -> Result<ObjectStore> {
    new_fs_object_store(path, &FileConfig::default())
        .await
        .context(DatanodeSnafu)
}
