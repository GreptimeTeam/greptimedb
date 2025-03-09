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
use mito2::access_layer::SstInfoArray;
use object_store::manager::ObjectStoreManagerRef;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{DatanodeSnafu, Result};
use crate::table::TableMetadataHelper;

/// Input file type.
pub enum InputFileType {
    /// File type is Parquet.
    Parquet,
    /// File type is remote write JSON.
    RemoteWrite,
}

/// Description of a file to convert.
pub struct InputFile {
    /// Table to write.
    /// For metric engine, it needs to be the physical table name.
    pub table_name: String,
    /// Path to the file.
    pub path: String,
    /// Type of the input file.
    pub file_type: InputFileType,
}

/// Description of converted files for an input file.
/// A input file can be converted to multiple output files.
pub struct OutputSst {
    /// Meta of output SST files.
    pub ssts: SstInfoArray,
}

/// SST converter takes a list of source files and converts them to SST files.
pub struct SstConverter {
    /// Object store for input files.
    input_store: ObjectStore,
    /// Object store manager for output files.
    output_store_manager: ObjectStoreManagerRef,
    /// Helper to get table meta.
    table_helper: TableMetadataHelper,
    /// Output path for the converted SST files.
    /// If it is not None, the converted SST files will be written to the specified path
    /// in the `input_store`.
    /// This is for debugging purposes.
    output_path: Option<String>,
}

impl SstConverter {
    /// Converts a list of input to a list of outputs.
    pub async fn convert(&self, input: &[InputFile]) -> Result<Vec<OutputSst>> {
        todo!()
    }
}

/// Builder to build a SST converter.
pub struct SstConverterBuilder {
    input_path: String,
    meta_options: MetaClientOptions,
    storage_config: StorageConfig,
    output_path: Option<String>,
}

impl SstConverterBuilder {
    /// Creates a new builder with a file system path as input.
    pub fn new_fs(input_path: String) -> Self {
        Self {
            input_path,
            meta_options: MetaClientOptions::default(),
            storage_config: StorageConfig::default(),
            output_path: None,
        }
    }

    /// Attaches the meta client options.
    pub fn with_meta_options(mut self, meta_options: MetaClientOptions) -> Self {
        self.meta_options = meta_options;
        self
    }

    /// Attaches the storage config.
    pub fn with_storage_config(mut self, storage_config: StorageConfig) -> Self {
        self.storage_config = storage_config;
        self
    }

    /// Sets the output path for the converted SST files.
    /// This is for debugging purposes.
    pub fn with_output_path(mut self, output_path: String) -> Self {
        self.output_path = Some(output_path);
        self
    }

    /// Builds a SST converter.
    pub async fn build(self) -> Result<SstConverter> {
        let input_store = new_input_store(&self.input_path).await?;
        let output_store_manager = new_object_store_manager(&self.storage_config).await?;
        let table_helper = TableMetadataHelper::new(&self.meta_options).await?;

        Ok(SstConverter {
            input_store,
            output_store_manager,
            table_helper,
            output_path: self.output_path,
        })
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
