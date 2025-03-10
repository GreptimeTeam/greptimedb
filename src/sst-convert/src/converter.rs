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

use std::sync::Arc;

use common_telemetry::info;
use datanode::config::StorageConfig;
use datanode::datanode::DatanodeBuilder;
use meta_client::MetaClientOptions;
use mito2::access_layer::SstInfoArray;
use mito2::config::MitoConfig;
use mito2::read::Source;
use mito2::sst::parquet::WriteOptions;
use object_store::manager::ObjectStoreManagerRef;
use object_store::services::Fs;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{DatanodeSnafu, MitoSnafu, ObjectStoreSnafu, Result};
use crate::reader::InputReaderBuilder;
use crate::table::TableMetadataHelper;
use crate::writer::RegionWriterBuilder;

/// Input file type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputFileType {
    /// File type is Parquet.
    Parquet,
    /// File type is remote write JSON.
    RemoteWrite,
}

/// Description of a file to convert.
#[derive(Debug, Clone)]
pub struct InputFile {
    /// Catalog of the table.
    pub catalog: String,
    /// Schema of the table.
    pub schema: String,
    /// Table to write.
    /// For metric engine, it needs to be the physical table name.
    pub table: String,
    /// Path to the file.
    pub path: String,
    /// Type of the input file.
    pub file_type: InputFileType,
}

/// Description of converted files for an input file.
/// A input file can be converted to multiple output files.
#[derive(Debug)]
pub struct OutputSst {
    /// Meta of output SST files.
    pub ssts: SstInfoArray,
}

/// SST converter takes a list of source files and converts them to SST files.
pub struct SstConverter {
    /// Object store for input files.
    pub input_store: ObjectStore,
    /// Output path for the converted SST files.
    /// If it is not None, the converted SST files will be written to the specified path
    /// in the `input_store`.
    /// This is for debugging purposes.
    output_path: Option<String>,
    reader_builder: InputReaderBuilder,
    writer_builder: RegionWriterBuilder,
    write_opts: WriteOptions,
}

impl SstConverter {
    /// Converts a list of input to a list of outputs.
    pub async fn convert(&mut self, input: &[InputFile]) -> Result<Vec<OutputSst>> {
        common_telemetry::info!("Converting input {} files", input.len());

        let mut outputs = Vec::with_capacity(input.len());
        let bar = indicatif::ProgressBar::new(input.len() as u64);
        for file in input {
            let output = self.convert_one(file).await?;
            outputs.push(output);
            bar.inc(1);
        }
        bar.finish();

        common_telemetry::info!("Converted {} files", outputs.len());

        Ok(outputs)
    }

    /// Converts one input.
    pub async fn convert_one(&mut self, input: &InputFile) -> Result<OutputSst> {
        common_telemetry::info!(
            "Converting input file, input_path: {}, output_path: {}",
            input.path,
            self.output_path.as_deref().unwrap_or(&input.path)
        );

        let reader_info = self.reader_builder.read_input(input).await?;
        let source = Source::Reader(reader_info.reader);
        let output_dir = self
            .output_path
            .as_deref()
            .unwrap_or(&reader_info.region_dir);
        let writer = self
            .writer_builder
            .build(reader_info.metadata, output_dir, reader_info.region_options)
            .await
            .context(MitoSnafu)?;

        let ssts = writer
            .write_sst(source, &self.write_opts)
            .await
            .context(MitoSnafu)?;
        common_telemetry::info!("Converted input file, input_path: {}", input.path);
        Ok(OutputSst { ssts })
    }
}

/// Builder to build a SST converter.
#[derive(Clone)]
pub struct SstConverterBuilder {
    input_path: String,
    meta_options: MetaClientOptions,
    storage_config: StorageConfig,
    output_path: Option<String>,
    config: MitoConfig,
}

impl SstConverterBuilder {
    /// Creates a new builder with a file system path as input.
    pub fn new_fs(input_path: String) -> Self {
        Self {
            input_path,
            meta_options: MetaClientOptions::default(),
            storage_config: StorageConfig::default(),
            output_path: None,
            config: MitoConfig::default(),
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

    /// Sets the config for the converted SST files.
    pub fn with_config(mut self, config: MitoConfig) -> Self {
        self.config = config;
        self
    }

    /// Builds a SST converter.
    pub async fn build(mut self) -> Result<SstConverter> {
        self.config
            .sanitize(&self.storage_config.data_home)
            .context(MitoSnafu)?;

        common_telemetry::info!(
            "Building SST converter, input_path: {}, storage_config: {:?}, config: {:?}",
            self.input_path,
            self.storage_config,
            self.config
        );

        let input_store = new_input_store(&self.input_path).await?;
        let output_store_manager = new_object_store_manager(&self.storage_config).await?;
        let table_helper = TableMetadataHelper::new(&self.meta_options).await?;
        let config = Arc::new(self.config);
        let reader_builder = InputReaderBuilder::new(
            input_store.clone(),
            table_helper,
            output_store_manager.clone(),
            config.clone(),
        );
        let writer_builder = RegionWriterBuilder::new(config, output_store_manager)
            .await
            .context(MitoSnafu)?;

        Ok(SstConverter {
            input_store,
            output_path: self.output_path,
            reader_builder,
            writer_builder,
            write_opts: WriteOptions::default(),
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
    let builder = Fs::default().root(path);
    info!("Creating input store, path: {}", path);
    let object_store = ObjectStore::new(builder)
        .context(ObjectStoreSnafu)?
        .finish();
    info!("Created input store: {:?}", object_store);
    Ok(object_store)
}
