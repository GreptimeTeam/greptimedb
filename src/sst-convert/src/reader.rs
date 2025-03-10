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

//! Reader to read input data in different formats.

use std::collections::HashMap;
use std::sync::Arc;

use mito2::config::MitoConfig;
use mito2::read::BoxedBatchReader;
use mito2::region::opener::RegionMetadataLoader;
use mito2::region::options::RegionOptions;
use object_store::manager::ObjectStoreManagerRef;
use object_store::util::join_dir;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_REGION_SUBDIR;
use store_api::path_utils::region_dir;
use store_api::storage::{RegionId, SequenceNumber};
use table::metadata::TableId;

use crate::converter::{InputFile, InputFileType};
use crate::error::{MissingTableSnafu, MitoSnafu, Result};
use crate::reader::remote_write::RemoteWriteReader;
use crate::table::TableMetadataHelper;
use crate::OpenDALParquetReader;

pub(crate) mod parquet;
pub mod remote_write;

/// Reader and context.
pub struct ReaderInfo {
    pub reader: BoxedBatchReader,
    pub region_dir: String,
    pub region_options: RegionOptions,
    pub metadata: RegionMetadataRef,
}

/// Builder to build readers to read input files.
pub(crate) struct InputReaderBuilder {
    input_store: ObjectStore,
    table_helper: TableMetadataHelper,
    region_loader: RegionMetadataLoader,
    /// Cached region infos for tables.
    region_infos: HashMap<String, RegionInfo>,
}

impl InputReaderBuilder {
    pub(crate) fn new(
        input_store: ObjectStore,
        table_helper: TableMetadataHelper,
        object_store_manager: ObjectStoreManagerRef,
        config: Arc<MitoConfig>,
    ) -> Self {
        let region_loader = RegionMetadataLoader::new(config, object_store_manager);

        Self {
            input_store,
            table_helper,
            region_loader,
            region_infos: HashMap::new(),
        }
    }

    /// Builds a reader to read the input file.
    pub async fn read_input(&mut self, input: &InputFile) -> Result<ReaderInfo> {
        match input.file_type {
            InputFileType::Parquet => self.read_parquet(input).await,
            InputFileType::RemoteWrite => self.read_remote_write(input).await,
        }
    }

    /// Builds a reader to read the parquet file.
    pub async fn read_parquet(&mut self, input: &InputFile) -> Result<ReaderInfo> {
        let region_info = self.get_region_info(input).await?;
        let reader = OpenDALParquetReader::new(
            self.input_store.clone(),
            &input.path,
            region_info.metadata.clone(),
            Some(region_info.flushed_sequence),
        )
        .await?;

        Ok(ReaderInfo {
            reader: Box::new(reader),
            region_dir: region_info.region_dir,
            region_options: region_info.region_options,
            metadata: region_info.metadata,
        })
    }

    /// Builds a reader to read the remote write file.
    pub async fn read_remote_write(&mut self, input: &InputFile) -> Result<ReaderInfo> {
        let region_info = self.get_region_info(input).await?;
        // TODO(yingwen): Should update the sequence.
        let reader = RemoteWriteReader::open(
            self.input_store.clone(),
            &input.path,
            &input.catalog,
            &input.schema,
            region_info.metadata.clone(),
            self.table_helper.clone(),
        )
        .await?
        .with_sequence(region_info.flushed_sequence);

        Ok(ReaderInfo {
            reader: Box::new(reader),
            region_dir: region_info.region_dir,
            region_options: region_info.region_options,
            metadata: region_info.metadata,
        })
    }

    async fn get_region_info(&mut self, input: &InputFile) -> Result<RegionInfo> {
        let cache_key = cache_key(&input.catalog, &input.schema, &input.table);
        if let Some(region_info) = self.region_infos.get(&cache_key) {
            return Ok(region_info.clone());
        }

        let table_info = self
            .table_helper
            .get_table(&input.catalog, &input.schema, &input.table)
            .await?
            .context(MissingTableSnafu {
                table_name: &input.table,
            })?;
        let region_id = to_region_id(table_info.table_info.ident.table_id);
        let opts = table_info.table_info.to_region_options();
        // TODO(yingwen): We ignore WAL options now. We should `prepare_wal_options()` in the future.
        let region_options = RegionOptions::try_from(&opts).context(MitoSnafu)?;
        let mut region_dir = region_dir(&table_info.region_storage_path(), region_id);
        if input.file_type == InputFileType::RemoteWrite {
            // metric engine has two internal regions.
            region_dir = join_dir(&region_dir, DATA_REGION_SUBDIR);
        }
        let manifest = self
            .region_loader
            .load_manifest(&region_dir, &region_options)
            .await
            .context(MitoSnafu)?
            .context(MissingTableSnafu {
                table_name: &table_info.table_info.name,
            })?;
        let region_info = RegionInfo {
            metadata: manifest.metadata.clone(),
            flushed_sequence: manifest.flushed_sequence,
            region_dir,
            region_options,
        };
        self.region_infos.insert(cache_key, region_info.clone());

        Ok(region_info)
    }
}

fn to_region_id(table_id: TableId) -> RegionId {
    RegionId::new(table_id, 0)
}

fn cache_key(catalog: &str, schema: &str, table: &str) -> String {
    format!("{}/{}/{}", catalog, schema, table)
}

#[derive(Clone)]
struct RegionInfo {
    metadata: RegionMetadataRef,
    flushed_sequence: SequenceNumber,
    region_dir: String,
    region_options: RegionOptions,
}
