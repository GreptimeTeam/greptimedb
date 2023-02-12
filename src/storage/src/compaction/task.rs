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

use std::sync::{Arc, Mutex};

use object_store::ObjectStore;
use store_api::logstore::LogStore;
use uuid::Uuid;

use crate::compaction::writer::build_sst_reader;
use crate::error::Result;
use crate::manifest::action::RegionEdit;
use crate::manifest::region::RegionManifest;
use crate::region::RegionWriterRef;
use crate::schema::RegionSchemaRef;
use crate::sst::parquet::{ParquetWriter, Source};
use crate::sst::{AccessLayerRef, FileHandle, FileMeta, Level, SstInfo, WriteOptions};
use crate::version::VersionControlRef;
use crate::wal::Wal;

#[async_trait::async_trait]
pub trait CompactionTask: Send + Sync + 'static {
    async fn run(self) -> Result<()>;
}

#[allow(unused)]
pub(crate) struct CompactionTaskImpl {
    pub schema: RegionSchemaRef,
    pub sst_layer: AccessLayerRef,
    pub outputs: Vec<CompactionOutput>,
    pub writer: RegionWriterRef,
    pub version: VersionControlRef,
    pub compacted_inputs: Arc<Mutex<Vec<FileMeta>>>,
}

impl CompactionTaskImpl {
    async fn merge_ssts(&mut self) -> Result<Vec<FileMeta>> {
        let mut futs = Vec::with_capacity(self.outputs.len());
        for output in self.outputs.drain(..) {
            let schema = self.schema.clone();
            let sst_layer = self.sst_layer.clone();
            let object_store = self.sst_layer.object_store();

            let compacted = self.compacted_inputs.clone();
            futs.push(async move {
                match output.run(schema, sst_layer, object_store).await {
                    Ok(meta) => {
                        let mut compacted = compacted.lock().unwrap();
                        compacted.extend(output.inputs.iter().map(|f| FileMeta {
                            file_name: f.file_name().to_string(),
                            time_range: f.time_range().clone(),
                            level: f.level(),
                        }));
                        Ok(meta)
                    }
                    Err(e) => Err(e),
                }
            });
        }

        futures::future::join_all(futs)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
    }

    async fn write_manifest_and_apply(&self, files: Vec<FileMeta>) -> Result<()> {
        let region_version = self.version.metadata().version();
        let flushed_sequence = self.version.current().flushed_sequence();

        let edit = RegionEdit {
            region_version,
            flushed_sequence,
            files_to_add: files,
            files_to_remove: vec![],
        };

        todo!()
        // self.writer.write_edit_and_apply()
    }
}

#[async_trait::async_trait]
impl CompactionTask for CompactionTaskImpl {
    async fn run(mut self) -> Result<()> {
        let ssts = self.merge_ssts().await?;

        Ok(())
    }
}

#[allow(unused)]
pub(crate) struct CompactionInput {
    input_level: u8,
    output_level: u8,
    file: FileHandle,
}

/// Many-to-many compaction can be decomposed to a many-to-one compaction from level n to level n+1
/// and a many-to-one compaction from level n+1 to level n+1.
#[derive(Debug)]
#[allow(unused)]
pub struct CompactionOutput {
    /// Compaction output file level.
    pub(crate) output_level: Level,
    /// The left bound of time bucket.
    pub(crate) bucket_bound: i64,
    /// Bucket duration in seconds.
    pub(crate) bucket: i64,
    /// Compaction input files.
    pub(crate) inputs: Vec<FileHandle>,
}

impl CompactionOutput {
    async fn run(
        &self,
        schema: RegionSchemaRef,
        sst_layer: AccessLayerRef,
        object_store: ObjectStore,
    ) -> Result<FileMeta> {
        let reader = build_sst_reader(
            schema,
            sst_layer,
            &self.inputs,
            self.bucket_bound,
            self.bucket_bound + self.bucket,
        )
        .await
        .unwrap();
        let output_file_name = format!("{}.parquet", Uuid::new_v4().hyphenated());
        let opts = WriteOptions {};
        let SstInfo { time_range } =
            ParquetWriter::new(&output_file_name, Source::Reader(reader), object_store)
                .write_sst(&opts)
                .await?;

        Ok(FileMeta {
            file_name: output_file_name,
            time_range,
            level: self.output_level,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::compaction::task::CompactionTask;

    pub type CallbackRef = Arc<dyn Fn() + Send + Sync>;
    pub struct NoopCompactionTask {
        pub cbs: Vec<CallbackRef>,
    }

    impl NoopCompactionTask {
        pub fn new(cbs: Vec<CallbackRef>) -> Self {
            Self { cbs }
        }
    }

    #[async_trait::async_trait]
    impl CompactionTask for NoopCompactionTask {
        async fn run(self) -> Result<()> {
            for cb in &self.cbs {
                cb()
            }
            Ok(())
        }
    }
}
