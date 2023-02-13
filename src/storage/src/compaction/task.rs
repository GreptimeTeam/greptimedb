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

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use common_telemetry::{error, info};
use object_store::ObjectStore;
use store_api::logstore::LogStore;
use uuid::Uuid;

use crate::compaction::writer::build_sst_reader;
use crate::error::Result;
use crate::manifest::action::RegionEdit;
use crate::manifest::region::RegionManifest;
use crate::region::{RegionWriterRef, SharedDataRef};
use crate::schema::RegionSchemaRef;
use crate::sst::parquet::{ParquetWriter, Source};
use crate::sst::{AccessLayerRef, FileHandle, FileMeta, Level, SstInfo, WriteOptions};
use crate::wal::Wal;

#[async_trait::async_trait]
pub trait CompactionTask: Send + Sync + 'static {
    async fn run(self) -> Result<()>;
}

#[allow(unused)]
pub(crate) struct CompactionTaskImpl<S: LogStore> {
    pub schema: RegionSchemaRef,
    pub sst_layer: AccessLayerRef,
    pub outputs: Vec<CompactionOutput>,
    pub writer: RegionWriterRef,
    pub shared_data: SharedDataRef,
    pub wal: Wal<S>,
    pub manifest: RegionManifest,
}

impl<S: LogStore> CompactionTaskImpl<S> {
    /// Compacts inputs SSTs, returns `(output file, compacted input file)`.
    async fn merge_ssts(&mut self) -> Result<(Vec<FileMeta>, Vec<FileMeta>)> {
        let mut futs = Vec::with_capacity(self.outputs.len());
        let compacted_inputs = Arc::new(Mutex::new(HashSet::new()));

        for output in self.outputs.drain(..) {
            let schema = self.schema.clone();
            let sst_layer = self.sst_layer.clone();
            let object_store = self.sst_layer.object_store();
            let compacted = compacted_inputs.clone();
            futs.push(async move {
                match output.run(schema, sst_layer, object_store).await {
                    Ok(meta) => {
                        compacted
                            .lock()
                            .unwrap()
                            .extend(output.inputs.iter().map(|f| FileMeta {
                                file_name: f.file_name().to_string(),
                                time_range: *f.time_range(),
                                level: f.level(),
                            }));
                        Ok(meta)
                    }
                    Err(e) => Err(e),
                }
            });
        }

        let outputs = futures::future::join_all(futs)
            .await
            .into_iter()
            .collect::<Result<_>>()?;
        let compacted = compacted_inputs.lock().unwrap().drain().collect();
        Ok((outputs, compacted))
    }

    /// Writes updated SST info into manifest.
    async fn write_manifest_and_apply(
        &self,
        output: Vec<FileMeta>,
        input: Vec<FileMeta>,
    ) -> Result<()> {
        let version = &self.shared_data.version_control;
        let region_version = version.metadata().version();

        let edit = RegionEdit {
            region_version,
            flushed_sequence: None,
            files_to_add: output,
            files_to_remove: input,
        };
        info!(
            "Compacted region: {}, region edit: {:?}",
            version.metadata().name(),
            edit
        );
        self.writer
            .write_edit_and_apply(&self.wal, &self.shared_data, &self.manifest, edit, None)
            .await
    }

    /// Mark files are under compaction.
    fn mark_files_compacting(&self, compacting: bool) {
        for o in &self.outputs {
            for input in &o.inputs {
                input.set_compacting(compacting);
            }
        }
    }
}

#[async_trait::async_trait]
impl<S: LogStore> CompactionTask for CompactionTaskImpl<S> {
    async fn run(mut self) -> Result<()> {
        self.mark_files_compacting(true);
        match self.merge_ssts().await {
            Ok((output, compacted)) => {
                self.write_manifest_and_apply(output, compacted).await?;
            }
            Err(e) => {
                self.mark_files_compacting(false);
                error!(e; "Failed to compact region: {}", self.shared_data.name());
            }
        }
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
        .await?;
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
