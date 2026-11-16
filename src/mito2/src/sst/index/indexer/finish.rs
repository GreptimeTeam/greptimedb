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

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_telemetry::warn;
use puffin::puffin_manager::{PuffinManager, PuffinWriter};

use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::ByteCount;
use crate::sst::index::{IndexOutput, Indexer};

impl Indexer {
    pub(crate) async fn do_finish(&mut self) -> IndexOutput {
        self.dense_pk_decoder = None;
        if self.is_empty() {
            return IndexOutput::default();
        }
        let Some(mut writer) = self.build_puffin_writer().await else {
            self.do_abort().await;
            return IndexOutput::default();
        };
        let mut output = IndexOutput::default();
        while !self.creators.is_empty() {
            let mut active = self.creators.remove(0);
            match active.creator.finish(&mut writer).await {
                Ok(entries) => {
                    if let Err(err) = entries.iter().try_for_each(|entry| entry.validate()) {
                        warn!(err; "Invalid index completion, region_id: {}, file_id: {}", self.region_id, self.file_id);
                        self.failure.get_or_insert_with(|| Arc::new(err));
                        drop(active);
                        self.do_abort().await;
                        return IndexOutput::default();
                    }
                    output.entries.extend(entries);
                }
                Err(err) => {
                    if active.is_builtin() && cfg!(any(test, feature = "test")) {
                        panic!("Failed to finish index: {err:?}");
                    }
                    warn!(err; "Failed to finish index, region_id: {}, file_id: {}, kinds: {:?}", self.region_id, self.file_id, active.kinds);
                    self.record_failure(err);
                    drop(active);
                    self.do_abort().await;
                    return IndexOutput::default();
                }
            }
        }
        self.do_prune_intm_sst_dir().await;
        output.file_size = self.do_finish_puffin_writer(writer).await;
        if output.file_size == 0 {
            return IndexOutput::default();
        }
        output.version = self.index_version;
        output
    }

    async fn build_puffin_writer(&mut self) -> Option<SstPuffinWriter> {
        let puffin_manager = self.puffin_manager.clone()?;

        let err = match puffin_manager
            .writer(&RegionIndexId::new(
                RegionFileId::new(self.physical_region_id, self.file_id),
                self.index_version,
            ))
            .await
        {
            Ok(writer) => return Some(writer),
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to create puffin writer, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to create puffin writer, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        self.record_failure(BoxedError::new(err));
        None
    }

    async fn do_finish_puffin_writer(&mut self, writer: SstPuffinWriter) -> ByteCount {
        let err = match writer.finish().await {
            Ok(size) => return size,
            Err(err) => err,
        };

        // Use the production best-effort path in tests as well, so injected
        // close failures can verify that no index descriptors are published.
        warn!(
            err; "Failed to finish puffin writer, region_id: {}, file_id: {}",
            self.region_id, self.file_id,
        );

        self.record_failure(BoxedError::new(err));
        0
    }

    pub(crate) async fn do_prune_intm_sst_dir(&mut self) {
        if let Some(manager) = self.intermediate_manager.take()
            && let Err(e) = manager.prune_sst_dir(&self.region_id, &self.file_id).await
        {
            warn!(e; "Failed to prune intermediate SST directory, region_id: {}, file_id: {}", self.region_id, self.file_id);
        }
    }
}
