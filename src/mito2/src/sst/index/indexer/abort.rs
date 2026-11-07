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

use common_telemetry::warn;

use crate::access_layer::TempFileCleaner;
use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::index::Indexer;

impl Indexer {
    /// Defers builtin indexes without discarding synchronous extension state.
    pub(crate) async fn disable_builtin_indexes(&mut self) {
        self.dense_pk_decoder = None;
        let mut position = 0;
        while position < self.creators.len() {
            if self.creators[position].is_builtin() {
                self.abort_creator(position).await;
            } else {
                position += 1;
            }
        }
        self.flush_mem_metrics();
    }

    pub(crate) async fn abort_creator(&mut self, position: usize) {
        let mut active = self.creators.remove(position);
        if let Err(err) = active.creator.abort().await {
            if active.is_builtin() && cfg!(any(test, feature = "test")) {
                panic!("Failed to abort index: {err:?}");
            }
            warn!(err; "Failed to abort index, region_id: {}, file_id: {}, kinds: {:?}", self.region_id, self.file_id, active.kinds);
        }
    }

    pub(crate) async fn do_abort(&mut self) {
        self.dense_pk_decoder = None;
        while !self.creators.is_empty() {
            self.abort_creator(0).await;
        }
        self.do_prune_intm_sst_dir().await;
        if self.write_cache_enabled {
            self.do_abort_clean_fs_temp_dir().await;
        }
        self.puffin_manager = None;
    }

    async fn do_abort_clean_fs_temp_dir(&mut self) {
        let Some(puffin_manager) = &self.puffin_manager else {
            return;
        };
        let fs_accessor = puffin_manager.file_accessor();

        let fs_handle = RegionIndexId::new(
            RegionFileId::new(self.physical_region_id, self.file_id),
            self.index_version,
        )
        .to_string();
        TempFileCleaner::clean_atomic_dir_files(fs_accessor.store().store(), &[&fs_handle]).await;
    }
}
