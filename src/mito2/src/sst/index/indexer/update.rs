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
use datatypes::arrow::record_batch::RecordBatch;

use crate::read::Batch;
use crate::sst::index::Indexer;

impl Indexer {
    pub(crate) async fn do_update(&mut self, batch: &mut Batch) {
        if batch.is_empty() {
            return;
        }
        if let Err(err) = self.prepare_primary_key(batch) {
            if cfg!(any(test, feature = "test")) {
                panic!("Failed to decode primary key for indexes: {err:?}");
            }
            warn!(err; "Failed to decode primary key for indexes, region_id: {}, file_id: {}", self.region_id, self.file_id);
            self.do_abort().await;
            return;
        }
        let mut position = 0;
        while position < self.creators.len() {
            match self.creators[position].creator.update(batch).await {
                Ok(()) => position += 1,
                Err(err) => {
                    if cfg!(any(test, feature = "test")) {
                        panic!("Failed to update index: {err:?}");
                    }
                    warn!(err; "Failed to update index, region_id: {}, file_id: {}, kind: {:?}", self.region_id, self.file_id, self.creators[position].kind);
                    self.do_abort().await;
                    return;
                }
            }
        }
    }

    pub(crate) async fn do_update_flat(&mut self, batch: &RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }
        let mut position = 0;
        while position < self.creators.len() {
            match self.creators[position].creator.update_flat(batch).await {
                Ok(()) => position += 1,
                Err(err) => {
                    if cfg!(any(test, feature = "test")) {
                        panic!("Failed to update flat index: {err:?}");
                    }
                    warn!(err; "Failed to update flat index, region_id: {}, file_id: {}, kind: {:?}", self.region_id, self.file_id, self.creators[position].kind);
                    self.do_abort().await;
                    return;
                }
            }
        }
    }
}
