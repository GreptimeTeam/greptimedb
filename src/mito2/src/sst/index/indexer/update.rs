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

        if !self.do_update_inverted_index(batch).await {
            self.do_abort().await;
        }
        if !self.do_update_fulltext_index(batch).await {
            self.do_abort().await;
        }
        if !self.do_update_bloom_filter(batch).await {
            self.do_abort().await;
        }
        #[cfg(feature = "vector_index")]
        if !self.do_update_vector_index(batch).await {
            self.do_abort().await;
        }
    }

    /// Returns false if the update failed.
    async fn do_update_inverted_index(&mut self, batch: &mut Batch) -> bool {
        let Some(creator) = self.inverted_indexer.as_mut() else {
            return true;
        };

        let Err(err) = creator.update(batch).await else {
            return true;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to update inverted index, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to update inverted index, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    /// Returns false if the update failed.
    async fn do_update_fulltext_index(&mut self, batch: &mut Batch) -> bool {
        let Some(creator) = self.fulltext_indexer.as_mut() else {
            return true;
        };

        let Err(err) = creator.update(batch).await else {
            return true;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to update full-text index, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to update full-text index, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    /// Returns false if the update failed.
    async fn do_update_bloom_filter(&mut self, batch: &mut Batch) -> bool {
        let Some(creator) = self.bloom_filter_indexer.as_mut() else {
            return true;
        };

        let Err(err) = creator.update(batch).await else {
            return true;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to update bloom filter, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to update bloom filter, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    /// Returns false if the update failed.
    #[cfg(feature = "vector_index")]
    async fn do_update_vector_index(&mut self, batch: &mut Batch) -> bool {
        let Some(creator) = self.vector_indexer.as_mut() else {
            return true;
        };

        let Err(err) = creator.update(batch).await else {
            return true;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to update vector index, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to update vector index, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    pub(crate) async fn do_update_flat(&mut self, batch: &RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }

        if !self.do_update_flat_inverted_index(batch).await {
            self.do_abort().await;
        }
        if !self.do_update_flat_fulltext_index(batch).await {
            self.do_abort().await;
        }
        if !self.do_update_flat_bloom_filter(batch).await {
            self.do_abort().await;
        }
    }

    /// Returns false if the update failed.
    async fn do_update_flat_inverted_index(&mut self, batch: &RecordBatch) -> bool {
        let Some(creator) = self.inverted_indexer.as_mut() else {
            return true;
        };

        let Err(err) = creator.update_flat(batch).await else {
            return true;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to update inverted index with flat format, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to update inverted index with flat format, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    /// Returns false if the update failed.
    async fn do_update_flat_fulltext_index(&mut self, batch: &RecordBatch) -> bool {
        let Some(creator) = self.fulltext_indexer.as_mut() else {
            return true;
        };

        let Err(err) = creator.update_flat(batch).await else {
            return true;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to update full-text index with flat format, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to update full-text index with flat format, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    /// Returns false if the update failed.
    async fn do_update_flat_bloom_filter(&mut self, batch: &RecordBatch) -> bool {
        let Some(creator) = self.bloom_filter_indexer.as_mut() else {
            return true;
        };

        let Err(err) = creator.update_flat(batch).await else {
            return true;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to update bloom filter with flat format, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to update bloom filter with flat format, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }
}
