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

use crate::sst::index::Indexer;

impl Indexer {
    pub(crate) async fn do_abort(&mut self) {
        self.do_abort_inverted_index().await;
        self.do_abort_fulltext_index().await;
        self.do_abort_bloom_filter().await;
        self.puffin_manager = None;
    }

    async fn do_abort_inverted_index(&mut self) {
        let Some(mut indexer) = self.inverted_indexer.take() else {
            return;
        };
        let Err(err) = indexer.abort().await else {
            return;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to abort inverted index, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to abort inverted index, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }
    }

    async fn do_abort_fulltext_index(&mut self) {
        let Some(mut indexer) = self.fulltext_indexer.take() else {
            return;
        };
        let Err(err) = indexer.abort().await else {
            return;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to abort full-text index, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to abort full-text index, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }
    }

    async fn do_abort_bloom_filter(&mut self) {
        let Some(mut indexer) = self.bloom_filter_indexer.take() else {
            return;
        };
        let Err(err) = indexer.abort().await else {
            return;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to abort bloom filter, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to abort bloom filter, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }
    }
}
