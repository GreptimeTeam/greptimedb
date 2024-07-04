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
use puffin::puffin_manager::PuffinWriter;

use crate::sst::index::Indexer;

impl Indexer {
    pub(crate) async fn do_abort(&mut self) {
        self.do_abort_inverted_index().await;
        self.do_abort_puffin_writer().await;
    }

    async fn do_abort_inverted_index(&mut self) {
        let Some(mut creator) = self.inverted_indexer.take() else {
            return;
        };

        let Err(err) = creator.abort().await else {
            return;
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to abort inverted index, region_id: {}, file_id: {}, err: {}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to abort inverted index, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }
    }

    pub(crate) async fn do_abort_puffin_writer(&mut self) {
        let Some(puffin_writer) = self.puffin_writer.take() else {
            return;
        };

        let err = match puffin_writer.finish().await {
            Ok(_) => return,
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to abort puffin writer, region_id: {}, file_id: {}, err: {}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to abort puffin writer, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }
    }
}
