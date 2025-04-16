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

use common_telemetry::error;
use rskafka::client::partition::OffsetAt;
use snafu::ResultExt;

use crate::error;
use crate::kafka::worker::{BackgroundProducerWorker, DumpIndexRequest};

impl BackgroundProducerWorker {
    pub(crate) async fn dump_index(&mut self, req: DumpIndexRequest) {
        match self
            .client
            .get_offset(OffsetAt::Latest)
            .await
            .context(error::GetOffsetSnafu {
                topic: &self.provider.topic,
            }) {
            Ok(offset) => {
                self.index_collector.set_latest_entry_id(offset as u64);
                self.index_collector.dump(req.encoder.as_ref());
                let _ = req.sender.send(());
            }
            Err(err) => error!(err; "Failed to do checkpoint"),
        }
    }
}
