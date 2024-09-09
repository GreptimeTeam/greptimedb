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

use std::path::Path;

use common_base::readable_size::ReadableSize;
use common_wal::config::kafka::common::KafkaConnectionConfig;
use common_wal::config::kafka::DatanodeKafkaConfig;
use common_wal::config::raft_engine::RaftEngineConfig;

use crate::kafka::log_store::KafkaLogStore;
use crate::raft_engine::log_store::RaftEngineLogStore;

/// Create a write log for the provided path, used for test.
pub async fn create_tmp_local_file_log_store<P: AsRef<Path>>(path: P) -> RaftEngineLogStore {
    let path = path.as_ref().display().to_string();
    let cfg = RaftEngineConfig {
        file_size: ReadableSize::kb(128),
        ..Default::default()
    };
    RaftEngineLogStore::try_new(path, &cfg).await.unwrap()
}

/// Create a [KafkaLogStore].
pub async fn create_kafka_log_store(broker_endpoints: Vec<String>) -> KafkaLogStore {
    KafkaLogStore::try_new(
        &DatanodeKafkaConfig {
            connection: KafkaConnectionConfig {
                broker_endpoints,
                ..Default::default()
            },
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap()
}
