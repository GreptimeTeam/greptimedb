// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use store_api::logstore::LogStore;
use tempdir::TempDir;

use crate::raft_engine::log_store::RaftEngineLogStore;
use crate::LogConfig;

/// Create a tmp directory for write log, used for test.
// TODO: Add a test feature
pub async fn create_tmp_local_file_log_store(dir: &str) -> (RaftEngineLogStore, TempDir) {
    let dir = TempDir::new(dir).unwrap();
    let cfg = LogConfig {
        append_buffer_size: 128,
        max_log_file_size: 128,
        log_file_dir: dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    };

    let logstore = RaftEngineLogStore::try_new(cfg).unwrap();
    logstore.start().await.unwrap();
    (logstore, dir)
}
