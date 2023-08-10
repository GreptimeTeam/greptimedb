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

use crate::raft_engine::log_store::RaftEngineLogStore;
use crate::LogConfig;

/// Create a write log for the provided path, used for test.
pub async fn create_tmp_local_file_log_store<P: AsRef<Path>>(path: P) -> RaftEngineLogStore {
    let cfg = LogConfig {
        file_size: 128 * 1024,
        log_file_dir: path.as_ref().display().to_string(),
        ..Default::default()
    };
    RaftEngineLogStore::try_new(cfg).await.unwrap()
}
