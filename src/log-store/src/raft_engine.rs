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

mod backend;
pub mod log_store;

use std::sync::Arc;

pub use backend::RaftEngineBackend;
pub use raft_engine::Config;
use raft_engine::Engine;
use snafu::ResultExt;
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::Provider;
use store_api::storage::RegionId;

use crate::error::{JoinSnafu, RaftEngineSnafu, Result};
use crate::raft_engine::protos::logstore::EntryImpl;

#[allow(renamed_and_removed_lints, mismatched_lifetime_syntaxes, unused_parens)]
pub mod protos {
    include!(concat!(env!("OUT_DIR"), concat!("/", "protos/", "mod.rs")));
}

/// Syncs and drops `engine` on a blocking thread, as both block: dropping the last reference
/// joins the engine's threads and trims and syncs the active log file, which is what closes it.
pub(crate) async fn close_engine(engine: Arc<Engine>) -> Result<()> {
    common_runtime::spawn_blocking_global(move || {
        let synced = engine.sync().context(RaftEngineSnafu);
        drop(engine);
        synced
    })
    .await
    .context(JoinSnafu)?
}

impl EntryImpl {
    pub fn create(id: u64, ns: u64, data: Vec<u8>) -> Self {
        Self {
            id,
            namespace_id: ns,
            data,
            ..Default::default()
        }
    }
}

impl From<EntryImpl> for Entry {
    fn from(
        EntryImpl {
            id,
            namespace_id,
            data,
            ..
        }: EntryImpl,
    ) -> Self {
        Entry::Naive(NaiveEntry {
            provider: Provider::raft_engine_provider(namespace_id),
            region_id: RegionId::from_u64(namespace_id),
            entry_id: id,
            data,
        })
    }
}

/// Total size of the raft-engine log files in `dir`.
///
/// raft-engine preallocates the active log file ahead of the writes, where the platform and
/// file system support it, and only trims that tail when the engine is closed.
#[cfg(test)]
pub(crate) fn log_files_size(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "raftlog"))
        .map(|path| std::fs::metadata(path).unwrap().len())
        .sum()
}

/// Size above which the log files are taken to still hold the tail raft-engine preallocates
/// for the active file. This is a detection threshold, below the 2 MiB raft-engine actually
/// allocates, not that allocation size.
#[cfg(test)]
pub(crate) const PREALLOCATION_THRESHOLD: u64 = 1024 * 1024;
