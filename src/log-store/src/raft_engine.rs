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

use crate::raft_engine::protos::logstore::EntryImpl;

mod backend;
pub mod log_store;

pub use backend::RaftEngineBackend;
pub use raft_engine::Config;
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::Provider;
use store_api::storage::RegionId;

#[allow(renamed_and_removed_lints)]
pub mod protos {
    include!(concat!(env!("OUT_DIR"), concat!("/", "protos/", "mod.rs")));
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
