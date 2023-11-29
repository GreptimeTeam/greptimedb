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

use std::hash::{Hash, Hasher};

use store_api::logstore::entry::{Entry, Id as EntryId};
use store_api::logstore::namespace::{Id as NamespaceId, Namespace};

use crate::error::Error;
use crate::raft_engine::protos::logstore::{EntryImpl, NamespaceImpl};

mod backend;
pub mod log_store;

pub use backend::RaftEngineBackend;
pub use raft_engine::Config;

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

impl NamespaceImpl {
    pub fn with_id(id: NamespaceId) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }
}

#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for NamespaceImpl {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Eq for NamespaceImpl {}

impl Namespace for NamespaceImpl {
    fn id(&self) -> NamespaceId {
        self.id
    }
}

impl Entry for EntryImpl {
    type Error = Error;
    type Namespace = NamespaceImpl;

    fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    fn id(&self) -> EntryId {
        self.id
    }

    fn namespace(&self) -> Self::Namespace {
        NamespaceImpl {
            id: self.namespace_id,
            ..Default::default()
        }
    }
}
