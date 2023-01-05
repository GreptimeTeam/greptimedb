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

use store_api::logstore::entry::{Entry as EntryTrait, Id};
use store_api::logstore::namespace::Namespace as NamespaceTrait;

use crate::error::Error;
use crate::raft_engine::protos::logstore::{Entry, Namespace};

pub mod log_store;

pub mod protos {
    include!(concat!(env!("OUT_DIR"), concat!("/", "protos/", "mod.rs")));
}

impl Entry {
    pub fn create(id: u64, ns: u64, data: Vec<u8>) -> Self {
        Self {
            id,
            namespace_id: ns,
            data,
            ..Default::default()
        }
    }
}
impl Namespace {
    pub fn with_id(id: Id) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Namespace {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl NamespaceTrait for Namespace {
    fn id(&self) -> store_api::logstore::namespace::Id {
        self.id
    }
}

impl EntryTrait for Entry {
    type Error = Error;
    type Namespace = Namespace;

    fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    fn id(&self) -> Id {
        self.id
    }

    fn set_id(&mut self, id: Id) {
        self.id = id
    }

    fn namespace(&self) -> Self::Namespace {
        Namespace {
            id: self.namespace_id,
            ..Default::default()
        }
    }
}
