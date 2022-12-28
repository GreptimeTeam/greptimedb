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

use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use raft_engine::Engine;
use store_api::logstore::entry::Id;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::{AppendResponse, LogStore};

use crate::error::Error;
use crate::raft_engine::protos::logstore::{Entry, Namespace};

pub struct RaftEngineLogstore {
    engine: Engine,
}

impl Debug for RaftEngineLogstore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RaftEngineLogstore")
    }
}

#[async_trait::async_trait]
impl LogStore for RaftEngineLogstore {
    type Error = Error;
    type Namespace = Namespace;
    type Entry = Entry;

    async fn start(&self) -> Result<(), Self::Error> {
        todo!()
    }

    async fn stop(&self) -> Result<(), Self::Error> {
        todo!()
    }

    async fn append(&self, e: Self::Entry) -> Result<AppendResponse, Self::Error> {
        todo!()
    }

    async fn append_batch(
        &self,
        ns: &Self::Namespace,
        e: Vec<Self::Entry>,
    ) -> Result<Id, Self::Error> {
        todo!()
    }

    async fn read(
        &self,
        ns: &Self::Namespace,
        id: Id,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>, Self::Error> {
        todo!()
    }

    async fn create_namespace(&mut self, ns: &Self::Namespace) -> Result<(), Self::Error> {
        todo!()
    }

    async fn delete_namespace(&mut self, ns: &Self::Namespace) -> Result<(), Self::Error> {
        todo!()
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error> {
        todo!()
    }

    fn entry<D: AsRef<[u8]>>(&self, data: D, id: Id, ns: Self::Namespace) -> Self::Entry {
        todo!()
    }

    fn namespace(&self, id: store_api::logstore::namespace::Id) -> Self::Namespace {
        todo!()
    }

    async fn obsolete(&self, namespace: Self::Namespace, id: Id) -> Result<(), Self::Error> {
        todo!()
    }
}
