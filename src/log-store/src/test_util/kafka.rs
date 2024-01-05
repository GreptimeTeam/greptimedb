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

use std::sync::atomic::{AtomicU64 as AtomicEntryId, Ordering};
use std::sync::Mutex;

use rand::distributions::Alphanumeric;
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use store_api::logstore::EntryId;

use crate::kafka::{EntryImpl, NamespaceImpl};

/// A builder for building entries for a namespace.
pub struct EntryBuilder {
    /// The namespace of the entries.
    ns: NamespaceImpl,
    /// The next entry id to allocate. It starts from 0 by default.
    next_entry_id: AtomicEntryId,
    /// A generator for supporting random data generation.
    /// Wrapped with Mutex<Option<_>> to provide interior mutability.
    rng: Mutex<Option<ThreadRng>>,
}

impl EntryBuilder {
    /// Creates an EntryBuilder for the given namespace.
    pub fn new(ns: NamespaceImpl) -> Self {
        Self {
            ns,
            next_entry_id: AtomicEntryId::new(0),
            rng: Mutex::new(Some(thread_rng())),
        }
    }

    /// Sets the next entry id to the given entry id.
    pub fn next_entry_id(self, entry_id: EntryId) -> Self {
        Self {
            next_entry_id: AtomicEntryId::new(entry_id),
            ..self
        }
    }

    /// Skips the next `step` entry ids and returns the next entry id after the stepping.
    pub fn skip(&mut self, step: EntryId) -> EntryId {
        let old = self.next_entry_id.fetch_add(step, Ordering::Relaxed);
        old + step
    }

    /// Builds an entry with the given data.
    pub fn with_data<D: AsRef<[u8]>>(&self, data: D) -> EntryImpl {
        EntryImpl {
            data: data.as_ref().to_vec(),
            id: self.alloc_entry_id(),
            ns: self.ns.clone(),
        }
    }

    /// Builds an entry with random data.
    pub fn with_random_data(&self) -> EntryImpl {
        self.with_data(self.make_random_data())
    }

    fn alloc_entry_id(&self) -> EntryId {
        self.next_entry_id.fetch_add(1, Ordering::Relaxed)
    }

    fn make_random_data(&self) -> Vec<u8> {
        let mut guard = self.rng.lock().unwrap();
        let rng = guard.as_mut().unwrap();
        (0..42).map(|_| rng.sample(Alphanumeric)).collect()
    }
}

/// Builds a batch of entries each with random data.
pub fn entries_with_random_data(batch_size: usize, builder: &EntryBuilder) -> Vec<EntryImpl> {
    (0..batch_size)
        .map(|_| builder.with_random_data())
        .collect()
}

/// Creates a new Kafka namespace with the given topic and region id.
pub fn new_namespace(topic: &str, region_id: u64) -> NamespaceImpl {
    NamespaceImpl {
        topic: topic.to_string(),
        region_id,
    }
}
