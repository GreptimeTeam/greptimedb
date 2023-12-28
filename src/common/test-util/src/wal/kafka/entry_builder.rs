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

use log_store::kafka::{EntryImpl, NamespaceImpl};
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use store_api::logstore::EntryId;

const DEFAULT_DATA: &[u8; 10] = b"[greptime]";

/// A builder for building entries for a namespace.
pub struct EntryBuilder {
    /// The namespace of the entries.
    ns: NamespaceImpl,
    /// The next entry id to allocate. It starts from 0 by default.
    next_entry_id: AtomicEntryId,
    /// A generator for supporting random data generation.
    /// Wrapped with Mutex<Option<_>> to provide interior mutability.
    rng: Mutex<Option<ThreadRng>>,
    /// The data pool from which random data is constructed.
    data_pool: Vec<u8>,
}

impl EntryBuilder {
    /// Creates an EntryBuilder for the given namespace.
    pub fn new(ns: NamespaceImpl) -> Self {
        // Makes a data pool with alphabets and numbers.
        let data_pool = ('a'..='z')
            .chain('A'..='Z')
            .chain((0..=9).map(|digit| char::from_digit(digit, 10).unwrap()))
            .map(|c| c as u8)
            .collect::<Vec<_>>();
        Self {
            ns,
            next_entry_id: AtomicEntryId::new(0),
            rng: Mutex::new(Some(thread_rng())),
            data_pool,
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

    /// Builds an entry with the default data.
    pub fn with_default_data(&self) -> EntryImpl {
        EntryImpl {
            data: DEFAULT_DATA.to_vec(),
            id: self.alloc_entry_id(),
            ns: self.ns.clone(),
        }
    }

    /// Builds an entry with random data.
    pub fn with_random_data(&self) -> EntryImpl {
        EntryImpl {
            data: self.make_random_data(),
            id: self.alloc_entry_id(),
            ns: self.ns.clone(),
        }
    }

    fn alloc_entry_id(&self) -> EntryId {
        self.next_entry_id.fetch_add(1, Ordering::Relaxed)
    }

    fn make_random_data(&self) -> Vec<u8> {
        let mut rng_guard = self.rng.lock().unwrap();
        let mut rng = rng_guard.as_mut().unwrap();
        let amount = rng.gen_range(0..self.data_pool.len());
        self.data_pool
            .choose_multiple(&mut rng, amount)
            .map(|x| *x)
            .collect()
    }
}
