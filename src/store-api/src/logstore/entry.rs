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

use std::mem::size_of;

use crate::logstore::provider::Provider;
use crate::storage::RegionId;

/// An entry's id.
/// Different log store implementations may interpret the id to different meanings.
pub type Id = u64;

/// The [Entry::Naive] is used in RaftEngineLogStore and KafkaLogStore.
///
/// The [Entry::MultiplePart] contains multiple parts of data that split from a large entry, is used in KafkaLogStore,
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Naive(NaiveEntry),
    MultiplePart(MultiplePartEntry),
}

impl Entry {
    /// Into [NaiveEntry] if it's type of [Entry::Naive].
    pub fn into_naive_entry(self) -> Option<NaiveEntry> {
        match self {
            Entry::Naive(entry) => Some(entry),
            Entry::MultiplePart(_) => None,
        }
    }

    /// Into [MultiplePartEntry] if it's type of [Entry::MultiplePart].
    pub fn into_multiple_part_entry(self) -> Option<MultiplePartEntry> {
        match self {
            Entry::Naive(_) => None,
            Entry::MultiplePart(entry) => Some(entry),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NaiveEntry {
    pub provider: Provider,
    pub region_id: RegionId,
    pub entry_id: Id,
    pub data: Vec<u8>,
}

impl NaiveEntry {
    /// Estimates the persisted size of the entry.
    fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.data.len() * size_of::<u8>()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiplePartHeader {
    First,
    Middle(usize),
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiplePartEntry {
    pub provider: Provider,
    pub region_id: RegionId,
    pub entry_id: Id,
    pub headers: Vec<MultiplePartHeader>,
    pub parts: Vec<Vec<u8>>,
}

impl MultiplePartEntry {
    fn is_complete(&self) -> bool {
        self.headers.contains(&MultiplePartHeader::First)
            && self.headers.contains(&MultiplePartHeader::Last)
    }

    /// Estimates the persisted size of the entry.
    fn estimated_size(&self) -> usize {
        size_of::<Self>()
            + self
                .parts
                .iter()
                .map(|data| data.len() * size_of::<u8>())
                .sum::<usize>()
            + self.headers.len() * size_of::<MultiplePartHeader>()
    }
}

impl Entry {
    /// Returns the [Provider]
    pub fn provider(&self) -> &Provider {
        match self {
            Entry::Naive(entry) => &entry.provider,
            Entry::MultiplePart(entry) => &entry.provider,
        }
    }

    /// Returns the [RegionId]
    pub fn region_id(&self) -> RegionId {
        match self {
            Entry::Naive(entry) => entry.region_id,
            Entry::MultiplePart(entry) => entry.region_id,
        }
    }

    /// Returns the [Id]
    pub fn entry_id(&self) -> Id {
        match self {
            Entry::Naive(entry) => entry.entry_id,
            Entry::MultiplePart(entry) => entry.entry_id,
        }
    }

    /// Returns the [Id]
    pub fn set_entry_id(&mut self, id: Id) {
        match self {
            Entry::Naive(entry) => entry.entry_id = id,
            Entry::MultiplePart(entry) => entry.entry_id = id,
        }
    }

    /// Returns true if it's a complete entry.
    pub fn is_complete(&self) -> bool {
        match self {
            Entry::Naive(_) => true,
            Entry::MultiplePart(entry) => entry.is_complete(),
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Entry::Naive(entry) => entry.data,
            Entry::MultiplePart(entry) => entry.parts.concat(),
        }
    }

    pub fn estimated_size(&self) -> usize {
        match self {
            Entry::Naive(entry) => entry.estimated_size(),
            Entry::MultiplePart(entry) => entry.estimated_size(),
        }
    }
}
