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

//! Storage engine implementation.
#![feature(map_first_last)]
mod background;
mod chunk;
pub mod codec;
pub mod config;
mod engine;
pub mod error;
mod flush;
pub mod manifest;
pub mod memtable;
pub mod metadata;
pub mod proto;
pub mod read;
pub mod region;
pub mod schema;
mod snapshot;
mod sst;
mod sync;
#[cfg(test)]
mod test_util;
mod version;
mod wal;
pub mod write_batch;

pub use engine::EngineImpl;
