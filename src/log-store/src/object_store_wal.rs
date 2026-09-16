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

//! Durable primitives of a WAL that stores entries as immutable object store
//! objects.
//!
//! Entries of many regions are batched into a single object, written once and
//! never mutated afterwards. An object is laid out as
//!
//! ```text
//! header | segment (region 1) | ... | segment (region N) | footer | trailer
//! ```
//!
//! The header carries the magic `GTWALOBJ`, the format version, the object
//! sequence and the instance that wrote the object. Each segment holds the
//! entries of exactly one region, ordered by entry id, and segments are ordered
//! by region id. The footer indexes every segment with its region id, entry id
//! range, byte range and CRC32. The fixed-size trailer points at the footer and
//! carries the CRC32 of the footer and of the whole object, so a reader locates
//! the footer by reading the fixed-length trailer at the end of the object.

// The format has no callers until the store that writes and reads objects lands.
#[allow(dead_code)]
mod format;
