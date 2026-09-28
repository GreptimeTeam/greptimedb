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
//! sequence, the epoch of the instance that wrote the object, the link to the
//! object it extends, and a CRC32 of the header. Each segment holds the
//! entries of exactly one region, ordered by entry id, and segments are ordered
//! by region id. The footer indexes every segment with its region id, entry id
//! range, byte range and CRC32. The fixed-size trailer points at the footer and
//! carries the CRC32 of the footer and of the whole object, so a reader locates
//! the footer by reading the fixed-length trailer at the end of the object.
//!
//! Object sequences increase monotonically within one prefix and may leave
//! gaps, so recovery continues after the largest sequence present. An object is
//! created conditionally: rewriting a sequence with the content it already
//! holds is a no-op at the object store, while different content under a taken
//! sequence is a conflict. A create can fail with an unknown outcome and its
//! object can still appear, so objects form a chain: every object links to the
//! object it extends by sequence and epoch, and recovery replays only the chain
//! that ends at the complete object with the largest epoch and sequence. An
//! object is complete when every link on its chain names a present object of
//! the recorded epoch, back to an object that starts the chain. Objects off the
//! chain are orphans, which are never replayed but keep their sequences. Each
//! open writes an object without segments above every present object before
//! it accepts writes, and its epoch is one above the sequence of that object,
//! so no two instances share an epoch and a late object of an earlier
//! instance never ends the chain.
//!
//! Recovery lists the objects, reads and verifies only the header, trailer and
//! footer of each, and indexes the footers of the chain in sequence order to
//! rebuild the object catalog, which rejects a sequence it already holds.
//! Segments are read and checksummed only when a read decodes them.
//!
//! Entry ids are object-sequence-major, see [`entry_id`]: the high bits of an
//! id name the object that holds the entry, the low bits its position among
//! the entries of its region in that object.

#[allow(dead_code)]
mod batch;
#[allow(dead_code)]
mod catalog;
#[allow(dead_code)]
mod format;
#[allow(dead_code)]
mod io;

#[allow(dead_code)]
mod store;

#[allow(unused_imports)]
pub(crate) use batch::entry_id;
#[allow(unused_imports)]
pub(crate) use store::ObjectStoreLogStore;
