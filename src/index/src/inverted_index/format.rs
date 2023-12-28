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

//! # SST Files with Inverted Index Format Specification
//!
//! ## File Structure
//!
//! Each SST file includes a series of inverted indices followed by a footer.
//!
//! `inverted_index₀ inverted_index₁ ... inverted_indexₙ footer`
//!
//! - Each `inverted_indexᵢ` represents an index entry corresponding to tag values and their locations within the file.
//! - `footer`: Contains metadata about the inverted indices, encoded as a protobuf message.
//!
//! ## Inverted Index Internals
//!
//! An inverted index comprises a collection of bitmaps, a null bitmap, and a finite state transducer (FST) indicating tag values' positions:
//!
//! `null_bitmap bitmap₀ bitmap₁ bitmap₂ ... bitmapₙ fst`
//!
//! - `null_bitmap`: Bitset tracking the presence of null values within the tag column.
//! - `bitmapᵢ`: Bitset indicating the presence of tag values within a row group.
//! - `fst`: Finite State Transducer providing an ordered map of bytes, representing the tag values.
//!
//! ## Footer Details
//!
//! The footer encapsulates the metadata for inversion mappings:
//!
//! `footer_payload footer_payload_size`
//!
//! - `footer_payload`: Protobuf-encoded [`InvertedIndexMetas`] describing the metadata of each inverted index.
//! - `footer_payload_size`: Size in bytes of the `footer_payload`, displayed as a `u32` integer.
//! - The footer aids in the interpretation of the inverted indices, providing necessary offset and count information.
//!
//! ## Reference
//!
//! More detailed information regarding the encoding of the inverted indices can be found in the [RFC].
//!
//! [`InvertedIndexMetas`]: https://github.com/GreptimeTeam/greptime-proto/blob/2aaee38de81047537dfa42af9df63bcfb866e06c/proto/greptime/v1/index/inverted_index.proto#L32-L64
//! [RFC]: https://github.com/GreptimeTeam/greptimedb/blob/main/docs/rfcs/2023-11-03-inverted-index.md

pub mod reader;
pub mod writer;

const FOOTER_PAYLOAD_SIZE_SIZE: u64 = 4;
const MIN_BLOB_SIZE: u64 = FOOTER_PAYLOAD_SIZE_SIZE;
