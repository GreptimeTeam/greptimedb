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

//! # Format specification for Puffin files
//!
//! ## File structure
//!
//! Magic Blob₁ Blob₂ ... Blobₙ Footer
//!
//! - `Magic` is four bytes 0x50, 0x46, 0x41, 0x31 (short for: Puffin Fratercula arctica, version 1),
//! - `Blobᵢ` is i-th blob contained in the file, to be interpreted by application according to the footer,
//! - `Footer` is defined below.
//!
//! ## Footer structure
//!
//! Magic FooterPayload FooterPayloadSize Flags Magic
//!
//! - `Magic`: four bytes, same as at the beginning of the file
//! - `FooterPayload`: optionally compressed, UTF-8 encoded JSON payload describing the blobs in the file, with the structure described below
//! - `FooterPayloadSize`: a length in bytes of the `FooterPayload` (after compression, if compressed), stored as 4 byte integer
//! - `Flags`: 4 bytes for boolean flags
//!   * byte 0 (first)
//!     - bit 0 (lowest bit): whether `FooterPayload` is compressed
//!     - all other bits are reserved for future use and should be set to 0 on write
//!   * all other bytes are reserved for future use and should be set to 0 on write
//! A 4 byte integer is always signed, in a two’s complement representation, stored little-endian.
//!
//! ## Footer Payload
//!
//! Footer payload bytes is either uncompressed or LZ4-compressed (as a single LZ4 compression frame with content size present),
//! UTF-8 encoded JSON payload representing a single [`FileMetadata`] object.
//!
//! [`FileMetadata`]: ../file_metadata/struct.FileMetadata.html

pub mod reader;
pub mod writer;

use bitflags::bitflags;

pub const MAGIC: [u8; 4] = [0x50, 0x46, 0x41, 0x31];

pub const MAGIC_SIZE: u64 = MAGIC.len() as u64;
pub const MIN_FILE_SIZE: u64 = MAGIC_SIZE + MIN_FOOTER_SIZE;
pub const FLAGS_SIZE: u64 = 4;
pub const PAYLOAD_SIZE_SIZE: u64 = 4;
pub const MIN_FOOTER_SIZE: u64 = MAGIC_SIZE * 2 + FLAGS_SIZE + PAYLOAD_SIZE_SIZE;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Flags: u32 {
        const DEFAULT = 0b00000000;

        const FOOTER_PAYLOAD_COMPRESSED_LZ4 = 0b00000001;
    }
}
