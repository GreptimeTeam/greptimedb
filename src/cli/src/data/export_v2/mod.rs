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

//! Export V2 module.
//!
//! This module provides the V2 implementation of database export functionality,
//! featuring:
//! - JSON-based schema export (version-agnostic)
//! - Manifest-based snapshot management
//! - Support for multiple storage backends (S3, OSS, GCS, Azure Blob, local FS)
//! - Resume capability for interrupted exports
//!
//! # Example
//!
//! ```bash
//! # Export schema only
//! greptime cli data export-v2 create \
//!   --addr 127.0.0.1:4000 \
//!   --to file:///tmp/snapshot \
//!   --schema-only
//!
//! # Export with time range (M2)
//! greptime cli data export-v2 create \
//!   --addr 127.0.0.1:4000 \
//!   --to s3://bucket/snapshots/prod-20250101 \
//!   --start-time 2025-01-01T00:00:00Z \
//!   --end-time 2025-01-31T23:59:59Z
//! ```

mod command;
pub mod error;
pub mod extractor;
pub mod manifest;
pub mod schema;
pub use command::ExportV2Command;

#[cfg(test)]
mod tests;

pub use super::snapshot_storage as storage;
