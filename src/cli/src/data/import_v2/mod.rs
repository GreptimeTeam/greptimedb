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

//! Import V2 module.
//!
//! This module provides the V2 implementation of database import functionality,
//! featuring:
//! - JSON-based schema import
//! - DDL generation from schema definitions
//! - Dry-run mode for verification
//!
//! # Example
//!
//! ```bash
//! # Dry-run import (verify without executing)
//! greptime cli data import-v2 \
//!   --addr 127.0.0.1:4000 \
//!   --from file:///tmp/snapshot \
//!   --dry-run
//!
//! # Actual import
//! greptime cli data import-v2 \
//!   --addr 127.0.0.1:4000 \
//!   --from s3://bucket/snapshots/prod-20250101
//! ```

mod command;
pub mod ddl_generator;
pub mod error;
pub mod executor;

pub use command::ImportV2Command;
