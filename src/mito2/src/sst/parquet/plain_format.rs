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

//! Format to store in parquet.
//!
//! We store two additional internal columns at last:
//! - `__sequence`, the sequence number of a row. Type: uint64
//! - `__op_type`, the op type of the row. Type: uint8
//!
//! We store other columns in the same order as [RegionMetadata::field_columns()](store_api::metadata::RegionMetadata::field_columns()).
//!

/// Number of columns that have fixed positions.
///
/// Contains all internal columns.
pub(crate) const PLAIN_FIXED_POS_COLUMN_NUM: usize = 2;
