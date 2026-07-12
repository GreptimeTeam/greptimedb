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

//! Constants.

use crate::storage::descriptors::ColumnId;

// ---------- Reserved column ids ----------------------------------------------

// The reserved column id is too large to be defined as enum value (denied by the
// `clippy::enum_clike_unportable_variant` lint). So we add this enum as offset
// in ReservedColumnId to get the final column id.
enum ReservedColumnType {
    Version = 0,
    Sequence,
    OpType,
    Tsid,
    TableId,
}

/// Column id reserved by the engine.
///
/// All reserved column id has MSB (Most Significant Bit) set to 1.
///
/// Reserved column includes version column and other internal columns.
pub struct ReservedColumnId;

impl ReservedColumnId {
    // Set MSB to 1.
    const BASE: ColumnId = 1 << (ColumnId::BITS - 1);

    /// Column id for version column.
    /// Version column is a special reserved column that is enabled by user and
    /// visible to user.
    pub const fn version() -> ColumnId {
        Self::BASE | ReservedColumnType::Version as ColumnId
    }

    /// Id for `__sequence` column.
    pub const fn sequence() -> ColumnId {
        Self::BASE | ReservedColumnType::Sequence as ColumnId
    }

    /// Id for `__op_type` column.
    pub const fn op_type() -> ColumnId {
        Self::BASE | ReservedColumnType::OpType as ColumnId
    }

    /// Id for storing TSID column.
    ///
    /// Used by: metric engine
    pub const fn tsid() -> ColumnId {
        Self::BASE | ReservedColumnType::Tsid as ColumnId
    }

    /// Id for storing logical table id column.
    ///
    /// Used by: metric engine
    pub const fn table_id() -> ColumnId {
        Self::BASE | ReservedColumnType::TableId as ColumnId
    }

    /// Test if the column id is reserved.
    pub fn is_reserved(column_id: ColumnId) -> bool {
        column_id & Self::BASE != 0
    }
}

// -----------------------------------------------------------------------------

// ---------- Names reserved for internal columns and engine -------------------

/// Name for reserved column: sequence
pub const SEQUENCE_COLUMN_NAME: &str = "__sequence";

/// Name for reserved column: op_type
pub const OP_TYPE_COLUMN_NAME: &str = "__op_type";

/// Name for reserved column: primary_key
pub const PRIMARY_KEY_COLUMN_NAME: &str = "__primary_key";

/// Internal Column Name
static INTERNAL_COLUMN_VEC: [&str; 3] = [
    SEQUENCE_COLUMN_NAME,
    OP_TYPE_COLUMN_NAME,
    PRIMARY_KEY_COLUMN_NAME,
];

pub fn is_internal_column(name: &str) -> bool {
    INTERNAL_COLUMN_VEC.contains(&name)
}

// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reserved_id() {
        assert_eq!(0x80000000, ReservedColumnId::version());
        assert_eq!(0x80000001, ReservedColumnId::sequence());
        assert_eq!(0x80000002, ReservedColumnId::op_type());
    }

    #[test]
    fn test_is_internal_column() {
        // contain internal column names
        assert!(is_internal_column(SEQUENCE_COLUMN_NAME));
        assert!(is_internal_column(OP_TYPE_COLUMN_NAME));
        assert!(is_internal_column(PRIMARY_KEY_COLUMN_NAME));

        // don't contain internal column names
        assert!(!is_internal_column("my__column"));
        assert!(!is_internal_column("my__sequence"));
        assert!(!is_internal_column("my__op_type"));
        assert!(!is_internal_column("my__primary_key"));
    }
}
