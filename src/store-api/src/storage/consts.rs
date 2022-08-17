//! Constants.

use crate::storage::descriptors::{ColumnFamilyId, ColumnId};

// ---------- Reserved column family ids ---------------------------------------

/// Column family Id for row key columns.
///
/// This is a virtual column family, actually row key columns are not
/// stored in any column family.
pub const KEY_CF_ID: ColumnFamilyId = 0;
/// Id for default column family.
pub const DEFAULT_CF_ID: ColumnFamilyId = 1;

// -----------------------------------------------------------------------------

// ---------- Reserved column ids ----------------------------------------------

// The reserved column id is too large to be defined as enum value (denied by the
// `clippy::enum_clike_unportable_variant` lint). So we add this enum as offset
// in ReservedColumnId to get the final column id.
enum ReservedColumnType {
    Version = 0,
    Sequence,
    ValueType,
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

    /// Id for `__value_type` column.
    pub const fn value_type() -> ColumnId {
        Self::BASE | ReservedColumnType::ValueType as ColumnId
    }
}

// -----------------------------------------------------------------------------

// ---------- Names reserved for internal columns and engine -------------------

/// Name of version column.
pub const VERSION_COLUMN_NAME: &str = "__version";

/// Names for default column family.
pub const DEFAULT_CF_NAME: &str = "default";

/// Name for reserved column: sequence
pub const SEQUENCE_COLUMN_NAME: &str = "__sequence";

/// Name for time index constraint name.
pub const TIME_INDEX_NAME: &str = "__time_index";

// TODO(yingwen): `__op_type` might be proper than `__value_type`.
/// Name for reserved column: value_type
pub const VALUE_TYPE_COLUMN_NAME: &str = "__value_type";

// -----------------------------------------------------------------------------

// ---------- Default options --------------------------------------------------

pub const READ_BATCH_SIZE: usize = 256;

// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reserved_id() {
        assert_eq!(0x80000000, ReservedColumnId::version());
        assert_eq!(0x80000001, ReservedColumnId::sequence());
        assert_eq!(0x80000002, ReservedColumnId::value_type());
    }
}
