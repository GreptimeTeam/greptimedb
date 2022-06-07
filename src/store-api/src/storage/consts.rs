//! Constants.

use crate::storage::descriptors::{ColumnFamilyId, ColumnId};

// Ids reserved for internal column families:

/// Column family Id for row key columns.
///
/// This is virtual column family, actually row key columns are not
/// stored in any column family.
pub const KEY_CF_ID: ColumnFamilyId = 0;
/// Id for default column family.
pub const DEFAULT_CF_ID: ColumnFamilyId = 1;

// Ids reserved for internal columns:

// TODO(yingwen): Reserve one bit for internal columns.
/// Column id for version column.
pub const VERSION_COLUMN_ID: ColumnId = 1;

// Names reserved for internal columns:

/// Name of version column.
pub const VERSION_COLUMN_NAME: &str = "__version";

// Names for default column family.
pub const DEFAULT_CF_NAME: &str = "default";
