pub const SYSTEM_CATALOG_NAME: &str = "system";
pub const INFORMATION_SCHEMA_NAME: &str = "information_schema";
pub const SYSTEM_CATALOG_TABLE_NAME: &str = "system_catalog";
pub const DEFAULT_CATALOG_NAME: &str = "greptime";
pub const DEFAULT_SCHEMA_NAME: &str = "public";

/// Reserves [0,MIN_USER_TABLE_ID) for internal usage.
/// User defined table id starts from this value.
pub const MIN_USER_TABLE_ID: u32 = 1024;
/// system_catalog table id
pub const SYSTEM_CATALOG_TABLE_ID: u32 = 0;
/// scripts table id
pub const SCRIPTS_TABLE_ID: u32 = 1;

pub(crate) const CATALOG_KEY_PREFIX: &str = "__c";
pub(crate) const SCHEMA_KEY_PREFIX: &str = "__s";
pub(crate) const TABLE_GLOBAL_KEY_PREFIX: &str = "__tg";
pub(crate) const TABLE_REGIONAL_KEY_PREFIX: &str = "__tr";
pub const TABLE_ID_KEY_PREFIX: &str = "__tid";
