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

//! Table and TableEngine requests

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use common_base::readable_size::ReadableSize;
use common_datasource::object_store::oss::is_supported_in_oss;
use common_datasource::object_store::s3::is_supported_in_s3;
use common_query::AddColumnLocation;
use common_time::TimeToLive;
use common_time::range::TimestampRange;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::VectorRef;
use datatypes::schema::{
    ColumnDefaultConstraint, ColumnSchema, FulltextOptions, SkippingIndexOptions,
};
use greptime_proto::v1::region::compact_request;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use store_api::metric_engine_consts::{
    LOGICAL_TABLE_METADATA_KEY, PHYSICAL_TABLE_METADATA_KEY, is_metric_engine_option_key,
};
use store_api::mito_engine_options::{
    APPEND_MODE_KEY, COMPACTION_TYPE, MEMTABLE_TYPE, MERGE_MODE_KEY, TWCS_FALLBACK_TO_LOCAL,
    TWCS_MAX_OUTPUT_FILE_SIZE, TWCS_TIME_WINDOW, TWCS_TRIGGER_FILE_NUM, is_mito_engine_option_key,
};
use store_api::region_request::{SetRegionOption, UnsetRegionOption};

use crate::error::{ParseTableOptionSnafu, Result};
use crate::metadata::{TableId, TableVersion};
use crate::table_reference::TableReference;

pub const FILE_TABLE_META_KEY: &str = "__private.file_table_meta";
pub const FILE_TABLE_LOCATION_KEY: &str = "location";
pub const FILE_TABLE_PATTERN_KEY: &str = "pattern";
pub const FILE_TABLE_FORMAT_KEY: &str = "format";

pub const TABLE_DATA_MODEL: &str = "table_data_model";
pub const TABLE_DATA_MODEL_TRACE_V1: &str = "greptime_trace_v1";

pub const OTLP_METRIC_COMPAT_KEY: &str = "otlp_metric_compat";
pub const OTLP_METRIC_COMPAT_PROM: &str = "prom";

pub const VALID_TABLE_OPTION_KEYS: [&str; 12] = [
    // common keys:
    WRITE_BUFFER_SIZE_KEY,
    TTL_KEY,
    STORAGE_KEY,
    COMMENT_KEY,
    SKIP_WAL_KEY,
    // file engine keys:
    FILE_TABLE_LOCATION_KEY,
    FILE_TABLE_FORMAT_KEY,
    FILE_TABLE_PATTERN_KEY,
    // metric engine keys:
    PHYSICAL_TABLE_METADATA_KEY,
    LOGICAL_TABLE_METADATA_KEY,
    // table model info
    TABLE_DATA_MODEL,
    OTLP_METRIC_COMPAT_KEY,
];

// Valid option keys when creating a db.
static VALID_DB_OPT_KEYS: Lazy<HashSet<&str>> = Lazy::new(|| {
    let mut set = HashSet::new();
    set.insert(TTL_KEY);
    set.insert(STORAGE_KEY);
    set.insert(MEMTABLE_TYPE);
    set.insert(APPEND_MODE_KEY);
    set.insert(MERGE_MODE_KEY);
    set.insert(SKIP_WAL_KEY);
    set.insert(COMPACTION_TYPE);
    set.insert(TWCS_FALLBACK_TO_LOCAL);
    set.insert(TWCS_TIME_WINDOW);
    set.insert(TWCS_TRIGGER_FILE_NUM);
    set.insert(TWCS_MAX_OUTPUT_FILE_SIZE);
    set
});

/// Returns true if the `key` is a valid key for database.
pub fn validate_database_option(key: &str) -> bool {
    VALID_DB_OPT_KEYS.contains(&key)
}

/// Returns true if the `key` is a valid key for any engine or storage.
pub fn validate_table_option(key: &str) -> bool {
    if is_supported_in_s3(key) {
        return true;
    }

    if is_supported_in_oss(key) {
        return true;
    }

    if is_mito_engine_option_key(key) {
        return true;
    }

    if is_metric_engine_option_key(key) {
        return true;
    }

    VALID_TABLE_OPTION_KEYS.contains(&key)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TableOptions {
    /// Memtable size of memtable.
    pub write_buffer_size: Option<ReadableSize>,
    /// Time-to-live of table. Expired data will be automatically purged.
    pub ttl: Option<TimeToLive>,
    /// Skip wal write for this table.
    pub skip_wal: bool,
    /// Extra options that may not applicable to all table engines.
    pub extra_options: HashMap<String, String>,
}

pub const WRITE_BUFFER_SIZE_KEY: &str = "write_buffer_size";
pub const TTL_KEY: &str = store_api::mito_engine_options::TTL_KEY;
pub const STORAGE_KEY: &str = "storage";
pub const COMMENT_KEY: &str = "comment";
pub const AUTO_CREATE_TABLE_KEY: &str = "auto_create_table";
pub const SKIP_WAL_KEY: &str = store_api::mito_engine_options::SKIP_WAL_KEY;

impl TableOptions {
    pub fn try_from_iter<T: ToString, U: IntoIterator<Item = (T, T)>>(
        iter: U,
    ) -> Result<TableOptions> {
        let mut options = TableOptions::default();

        let kvs: HashMap<String, String> = iter
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        if let Some(write_buffer_size) = kvs.get(WRITE_BUFFER_SIZE_KEY) {
            let size = ReadableSize::from_str(write_buffer_size).map_err(|_| {
                ParseTableOptionSnafu {
                    key: WRITE_BUFFER_SIZE_KEY,
                    value: write_buffer_size,
                }
                .build()
            })?;
            options.write_buffer_size = Some(size)
        }

        if let Some(ttl) = kvs.get(TTL_KEY) {
            let ttl_value = TimeToLive::from_humantime_or_str(ttl).map_err(|_| {
                ParseTableOptionSnafu {
                    key: TTL_KEY,
                    value: ttl,
                }
                .build()
            })?;
            options.ttl = Some(ttl_value);
        }

        if let Some(skip_wal) = kvs.get(SKIP_WAL_KEY) {
            options.skip_wal = skip_wal.parse().map_err(|_| {
                ParseTableOptionSnafu {
                    key: SKIP_WAL_KEY,
                    value: skip_wal,
                }
                .build()
            })?;
        }

        options.extra_options = HashMap::from_iter(
            kvs.into_iter()
                .filter(|(k, _)| k != WRITE_BUFFER_SIZE_KEY && k != TTL_KEY),
        );

        Ok(options)
    }
}

impl fmt::Display for TableOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut key_vals = vec![];
        if let Some(size) = self.write_buffer_size {
            key_vals.push(format!("{}={}", WRITE_BUFFER_SIZE_KEY, size));
        }

        if let Some(ttl) = self.ttl.map(|ttl| ttl.to_string()) {
            key_vals.push(format!("{}={}", TTL_KEY, ttl));
        }

        if self.skip_wal {
            key_vals.push(format!("{}={}", SKIP_WAL_KEY, self.skip_wal));
        }

        for (k, v) in &self.extra_options {
            key_vals.push(format!("{}={}", k, v));
        }

        write!(f, "{}", key_vals.join(" "))
    }
}

impl From<&TableOptions> for HashMap<String, String> {
    fn from(opts: &TableOptions) -> Self {
        let mut res = HashMap::with_capacity(2 + opts.extra_options.len());
        if let Some(write_buffer_size) = opts.write_buffer_size {
            let _ = res.insert(
                WRITE_BUFFER_SIZE_KEY.to_string(),
                write_buffer_size.to_string(),
            );
        }
        if let Some(ttl_str) = opts.ttl.map(|ttl| ttl.to_string()) {
            let _ = res.insert(TTL_KEY.to_string(), ttl_str);
        }
        res.extend(
            opts.extra_options
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        res
    }
}

/// Alter table request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterTableRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    pub alter_kind: AlterKind,
    // None in standalone.
    pub table_version: Option<TableVersion>,
}

/// Add column request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddColumnRequest {
    pub column_schema: ColumnSchema,
    pub is_key: bool,
    pub location: Option<AddColumnLocation>,
    /// Add column if not exists.
    pub add_if_not_exists: bool,
}

/// Change column datatype request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyColumnTypeRequest {
    pub column_name: String,
    pub target_type: ConcreteDataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterKind {
    AddColumns {
        columns: Vec<AddColumnRequest>,
    },
    DropColumns {
        names: Vec<String>,
    },
    ModifyColumnTypes {
        columns: Vec<ModifyColumnTypeRequest>,
    },
    RenameTable {
        new_table_name: String,
    },
    SetTableOptions {
        options: Vec<SetRegionOption>,
    },
    UnsetTableOptions {
        keys: Vec<UnsetRegionOption>,
    },
    SetIndexes {
        options: Vec<SetIndexOption>,
    },
    UnsetIndexes {
        options: Vec<UnsetIndexOption>,
    },
    DropDefaults {
        names: Vec<String>,
    },
    SetDefaults {
        defaults: Vec<SetDefaultRequest>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetDefaultRequest {
    pub column_name: String,
    pub default_constraint: Option<ColumnDefaultConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SetIndexOption {
    Fulltext {
        column_name: String,
        options: FulltextOptions,
    },
    Inverted {
        column_name: String,
    },
    Skipping {
        column_name: String,
        options: SkippingIndexOptions,
    },
}

impl SetIndexOption {
    /// Returns the column name of the index option.
    pub fn column_name(&self) -> &str {
        match self {
            SetIndexOption::Fulltext { column_name, .. } => column_name,
            SetIndexOption::Inverted { column_name, .. } => column_name,
            SetIndexOption::Skipping { column_name, .. } => column_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnsetIndexOption {
    Fulltext { column_name: String },
    Inverted { column_name: String },
    Skipping { column_name: String },
}

impl UnsetIndexOption {
    /// Returns the column name of the index option.
    pub fn column_name(&self) -> &str {
        match self {
            UnsetIndexOption::Fulltext { column_name, .. } => column_name,
            UnsetIndexOption::Inverted { column_name, .. } => column_name,
            UnsetIndexOption::Skipping { column_name, .. } => column_name,
        }
    }
}

#[derive(Debug)]
pub struct InsertRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub columns_values: HashMap<String, VectorRef>,
}

/// Delete (by primary key) request
#[derive(Debug)]
pub struct DeleteRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    /// Values of each column in this table's primary key and time index.
    ///
    /// The key is the column name, and the value is the column value.
    pub key_column_values: HashMap<String, VectorRef>,
}

#[derive(Debug)]
pub enum CopyDirection {
    Export,
    Import,
}

/// Copy table request
#[derive(Debug)]
pub struct CopyTableRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub location: String,
    pub with: HashMap<String, String>,
    pub connection: HashMap<String, String>,
    pub pattern: Option<String>,
    pub direction: CopyDirection,
    pub timestamp_range: Option<TimestampRange>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct FlushTableRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct BuildIndexTableRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompactTableRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub compact_options: compact_request::Options,
    pub parallelism: u32,
}

impl Default for CompactTableRequest {
    fn default() -> Self {
        Self {
            catalog_name: Default::default(),
            schema_name: Default::default(),
            table_name: Default::default(),
            compact_options: compact_request::Options::Regular(Default::default()),
            parallelism: 1,
        }
    }
}

/// Truncate table request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruncateTableRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
}

impl TruncateTableRequest {
    pub fn table_ref(&self) -> TableReference<'_> {
        TableReference {
            catalog: &self.catalog_name,
            schema: &self.schema_name,
            table: &self.table_name,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CopyDatabaseRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub location: String,
    pub with: HashMap<String, String>,
    pub connection: HashMap<String, String>,
    pub time_range: Option<TimestampRange>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CopyQueryToRequest {
    pub location: String,
    pub with: HashMap<String, String>,
    pub connection: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_validate_table_option() {
        assert!(validate_table_option(FILE_TABLE_LOCATION_KEY));
        assert!(validate_table_option(FILE_TABLE_FORMAT_KEY));
        assert!(validate_table_option(FILE_TABLE_PATTERN_KEY));
        assert!(validate_table_option(TTL_KEY));
        assert!(validate_table_option(WRITE_BUFFER_SIZE_KEY));
        assert!(validate_table_option(STORAGE_KEY));
        assert!(!validate_table_option("foo"));
    }

    #[test]
    fn test_serialize_table_options() {
        let options = TableOptions {
            write_buffer_size: None,
            ttl: Some(Duration::from_secs(1000).into()),
            extra_options: HashMap::new(),
            skip_wal: false,
        };
        let serialized = serde_json::to_string(&options).unwrap();
        let deserialized: TableOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(options, deserialized);
    }

    #[test]
    fn test_convert_hashmap_between_table_options() {
        let options = TableOptions {
            write_buffer_size: Some(ReadableSize::mb(128)),
            ttl: Some(Duration::from_secs(1000).into()),
            extra_options: HashMap::new(),
            skip_wal: false,
        };
        let serialized_map = HashMap::from(&options);
        let serialized = TableOptions::try_from_iter(&serialized_map).unwrap();
        assert_eq!(options, serialized);

        let options = TableOptions {
            write_buffer_size: None,
            ttl: Default::default(),
            extra_options: HashMap::new(),
            skip_wal: false,
        };
        let serialized_map = HashMap::from(&options);
        let serialized = TableOptions::try_from_iter(&serialized_map).unwrap();
        assert_eq!(options, serialized);

        let options = TableOptions {
            write_buffer_size: Some(ReadableSize::mb(128)),
            ttl: Some(Duration::from_secs(1000).into()),
            extra_options: HashMap::from([("a".to_string(), "A".to_string())]),
            skip_wal: false,
        };
        let serialized_map = HashMap::from(&options);
        let serialized = TableOptions::try_from_iter(&serialized_map).unwrap();
        assert_eq!(options, serialized);
    }

    #[test]
    fn test_table_options_to_string() {
        let options = TableOptions {
            write_buffer_size: Some(ReadableSize::mb(128)),
            ttl: Some(Duration::from_secs(1000).into()),
            extra_options: HashMap::new(),
            skip_wal: false,
        };

        assert_eq!(
            "write_buffer_size=128.0MiB ttl=16m 40s",
            options.to_string()
        );

        let options = TableOptions {
            write_buffer_size: Some(ReadableSize::mb(128)),
            ttl: Some(Duration::from_secs(1000).into()),
            extra_options: HashMap::from([("a".to_string(), "A".to_string())]),
            skip_wal: false,
        };

        assert_eq!(
            "write_buffer_size=128.0MiB ttl=16m 40s a=A",
            options.to_string()
        );

        let options = TableOptions {
            write_buffer_size: Some(ReadableSize::mb(128)),
            ttl: Some(Duration::from_secs(1000).into()),
            extra_options: HashMap::new(),
            skip_wal: true,
        };
        assert_eq!(
            "write_buffer_size=128.0MiB ttl=16m 40s skip_wal=true",
            options.to_string()
        );
    }
}
