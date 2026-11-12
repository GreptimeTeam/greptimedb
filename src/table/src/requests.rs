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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use common_base::readable_size::ReadableSize;
use common_datasource::object_store::oss::is_supported_in_oss;
use common_datasource::object_store::s3::is_supported_in_s3;
use common_query::AddColumnLocation;
use common_time::TimeToLive;
use common_time::range::TimestampRange;
use datatypes::data_type::ConcreteDataType;
use datatypes::json::JsonSettings;
use datatypes::prelude::VectorRef;
use datatypes::schema::{
    ColumnDefaultConstraint, ColumnSchema, FulltextOptions, Schema, SkippingIndexOptions,
};
use greptime_proto::v1::region::{build_index_request, compact_request};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use store_api::metric_engine_consts::{
    LOGICAL_TABLE_METADATA_KEY, PHYSICAL_TABLE_METADATA_KEY, is_metric_engine_option_key,
};
use store_api::mito_engine_options::{
    APPEND_MODE_KEY, COMPACTION_TYPE, EXPERIMENTAL_SST_FLOAT_FIELD_ENCODING, FloatFieldEncoding,
    MEMTABLE_BULK_ENCODE_BYTES_THRESHOLD, MEMTABLE_BULK_ENCODE_ROW_THRESHOLD,
    MEMTABLE_BULK_MAX_MERGE_GROUPS, MEMTABLE_BULK_MERGE_THRESHOLD, MEMTABLE_TYPE, MERGE_MODE_KEY,
    SST_FORMAT_KEY, TWCS_ACTIVE_WINDOW_L1_MERGE_TRIGGER, TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM,
    TWCS_FALLBACK_TO_LOCAL, TWCS_INACTIVE_WINDOW_L1_MERGE_TRIGGER,
    TWCS_INACTIVE_WINDOW_TRIGGER_FILE_NUM, TWCS_MAX_OUTPUT_FILE_SIZE, TWCS_TIME_WINDOW,
    TWCS_TRIGGER_FILE_NUM, is_mito_engine_option_key, normalize_twcs_trigger_options,
};
use store_api::region_request::{SetRegionOption, UnsetRegionOption};

use crate::error::{ConflictingTableOptionsSnafu, ParseTableOptionSnafu, Result};
use crate::metadata::{TableId, TableVersion};

mod semantic;
pub use semantic::*;

pub const FILE_TABLE_META_KEY: &str = "__private.file_table_meta";
pub const RELATIONAL_META_KEY: &str = "__private.relational_meta";
pub const FILE_TABLE_LOCATION_KEY: &str = "location";
pub const FILE_TABLE_PATTERN_KEY: &str = "pattern";
pub const FILE_TABLE_FORMAT_KEY: &str = "format";
pub const RELATIONAL_CONNECTION_KEY: &str = "connection";
pub const RELATIONAL_REMOTE_TABLE_KEY: &str = "remote_table";

/// Options captured at `CREATE EXTERNAL TABLE ... ENGINE=relational WITH (...)` time
/// and persisted in the region manifest.
///
/// `connection` is persisted in **plaintext** so the datanode can reconnect to the
/// remote database when serving queries on the region.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationalOptions {
    /// Full DSN, e.g. `mysql://user:password@host:3306/db`.
    pub connection: String,
    pub remote_table: String,
}

impl RelationalOptions {
    /// Returns a copy with the password in `connection` replaced by
    /// `REDACTED`, safe to surface in error messages and logs. The persisted
    /// manifest keeps the plaintext `connection` so the datanode can reconnect
    /// to the remote database when serving queries.
    pub fn redacted(&self) -> Self {
        Self {
            connection: redact_connection(&self.connection),
            remote_table: self.remote_table.clone(),
        }
    }
}

/// Redacts the password of a connection DSN, preserving the other fields
/// (scheme, host, port, user, database). On a parse failure the whole string is
/// replaced with `REDACTED` (best-effort).
pub fn redact_connection(connection: &str) -> String {
    const REDACTED: &str = "REDACTED";
    match url::Url::parse(connection) {
        Ok(mut url) => {
            if url.password().is_some() {
                let _ = url.set_password(Some(REDACTED));
            }
            url.to_string()
        }
        Err(_) => REDACTED.to_string(),
    }
}

/// Redacts passwords of connection DSNs embedded in arbitrary text such as raw
/// SQL, parser error messages, or token dumps. Text without a `scheme://` DSN is
/// returned unchanged.
pub fn redact_connection_in_text(text: &str) -> String {
    const REDACTED: &str = "REDACTED";
    let mut output = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match find_subslice(bytes, index, b"://") {
            Some(scheme_end) => {
                let mut start = scheme_end;
                while start > index && is_scheme_char(bytes[start - 1]) {
                    start -= 1;
                }
                output.push_str(&text[index..start]);

                let host_start = scheme_end + 3;
                let end = find_connection_end(bytes, host_start);
                let authority_end = bytes[host_start..end]
                    .iter()
                    .position(|&b| matches!(b, b'/' | b'?' | b'#'))
                    .map(|p| host_start + p)
                    .unwrap_or(end);
                let at = bytes[host_start..authority_end]
                    .iter()
                    .rposition(|&b| b == b'@')
                    .map(|p| host_start + p);

                match at.and_then(|at| {
                    bytes[host_start..at]
                        .iter()
                        .rposition(|&b| b == b':')
                        .map(|colon| (at, host_start + colon))
                }) {
                    Some((at, colon)) => {
                        output.push_str(&text[start..=colon]);
                        output.push_str(REDACTED);
                        output.push_str(&text[at..end]);
                    }
                    None => output.push_str(&text[start..end]),
                }
                index = end;
            }
            None => {
                output.push_str(&text[index..]);
                break;
            }
        }
    }
    output
}

fn is_scheme_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'+' | b'-' | b'.')
}

fn find_subslice(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack[from..]
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|pos| from + pos)
}

fn find_connection_end(bytes: &[u8], from: usize) -> usize {
    let mut end = from;
    while end < bytes.len() {
        let c = bytes[end];
        if c.is_ascii_whitespace()
            || matches!(
                c,
                b'\'' | b'"' | b',' | b';' | b'(' | b')' | b'<' | b'>' | b'`'
            )
        {
            break;
        }
        end += 1;
    }
    end
}

pub fn redact_relational_options(encoded: &str) -> String {
    serde_json::from_str::<RelationalOptions>(encoded)
        .ok()
        .and_then(|opts| serde_json::to_string(&opts.redacted()).ok())
        .unwrap_or_else(|| "REDACTED".to_string())
}

impl std::fmt::Debug for RelationalOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationalOptions")
            .field("connection", &redact_connection(&self.connection))
            .field("remote_table", &self.remote_table)
            .finish()
    }
}

pub const TABLE_DATA_MODEL: &str = "table_data_model";
pub const TABLE_DATA_MODEL_TRACE_V1: &str = "greptime_trace_v1";
/// Table data model used by the JSON2-based OTLP trace pipeline.
pub const TABLE_DATA_MODEL_TRACE_V2: &str = "greptime_trace_v2";
/// Table data model of OTLP log tables auto-created with the v1 built-in schema.
/// Log tables without this marker keep the legacy built-in schema.
pub const TABLE_DATA_MODEL_LOG_V1: &str = "greptime_log_v1";

/// Returns true for the Trace V1 and V2 data models supported
/// by semantic graph derivation.
pub fn is_trace_table(table_info: &crate::metadata::TableInfo) -> bool {
    let table_data_model = table_info.meta.options.data_model();
    matches!(
        table_data_model,
        Some(TABLE_DATA_MODEL_TRACE_V1 | TABLE_DATA_MODEL_TRACE_V2)
    )
}

/// Retained table metadata; OTLP metrics always use Prometheus-compatible conversion.
pub const OTLP_METRIC_COMPAT_KEY: &str = "otlp_metric_compat";
pub const OTLP_METRIC_COMPAT_PROM: &str = "prom";

pub const VALID_TABLE_OPTION_KEYS: [&str; 17] = [
    // common keys:
    WRITE_BUFFER_SIZE_KEY,
    TTL_KEY,
    STORAGE_KEY,
    COMMENT_KEY,
    SKIP_WAL_KEY,
    SST_FORMAT_KEY,
    // file engine keys:
    FILE_TABLE_LOCATION_KEY,
    FILE_TABLE_FORMAT_KEY,
    FILE_TABLE_PATTERN_KEY,
    // relational engine keys:
    RELATIONAL_CONNECTION_KEY,
    RELATIONAL_REMOTE_TABLE_KEY,
    // metric engine keys:
    PHYSICAL_TABLE_METADATA_KEY,
    LOGICAL_TABLE_METADATA_KEY,
    // table model info
    TABLE_DATA_MODEL,
    OTLP_METRIC_COMPAT_KEY,
    REPARTITION_COLUMN_HINT_KEY,
    REPARTITION_PARTITION_NUM_HINT_KEY,
];

pub const DDL_TIMEOUT: &str = "timeout";
pub const DDL_WAIT: &str = "wait";

pub const VALID_DDL_OPTION_KEYS: [&str; 2] = [DDL_TIMEOUT, DDL_WAIT];

/// The key of ingest rows rate limit option (rows per second, cluster-wide) in database options.
pub const INGEST_ROWS_RATE_LIMIT_KEY: &str = "ingest_rows_rate_limit";

// Valid option keys when creating a db.
static VALID_DB_OPT_KEYS: Lazy<HashSet<&str>> = Lazy::new(|| {
    let mut set = HashSet::new();
    set.insert(TTL_KEY);
    set.insert(STORAGE_KEY);
    set.insert(MEMTABLE_TYPE);
    set.insert(MEMTABLE_BULK_MERGE_THRESHOLD);
    set.insert(MEMTABLE_BULK_ENCODE_ROW_THRESHOLD);
    set.insert(MEMTABLE_BULK_ENCODE_BYTES_THRESHOLD);
    set.insert(MEMTABLE_BULK_MAX_MERGE_GROUPS);
    set.insert(APPEND_MODE_KEY);
    set.insert(MERGE_MODE_KEY);
    set.insert(SKIP_WAL_KEY);
    set.insert(COMPACTION_TYPE);
    set.insert(TWCS_FALLBACK_TO_LOCAL);
    set.insert(TWCS_TIME_WINDOW);
    set.insert(TWCS_TRIGGER_FILE_NUM);
    set.insert(TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM);
    set.insert(TWCS_ACTIVE_WINDOW_L1_MERGE_TRIGGER);
    set.insert(TWCS_INACTIVE_WINDOW_TRIGGER_FILE_NUM);
    set.insert(TWCS_INACTIVE_WINDOW_L1_MERGE_TRIGGER);
    set.insert(TWCS_MAX_OUTPUT_FILE_SIZE);
    set.insert(SST_FORMAT_KEY);
    set.insert(INGEST_ROWS_RATE_LIMIT_KEY);
    set
});

/// Returns true if the `key` is a valid key for database.
pub fn validate_database_option(key: &str) -> bool {
    VALID_DB_OPT_KEYS.contains(&key)
}

/// Validates a database option value, returning the violated constraint on error.
pub fn validate_database_option_value(
    key: &str,
    value: Option<&str>,
) -> std::result::Result<(), &'static str> {
    if key == INGEST_ROWS_RATE_LIMIT_KEY {
        return value
            .and_then(|value| value.parse::<u64>().ok())
            .map(|_| ())
            .ok_or("expected a non-negative integer fitting in u64");
    }
    let (minimum, constraint) = match key {
        TWCS_TRIGGER_FILE_NUM
        | TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM
        | TWCS_INACTIVE_WINDOW_TRIGGER_FILE_NUM => {
            (0, "expected a non-negative integer fitting in usize")
        }
        TWCS_ACTIVE_WINDOW_L1_MERGE_TRIGGER | TWCS_INACTIVE_WINDOW_L1_MERGE_TRIGGER => {
            (2, "expected an integer greater than or equal to 2")
        }
        _ => return Ok(()),
    };
    if value
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|files| files >= minimum)
    {
        Ok(())
    } else {
        Err(constraint)
    }
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

    // Semantic-layer keys share a reserved prefix instead of a fixed allowlist so
    // the vocabulary can grow without touching this gate. See `semantic` module.
    if is_semantic_option_key(key) {
        return true;
    }

    VALID_TABLE_OPTION_KEYS.contains(&key) || VALID_DDL_OPTION_KEYS.contains(&key)
}

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TableOptions {
    /// Per-region write buffer stall threshold. Writes are rejected at twice this size.
    pub write_buffer_size: Option<ReadableSize>,
    /// Time-to-live of table. Expired data will be automatically purged.
    pub ttl: Option<TimeToLive>,
    /// Skip wal write for this table.
    pub skip_wal: bool,
    /// Extra options that may not applicable to all table engines.
    pub extra_options: HashMap<String, String>,
}

pub const WRITE_BUFFER_SIZE_KEY: &str = store_api::mito_engine_options::WRITE_BUFFER_SIZE_KEY;
pub const TTL_KEY: &str = store_api::mito_engine_options::TTL_KEY;
pub const STORAGE_KEY: &str = "storage";
pub const COMMENT_KEY: &str = "comment";
pub const AUTO_CREATE_TABLE_KEY: &str = "auto_create_table";
pub const SKIP_WAL_KEY: &str = store_api::mito_engine_options::SKIP_WAL_KEY;
pub const TRACE_TABLE_PARTITIONS_HINT_KEY: &str = "trace_table_partitions";
pub const REPARTITION_COLUMN_HINT_KEY: &str = "repartition.column.hint";

/// Table-level partition count hint consumed by the auto-repartition planner.
pub const REPARTITION_PARTITION_NUM_HINT_KEY: &str = "repartition.partition.num.hint";

impl TableOptions {
    /// Returns the table data model, if specified.
    pub fn data_model(&self) -> Option<&str> {
        self.extra_options.get(TABLE_DATA_MODEL).map(String::as_str)
    }

    pub fn try_from_iter<T: ToString, U: IntoIterator<Item = (T, T)>>(
        iter: U,
    ) -> Result<TableOptions> {
        let mut options = TableOptions::default();

        let mut kvs: HashMap<String, String> = iter
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        normalize_twcs_trigger_options(&mut kvs).map_err(|conflict| {
            ConflictingTableOptionsSnafu {
                first_key: TWCS_TRIGGER_FILE_NUM,
                first_value: conflict.legacy_value,
                second_key: TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM,
                second_value: conflict.canonical_value,
            }
            .build()
        })?;

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

        if let Some(encoding) = kvs.get(EXPERIMENTAL_SST_FLOAT_FIELD_ENCODING) {
            encoding.parse::<FloatFieldEncoding>().map_err(|_| {
                ParseTableOptionSnafu {
                    key: EXPERIMENTAL_SST_FLOAT_FIELD_ENCODING,
                    value: encoding,
                }
                .build()
            })?;
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

/// Option keys whose values contain credentials and must be masked when rendered.
const CREDENTIAL_OPTION_KEYS: [&str; 5] = [
    "access_key_id",
    "connection",
    "secret_access_key",
    "session_token",
    "access_key_secret",
];

/// Placeholder rendered instead of a credential value.
pub const REDACTED_OPTION_VALUE: &str = "******";

pub fn is_credential_option(key: &str) -> bool {
    CREDENTIAL_OPTION_KEYS.contains(&key)
}

/// Renders the options for users, e.g. `information_schema.tables.create_options`,
/// so credential values are masked.
impl fmt::Display for TableOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut key_vals = vec![];
        if let Some(size) = self.write_buffer_size {
            key_vals.push(format!("{}={}", WRITE_BUFFER_SIZE_KEY, size));
        }

        if let Some(ttl) = self.ttl.map(|ttl| ttl.to_string()) {
            key_vals.push(format!("{}={}", TTL_KEY, ttl));
        }

        if self.skip_wal && !self.extra_options.contains_key(SKIP_WAL_KEY) {
            key_vals.push(format!("{}={}", SKIP_WAL_KEY, self.skip_wal));
        }

        // Sort by key so `create_options` is stable across queries; `HashMap` order is not.
        let extra_options: BTreeMap<_, _> = self.extra_options.iter().collect();
        for (k, v) in extra_options {
            let value = if is_credential_option(k) {
                REDACTED_OPTION_VALUE.to_string()
            } else if k == RELATIONAL_META_KEY {
                redact_relational_options(v)
            } else {
                v.to_string()
            };
            key_vals.push(format!("{}={}", k, value));
        }

        write!(f, "{}", key_vals.join(" "))
    }
}

impl fmt::Debug for TableOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut extra_options = self.extra_options.clone();
        if let Some(encoded) = extra_options.get_mut(RELATIONAL_META_KEY) {
            *encoded = redact_relational_options(encoded);
        }
        f.debug_struct("TableOptions")
            .field("write_buffer_size", &self.write_buffer_size)
            .field("ttl", &self.ttl)
            .field("skip_wal", &self.skip_wal)
            .field("extra_options", &extra_options)
            .finish()
    }
}

impl From<&TableOptions> for HashMap<String, String> {
    fn from(opts: &TableOptions) -> Self {
        let mut res = HashMap::with_capacity(3 + opts.extra_options.len());
        if let Some(write_buffer_size) = opts.write_buffer_size {
            let _ = res.insert(
                WRITE_BUFFER_SIZE_KEY.to_string(),
                write_buffer_size.to_string(),
            );
        }
        if let Some(ttl_str) = opts.ttl.map(|ttl| ttl.to_string()) {
            let _ = res.insert(TTL_KEY.to_string(), ttl_str);
        }
        if opts.skip_wal {
            let _ = res.insert(SKIP_WAL_KEY.to_string(), true.to_string());
        }
        res.extend(opts.extra_options.clone());
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

/// Set JSON2 settings request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetJsonSettingsRequest {
    pub column_name: String,
    pub settings: JsonSettings,
}

/// A family of annotation table options: pure metadata markers that no region
/// consumes. Setting or unsetting them only rewrites the table's
/// `extra_options`, so the alter skips region dispatch entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnnotationFamily {
    /// `greptime.semantic.*` options (see the [`semantic`] module).
    Semantic,
    /// Column and partition count hints consumed by the auto-repartition planner.
    RepartitionHint,
}

impl AnnotationFamily {
    /// The key namespace used in diagnostics; accepted keys are classified by [`Self::of_key`].
    pub fn namespace(self) -> &'static str {
        match self {
            Self::Semantic => SEMANTIC_PREFIX,
            Self::RepartitionHint => "repartition.",
        }
    }

    pub fn of_key(key: &str) -> Option<Self> {
        if key.starts_with(SEMANTIC_PREFIX) {
            Some(Self::Semantic)
        } else if matches!(
            key,
            REPARTITION_COLUMN_HINT_KEY | REPARTITION_PARTITION_NUM_HINT_KEY
        ) {
            Some(Self::RepartitionHint)
        } else {
            None
        }
    }

    /// Whether this family may be altered on logical metric tables. Only
    /// families whose values nothing on the physical side consumes qualify;
    /// the repartition hint drives physical region repartitioning.
    pub fn allows_logical_tables(self) -> bool {
        match self {
            Self::Semantic => true,
            Self::RepartitionHint => false,
        }
    }

    /// Whether duplicate keys are rejected in this family's SET/UNSET batch.
    pub fn requires_unique_keys(self) -> bool {
        matches!(self, Self::RepartitionHint)
    }

    /// The error for a SET/UNSET batch mixing this family with other options.
    pub fn mixed_batch_error(self) -> String {
        match self {
            Self::Semantic => format!(
                "`{SEMANTIC_PREFIX}*` options must be altered separately from other table options"
            ),
            Self::RepartitionHint => {
                "repartition hints must be altered separately from other table options".to_string()
            }
        }
    }
}

/// Why an annotation SET/UNSET key batch was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnnotationKeyError {
    MixedFamilies { family: AnnotationFamily },
    DuplicateKey,
}

impl fmt::Display for AnnotationKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MixedFamilies { family } => f.write_str(&family.mixed_batch_error()),
            Self::DuplicateKey => f.write_str("duplicate repartition hint keys"),
        }
    }
}

/// Validates a SET/UNSET batch and returns its annotation family.
///
/// Empty batches and batches containing only non-annotation keys return `None`.
/// Annotation keys cannot share a batch with another family or region options.
/// Duplicate keys are rejected only for families that require unique keys.
pub fn validate_annotation_keys<'a>(
    keys: impl IntoIterator<Item = &'a str>,
) -> std::result::Result<Option<AnnotationFamily>, AnnotationKeyError> {
    let mut keys = keys.into_iter();
    let Some(first) = keys.next() else {
        return Ok(None);
    };
    let family = AnnotationFamily::of_key(first);
    let reject_duplicates = family.is_some_and(|family| family.requires_unique_keys());
    let mut seen = HashSet::new();
    if reject_duplicates {
        seen.insert(first);
    }
    for key in keys {
        let this = AnnotationFamily::of_key(key);
        if this != family
            && let Some(family) = family.or(this)
        {
            return Err(AnnotationKeyError::MixedFamilies { family });
        }
        if reject_duplicates && !seen.insert(key) {
            return Err(AnnotationKeyError::DuplicateKey);
        }
    }
    Ok(family)
}

/// Table shape an annotation option is validated against.
pub struct AnnotationContext<'a> {
    pub data_model: Option<&'a str>,
    pub schema: &'a Schema,
    pub partition_key_indices: &'a [usize],
}

/// Why an annotation option was rejected. Typed so each DDL entry point maps
/// rules onto its existing error variants and status codes: ALTER keeps
/// missing columns as `TableColumnNotFound` (4002), CREATE keeps its
/// `InvalidArguments` family — the rules converge, the contracts do not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnnotationValidationError {
    UnknownKey {
        key: String,
    },
    InvalidValue {
        key: String,
        value: String,
    },
    ColumnNotFound {
        column: String,
    },
    ColumnNotStringForm {
        key: String,
        column: String,
        ty: ConcreteDataType,
    },
    InvalidPartitionNumHint {
        value: String,
    },
    NotSingleColumn,
    PartitionMetadataConflict,
    TimeIndexConflict,
}

impl fmt::Display for AnnotationValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownKey { key } => write!(f, "unknown semantic option `{key}`"),
            Self::InvalidValue { key, value } => {
                write!(f, "invalid value `{value}` for semantic option `{key}`")
            }
            Self::ColumnNotFound { column } => write!(f, "column `{column}` not found"),
            Self::ColumnNotStringForm { key, column, ty } => write!(
                f,
                "entity column `{column}` (option `{key}`) has type `{ty}`, \
                 which cannot render as a string"
            ),
            Self::InvalidPartitionNumHint { value } => write!(
                f,
                "{REPARTITION_PARTITION_NUM_HINT_KEY} expects a positive integer within u32 range, got `{value}`"
            ),
            Self::NotSingleColumn => write!(
                f,
                "{REPARTITION_COLUMN_HINT_KEY} expects exactly one column name"
            ),
            Self::PartitionMetadataConflict => write!(
                f,
                "cannot set {REPARTITION_COLUMN_HINT_KEY} on a table with partition metadata"
            ),
            Self::TimeIndexConflict => write!(
                f,
                "cannot set {REPARTITION_COLUMN_HINT_KEY} to the time index column"
            ),
        }
    }
}

/// Validates one annotation option and returns the value to store — the
/// repartition hints are trimmed, semantic values pass
/// through unchanged.
pub(crate) fn validate_and_normalize_annotation(
    family: AnnotationFamily,
    cx: &AnnotationContext<'_>,
    key: &str,
    value: &str,
) -> std::result::Result<String, AnnotationValidationError> {
    match family {
        AnnotationFamily::Semantic => {
            if !is_semantic_option_key(key) {
                return Err(AnnotationValidationError::UnknownKey {
                    key: key.to_string(),
                });
            }
            if !validate_semantic_option(key, value) {
                return Err(AnnotationValidationError::InvalidValue {
                    key: key.to_string(),
                    value: value.to_string(),
                });
            }
            if parse_entity_option_key(key).is_some() {
                for column in parse_entity_columns(value) {
                    if trace_v2_attribute(cx.schema, cx.data_model, &column).is_some() {
                        continue;
                    }
                    let schema = cx.schema.column_schema_by_name(&column).ok_or_else(|| {
                        AnnotationValidationError::ColumnNotFound {
                            column: column.clone(),
                        }
                    })?;
                    if !has_stable_string_form(&schema.data_type) {
                        return Err(AnnotationValidationError::ColumnNotStringForm {
                            key: key.to_string(),
                            column,
                            ty: schema.data_type.clone(),
                        });
                    }
                }
            }
            Ok(value.to_string())
        }
        AnnotationFamily::RepartitionHint if key == REPARTITION_PARTITION_NUM_HINT_KEY => {
            let value = value.trim();
            if !matches!(value.parse::<u32>(), Ok(1..)) {
                return Err(AnnotationValidationError::InvalidPartitionNumHint {
                    value: value.to_string(),
                });
            }
            Ok(value.to_string())
        }
        AnnotationFamily::RepartitionHint => {
            let column_name = value.trim();
            if column_name.is_empty() || column_name.contains(',') {
                return Err(AnnotationValidationError::NotSingleColumn);
            }
            if !cx.partition_key_indices.is_empty() {
                return Err(AnnotationValidationError::PartitionMetadataConflict);
            }
            let column_index = cx.schema.column_index_by_name(column_name).ok_or_else(|| {
                AnnotationValidationError::ColumnNotFound {
                    column: column_name.to_string(),
                }
            })?;
            if cx.schema.timestamp_index() == Some(column_index) {
                return Err(AnnotationValidationError::TimeIndexConflict);
            }
            Ok(column_name.to_string())
        }
    }
}

/// CREATE-side entry: validates every annotation option present in `options`
/// and writes normalized values back in place.
pub fn validate_and_normalize_annotation_options(
    options: &mut TableOptions,
    schema: &Schema,
    partition_key_indices: &[usize],
) -> std::result::Result<(), AnnotationValidationError> {
    let cx = AnnotationContext {
        data_model: options.data_model(),
        schema,
        partition_key_indices,
    };
    let mut normalized = Vec::new();
    for (key, value) in &options.extra_options {
        let Some(family) = AnnotationFamily::of_key(key) else {
            continue;
        };
        let checked = validate_and_normalize_annotation(family, &cx, key, value)?;
        if checked != *value {
            normalized.push((key.clone(), checked));
        }
    }
    for (key, value) in normalized {
        options.extra_options.insert(key, value);
    }
    Ok(())
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
    SetJsonSettings {
        request: SetJsonSettingsRequest,
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
    SetAnnotations {
        family: AnnotationFamily,
        options: Vec<(String, String)>,
    },
    UnsetAnnotations {
        family: AnnotationFamily,
        keys: Vec<String>,
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
    /// Whether this insert should skip WAL.
    pub skip_wal: bool,
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
    /// The index build mode. Absent options select SST indexes.
    pub options: Option<build_index_request::Options>,
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
    pub time_range: Option<TimestampRange>,
}

impl Default for CompactTableRequest {
    fn default() -> Self {
        Self {
            catalog_name: Default::default(),
            schema_name: Default::default(),
            table_name: Default::default(),
            compact_options: compact_request::Options::Regular(Default::default()),
            parallelism: 1,
            time_range: None,
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

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;

    use super::*;

    #[test]
    fn test_redact_connection() {
        assert_eq!(
            redact_connection("mysql://user:secret@127.0.0.1:3306/db"),
            "mysql://user:REDACTED@127.0.0.1:3306/db"
        );
        // A DSN without a password is left unchanged.
        assert_eq!(
            redact_connection("mysql://user@127.0.0.1:3306/db"),
            "mysql://user@127.0.0.1:3306/db"
        );
        // Unparsable input is fully redacted.
        assert_eq!(redact_connection("not-a-url"), "REDACTED");
    }

    #[test]
    fn test_redact_connection_in_text() {
        assert_eq!(
            redact_connection_in_text(
                "CREATE EXTERNAL TABLE t (...) ENGINE=relational \
                 WITH(connection='mysql://user:secret@127.0.0.1:3306/db', remote_table='sample')"
            ),
            "CREATE EXTERNAL TABLE t (...) ENGINE=relational \
             WITH(connection='mysql://user:REDACTED@127.0.0.1:3306/db', remote_table='sample')"
        );
        assert_eq!(
            redact_connection_in_text("Expected =, found: mysql://user:secret@127.0.0.1:3306/db"),
            "Expected =, found: mysql://user:REDACTED@127.0.0.1:3306/db"
        );
        assert_eq!(
            redact_connection_in_text("postgresql://user:secret@host:5432/db?sslmode=require"),
            "postgresql://user:REDACTED@host:5432/db?sslmode=require"
        );
        // A DSN without a password, or a non-DSN, is left unchanged.
        assert_eq!(
            redact_connection_in_text("mysql://host/db"),
            "mysql://host/db"
        );
        assert_eq!(
            redact_connection_in_text("mysql://host/db?email=a@b.com"),
            "mysql://host/db?email=a@b.com"
        );
        assert_eq!(redact_connection_in_text("SELECT 1"), "SELECT 1");
        // Multiple DSNs are all redacted.
        assert_eq!(
            redact_connection_in_text("mysql://a:one@h1/db1 and mysql://b:two@h2/db2"),
            "mysql://a:REDACTED@h1/db1 and mysql://b:REDACTED@h2/db2"
        );
    }

    #[test]
    fn test_relational_options_redacted() {
        let options = RelationalOptions {
            connection: "mysql://user:secret@127.0.0.1:3306/db".to_string(),
            remote_table: "sample".to_string(),
        };
        let redacted = options.redacted();
        assert_eq!(
            redacted.connection,
            "mysql://user:REDACTED@127.0.0.1:3306/db"
        );
        assert_eq!(redacted.remote_table, "sample");
    }

    #[test]
    fn test_relational_options_debug_redacts_connection() {
        let options = RelationalOptions {
            connection: "mysql://user:secret@127.0.0.1:3306/db".to_string(),
            remote_table: "sample".to_string(),
        };
        let debug = format!("{options:?}");
        assert!(!debug.contains("secret"));
        assert!(debug.contains("REDACTED"));
        assert!(debug.contains("sample"));
    }

    #[test]
    fn test_table_options_display_redacts_relational_meta() {
        let options = TableOptions {
            extra_options: HashMap::from([(
                RELATIONAL_META_KEY.to_string(),
                r#"{"connection":"mysql://user:secret@127.0.0.1:3306/db","remote_table":"sample"}"#
                    .to_string(),
            )]),
            ..Default::default()
        };
        let rendered = options.to_string();
        assert!(!rendered.contains("secret"));
        assert!(rendered.contains("REDACTED"));
    }

    #[test]
    fn test_table_options_debug_redacts_relational_meta() {
        let options = TableOptions {
            extra_options: HashMap::from([(
                RELATIONAL_META_KEY.to_string(),
                r#"{"connection":"mysql://user:secret@127.0.0.1:3306/db","remote_table":"sample"}"#
                    .to_string(),
            )]),
            ..Default::default()
        };
        let rendered = format!("{options:?}");
        assert!(!rendered.contains("secret"));
        assert!(rendered.contains("REDACTED"));
    }

    #[test]
    fn test_validate_annotation_keys() {
        let column = REPARTITION_COLUMN_HINT_KEY;
        let count = REPARTITION_PARTITION_NUM_HINT_KEY;
        for (keys, expected) in [
            (vec![], None),
            (vec![TTL_KEY, TTL_KEY], None),
            (vec!["repartition.unknown.hint"], None),
            (vec![column], Some(AnnotationFamily::RepartitionHint)),
            (vec![count], Some(AnnotationFamily::RepartitionHint)),
            (vec![column, count], Some(AnnotationFamily::RepartitionHint)),
            (vec![count, column], Some(AnnotationFamily::RepartitionHint)),
            (
                vec!["greptime.semantic.source", "greptime.semantic.source"],
                Some(AnnotationFamily::Semantic),
            ),
        ] {
            assert_eq!(validate_annotation_keys(keys), Ok(expected));
        }
        for keys in [
            vec![column, column],
            vec![count, count],
            vec![column, count, column],
        ] {
            assert_eq!(
                validate_annotation_keys(keys),
                Err(AnnotationKeyError::DuplicateKey)
            );
        }
        for keys in [
            vec![column, TTL_KEY],
            vec![TTL_KEY, count],
            vec![column, "greptime.semantic.source"],
        ] {
            assert_eq!(
                validate_annotation_keys(keys),
                Err(AnnotationKeyError::MixedFamilies {
                    family: AnnotationFamily::RepartitionHint,
                })
            );
        }
    }

    #[test]
    fn test_validate_table_option() {
        assert!(validate_table_option(FILE_TABLE_LOCATION_KEY));
        assert!(validate_table_option(FILE_TABLE_FORMAT_KEY));
        assert!(validate_table_option(FILE_TABLE_PATTERN_KEY));
        assert!(validate_table_option(RELATIONAL_CONNECTION_KEY));
        assert!(validate_table_option(RELATIONAL_REMOTE_TABLE_KEY));
        assert!(validate_table_option(TTL_KEY));
        assert!(validate_table_option(WRITE_BUFFER_SIZE_KEY));
        assert!(validate_table_option(STORAGE_KEY));
        assert!(validate_table_option(MEMTABLE_BULK_MERGE_THRESHOLD));
        assert!(validate_table_option(EXPERIMENTAL_SST_FLOAT_FIELD_ENCODING));
        assert!(validate_table_option(REPARTITION_COLUMN_HINT_KEY));
        assert!(validate_table_option(REPARTITION_PARTITION_NUM_HINT_KEY));
        assert_eq!(AnnotationFamily::of_key("repartition.unknown.hint"), None);
        assert!(!validate_table_option("foo"));

        // Only whitelisted semantic keys are accepted.
        assert!(validate_table_option(SEMANTIC_SIGNAL_TYPE));
        assert!(validate_table_option(SEMANTIC_METRIC_TYPE));
        // Unknown semantic key, near-miss, and the internal transport key are rejected.
        assert!(!validate_table_option("greptime.semantic.future.key"));
        assert!(!validate_table_option("greptime.semanticx"));
        assert!(!validate_table_option(SEMANTIC_PER_TABLE_INDEX_KEY));
    }

    #[test]
    fn test_validate_database_option() {
        assert!(validate_database_option(MEMTABLE_TYPE));
        assert!(validate_database_option(MEMTABLE_BULK_MERGE_THRESHOLD));
        assert!(validate_database_option(MEMTABLE_BULK_ENCODE_ROW_THRESHOLD));
        assert!(validate_database_option(
            MEMTABLE_BULK_ENCODE_BYTES_THRESHOLD
        ));
        assert!(validate_database_option(MEMTABLE_BULK_MAX_MERGE_GROUPS));
        assert!(validate_database_option(
            "compaction.twcs.active_window.trigger_file_num"
        ));
        assert!(validate_database_option(
            "compaction.twcs.active_window.l1_merge_trigger"
        ));
        assert!(validate_database_option(
            "compaction.twcs.inactive_window.trigger_file_num"
        ));
        assert!(validate_database_option(
            "compaction.twcs.inactive_window.l1_merge_trigger"
        ));
        assert!(validate_database_option(INGEST_ROWS_RATE_LIMIT_KEY));
        assert!(validate_database_option("ingest_rows_rate_limit"));
        assert!(!validate_database_option("foo"));
    }

    #[test]
    fn test_parse_float_field_encoding_option() {
        let options = TableOptions::try_from_iter([(
            EXPERIMENTAL_SST_FLOAT_FIELD_ENCODING,
            "byte_stream_split",
        )])
        .unwrap();
        assert_eq!(
            options
                .extra_options
                .get(EXPERIMENTAL_SST_FLOAT_FIELD_ENCODING),
            Some(&"byte_stream_split".to_string())
        );
        assert!(
            TableOptions::try_from_iter([(EXPERIMENTAL_SST_FLOAT_FIELD_ENCODING, "invalid",)])
                .is_err()
        );
    }

    #[test]
    fn test_database_trigger_value_boundaries() {
        let maximum = usize::MAX.to_string();
        let overflow = format!("{maximum}0");
        for (key, minimum) in [
            (TWCS_TRIGGER_FILE_NUM, 0),
            (TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM, 0),
            (TWCS_INACTIVE_WINDOW_TRIGGER_FILE_NUM, 0),
            (TWCS_ACTIVE_WINDOW_L1_MERGE_TRIGGER, 2),
            (TWCS_INACTIVE_WINDOW_L1_MERGE_TRIGGER, 2),
        ] {
            for invalid in [
                None,
                Some(""),
                Some("invalid"),
                Some("-1"),
                Some(overflow.as_str()),
            ] {
                assert!(
                    validate_database_option_value(key, invalid).is_err(),
                    "{key}: {invalid:?}"
                );
            }
            for valid in ["2", maximum.as_str()] {
                assert!(
                    validate_database_option_value(key, Some(valid)).is_ok(),
                    "{key}: {valid}"
                );
            }
            for boundary in ["0", "1"] {
                assert_eq!(
                    validate_database_option_value(key, Some(boundary)).is_ok(),
                    minimum == 0,
                    "{key}: {boundary}"
                );
            }
        }
    }

    #[test]
    fn test_database_ingest_rate_limit_value_boundaries() {
        for invalid in [
            None,
            Some(""),
            Some("abc"),
            Some("1000/s"),
            Some("-1"),
            Some("1.5"),
            Some("18446744073709551616"),
        ] {
            assert!(validate_database_option_value(INGEST_ROWS_RATE_LIMIT_KEY, invalid).is_err());
        }
        let maximum = u64::MAX.to_string();
        for valid in ["0", "1", maximum.as_str()] {
            assert!(
                validate_database_option_value(INGEST_ROWS_RATE_LIMIT_KEY, Some(valid)).is_ok()
            );
        }
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
            ttl: None,
            extra_options: HashMap::from([(SKIP_WAL_KEY.to_string(), true.to_string())]),
            skip_wal: true,
        };
        let serialized_map = HashMap::from(&options);
        assert_eq!(
            Some("true"),
            serialized_map.get(SKIP_WAL_KEY).map(String::as_str)
        );
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

        let options = TableOptions {
            extra_options: HashMap::from([(SKIP_WAL_KEY.to_string(), false.to_string())]),
            skip_wal: false,
            ..Default::default()
        };
        let serialized_map = HashMap::from(&options);
        let serialized = TableOptions::try_from_iter(&serialized_map).unwrap();
        assert_eq!(options, serialized);
    }

    #[test]
    fn test_table_options_normalizes_twcs_trigger_aliases() {
        for options in [
            vec![(TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM, "4")],
            vec![
                (TWCS_TRIGGER_FILE_NUM, "4"),
                (TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM, "4"),
            ],
        ] {
            let table_options = TableOptions::try_from_iter(options).unwrap();
            assert_eq!(
                HashMap::from([(TWCS_TRIGGER_FILE_NUM.to_string(), "4".to_string())]),
                table_options.extra_options
            );
        }
    }

    #[test]
    fn test_table_options_rejects_conflicting_twcs_trigger_aliases() {
        let error = TableOptions::try_from_iter([
            (TWCS_TRIGGER_FILE_NUM, "4"),
            (TWCS_ACTIVE_WINDOW_TRIGGER_FILE_NUM, "8"),
        ])
        .unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, error.status_code());
        assert_eq!(
            "Conflicting table options: compaction.twcs.trigger_file_num=4 and compaction.twcs.active_window.trigger_file_num=8",
            error.to_string()
        );
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

        let options = TableOptions {
            write_buffer_size: None,
            ttl: None,
            extra_options: HashMap::from([(SKIP_WAL_KEY.to_string(), "false".to_string())]),
            skip_wal: false,
        };
        assert_eq!("skip_wal=false", options.to_string());
    }
}
