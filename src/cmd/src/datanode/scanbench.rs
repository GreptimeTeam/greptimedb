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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use colored::Colorize;
use common_base::Plugins;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_meta::cache::{new_schema_cache, new_table_schema_cache};
use common_meta::key::SchemaMetadataManager;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_wal::config::DatanodeWalConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{BinaryExpr, Expr as DfExpr, ExprSchemable, Operator};
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{DFSchemaRef, ScalarValue, ToDFSchema};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::{
    Array, Float64Array, LargeStringArray, StringArray, StringViewArray, TimestampMillisecondArray,
};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log_store::kafka::log_store::KafkaLogStore;
use log_store::noop::log_store::NoopLogStore;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use mito2::sst::file_ref::FileReferenceManager;
use moka::future::CacheBuilder;
use object_store::manager::ObjectStoreManager;
use object_store::util::normalize_dir;
use query::optimizer::parallelize_scan::ParallelizeScan;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use snafu::{OptionExt, ResultExt};
use sqlparser::ast::ExprWithAlias as SqlExprWithAlias;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use store_api::metadata::RegionMetadata;
use store_api::path_utils::WAL_DIR;
use store_api::region_engine::{PrepareRequest, QueryScanContext, RegionEngine};
use store_api::region_request::{PathType, RegionOpenRequest, RegionRequest};
use store_api::storage::{
    ProjectionInput, RegionId, ScanRequest, TimeSeriesDistribution, TimeSeriesRowSelector,
};
use tokio::fs;

use crate::datanode::objbench::{build_object_store, parse_config};
use crate::error;

/// Scan benchmark command - benchmarks scanning a region directly from storage.
#[derive(Debug, Parser)]
pub struct ScanbenchCommand {
    /// Path to config TOML file (same format as standalone/datanode config)
    #[clap(long, value_name = "FILE")]
    config: PathBuf,

    /// Region ID: either numeric u64 (e.g. "4398046511104") or "table_id:region_num" (e.g. "1024:0")
    #[clap(long)]
    region_id: String,

    /// Table directory relative to data home (e.g. "data/greptime/public/1024/")
    #[clap(long)]
    table_dir: String,

    /// Scanner type: seq, unordered, series
    #[clap(long, default_value = "seq")]
    scanner: String,

    /// Path to scan request JSON config file (optional)
    #[clap(long, value_name = "FILE")]
    scan_config: Option<PathBuf>,

    /// Number of partitions for parallel scan (simulates parallelism)
    #[clap(long, default_value = "1")]
    parallelism: usize,

    /// Number of iterations for benchmarking
    #[clap(long, default_value = "1")]
    iterations: usize,

    /// Path type for the region: bare, data, metadata
    #[clap(long, default_value = "bare")]
    path_type: String,

    /// Verbose output
    #[clap(short, long, default_value_t = false)]
    verbose: bool,

    /// Output pprof flamegraph
    #[clap(long, value_name = "FILE")]
    pprof_file: Option<PathBuf>,

    /// Enable WAL replay when opening the region.
    #[clap(long, default_value_t = false)]
    enable_wal: bool,

    /// Start pprof after the first iteration (use first iteration as warmup).
    #[clap(long, default_value_t = false)]
    pprof_after_warmup: bool,

    /// Compute a deterministic SHA-256 digest of visible logical metric rows.
    #[clap(long, default_value_t = false)]
    logical_row_digest: bool,

    /// Label column included in the logical row digest. May be specified repeatedly.
    #[clap(long = "digest-label", value_name = "NAME")]
    digest_labels: Vec<String>,

    /// Timestamp column included in the logical row digest.
    #[clap(long, value_name = "NAME")]
    digest_timestamp_column: Option<String>,

    /// Value column included in the logical row digest.
    #[clap(long, value_name = "NAME")]
    digest_value_column: Option<String>,
}

/// JSON config for scan request parameters.
#[derive(Debug, Deserialize, Default)]
struct ScanConfig {
    projection: Option<Vec<usize>>,
    projection_names: Option<Vec<String>>,
    filters: Option<Vec<String>>,
    series_row_selector: Option<String>,
}

const LOGICAL_ROW_DIGEST_DOMAIN: &[u8] = b"greptimedb.logical-metric-row-digest.v1\0";
const LOGICAL_ROW_DIGEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
struct DigestColumn {
    name: String,
    column_id: u32,
}

#[derive(Debug)]
struct LogicalRowDigest {
    labels: Vec<DigestColumn>,
    timestamp: DigestColumn,
    value: DigestColumn,
    hasher: Sha256,
    rows: u64,
}

enum RawStringColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> RawStringColumn<'a> {
    fn try_new(array: &'a dyn Array) -> error::Result<Self> {
        match array.data_type() {
            ArrowDataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(Self::Utf8)
                .ok_or_else(|| illegal_config("Failed to decode digest STRING column")),
            ArrowDataType::LargeUtf8 => array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(Self::LargeUtf8)
                .ok_or_else(|| illegal_config("Failed to decode digest STRING column")),
            ArrowDataType::Utf8View => array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .map(Self::Utf8View)
                .ok_or_else(|| illegal_config("Failed to decode digest STRING column")),
            data_type => Err(illegal_config(format!(
                "Digest label column must be STRING, got {data_type:?}"
            ))),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(array) => array.is_null(row),
            Self::LargeUtf8(array) => array.is_null(row),
            Self::Utf8View(array) => array.is_null(row),
        }
    }

    fn value(&self, row: usize) -> &str {
        match self {
            Self::Utf8(array) => array.value(row),
            Self::LargeUtf8(array) => array.value(row),
            Self::Utf8View(array) => array.value(row),
        }
    }
}

fn illegal_config(msg: impl Into<String>) -> error::Error {
    error::IllegalConfigSnafu { msg: msg.into() }.build()
}

fn write_name(hasher: &mut Sha256, name: &str) {
    hasher.update((name.len() as u32).to_be_bytes());
    hasher.update(name.as_bytes());
}

impl LogicalRowDigest {
    fn new(labels: Vec<DigestColumn>, timestamp: DigestColumn, value: DigestColumn) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(LOGICAL_ROW_DIGEST_DOMAIN);
        hasher.update((labels.len() as u32).to_be_bytes());
        for label in &labels {
            write_name(&mut hasher, &label.name);
        }
        write_name(&mut hasher, &timestamp.name);
        hasher.update(b"millisecond\0");
        write_name(&mut hasher, &value.name);
        hasher.update(b"float64\0");

        Self {
            labels,
            timestamp,
            value,
            hasher,
            rows: 0,
        }
    }

    fn projection(&self, metadata: &RegionMetadata) -> error::Result<Vec<usize>> {
        self.labels
            .iter()
            .chain(std::iter::once(&self.timestamp))
            .chain(std::iter::once(&self.value))
            .map(|column| {
                metadata.column_index_by_name(&column.name).ok_or_else(|| {
                    illegal_config(format!(
                        "Digest column '{}' is missing from the region",
                        column.name
                    ))
                })
            })
            .collect()
    }

    fn update(&mut self, batch: &RecordBatch) -> error::Result<()> {
        let expected_columns = self.labels.len() + 2;
        if batch.num_columns() != expected_columns {
            return Err(illegal_config(format!(
                "Digest scan returned {} columns, expected {expected_columns}",
                batch.num_columns()
            )));
        }

        let schema = batch.schema();
        let expected_names = self
            .labels
            .iter()
            .map(|column| column.name.as_str())
            .chain(std::iter::once(self.timestamp.name.as_str()))
            .chain(std::iter::once(self.value.name.as_str()));
        for (index, expected_name) in expected_names.enumerate() {
            let field = schema.field(index);
            if field.name() != expected_name {
                return Err(illegal_config(format!(
                    "Digest scan column {index} is '{}', expected '{expected_name}'",
                    field.name()
                )));
            }
            if batch.column(index).len() != batch.num_rows() {
                return Err(illegal_config(format!(
                    "Digest scan column '{expected_name}' has an invalid row count"
                )));
            }
        }

        let labels = self
            .labels
            .iter()
            .enumerate()
            .map(|(index, _)| RawStringColumn::try_new(batch.column(index).as_ref()))
            .collect::<error::Result<Vec<_>>>()?;
        let timestamp_index = self.labels.len();
        let timestamp_array = batch.column(timestamp_index);
        if timestamp_array.data_type() != &ArrowDataType::Timestamp(TimeUnit::Millisecond, None) {
            return Err(illegal_config(format!(
                "Digest timestamp column '{}' must be TIMESTAMP_MILLISECOND, got {:?}",
                self.timestamp.name,
                timestamp_array.data_type()
            )));
        }
        let timestamps = timestamp_array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                illegal_config("Failed to decode digest TIMESTAMP_MILLISECOND column")
            })?;

        let value_array = batch.column(timestamp_index + 1);
        if value_array.data_type() != &ArrowDataType::Float64 {
            return Err(illegal_config(format!(
                "Digest value column '{}' must be FLOAT64, got {:?}",
                self.value.name,
                value_array.data_type()
            )));
        }
        let values = value_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| illegal_config("Failed to decode digest FLOAT64 column"))?;

        let new_rows = self
            .rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| illegal_config("Digest row count overflow"))?;
        for row in 0..batch.num_rows() {
            for label in &labels {
                if label.is_null(row) {
                    return Err(illegal_config("Digest label column contains NULL"));
                }
                let value = label.value(row);
                let length = u32::try_from(value.len())
                    .map_err(|_| illegal_config("Digest label value exceeds u32 length framing"))?;
                self.hasher.update(length.to_be_bytes());
                self.hasher.update(value.as_bytes());
            }
            if timestamps.is_null(row) {
                return Err(illegal_config("Digest timestamp column contains NULL"));
            }
            self.hasher.update(timestamps.value(row).to_be_bytes());
            if values.is_null(row) {
                return Err(illegal_config("Digest value column contains NULL"));
            }
            self.hasher
                .update(values.value(row).to_bits().to_be_bytes());
        }
        self.rows = new_rows;
        Ok(())
    }

    fn digest_hex(&self) -> String {
        hex::encode(self.hasher.clone().finalize())
    }
}

fn is_safe_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn validate_digest_column(
    metadata: &RegionMetadata,
    name: &str,
    expected_type: ConcreteDataType,
    kind: &str,
) -> error::Result<DigestColumn> {
    let index = metadata
        .column_index_by_name(name)
        .ok_or_else(|| illegal_config(format!("Digest {kind} column '{name}' is missing")))?;
    let column = &metadata.column_metadatas[index];
    if column.column_schema.data_type != expected_type {
        return Err(illegal_config(format!(
            "Digest {kind} column '{name}' has type {}, expected {expected_type}",
            column.column_schema.data_type
        )));
    }
    Ok(DigestColumn {
        name: name.to_string(),
        column_id: column.column_id,
    })
}

fn validate_logical_row_digest_options(command: &ScanbenchCommand) -> error::Result<()> {
    if !command.logical_row_digest {
        return Ok(());
    }
    if command.scanner != "series" {
        return Err(illegal_config(
            "--logical-row-digest requires --scanner series",
        ));
    }
    if command.parallelism != 1 {
        return Err(illegal_config(
            "--logical-row-digest requires --parallelism 1",
        ));
    }
    if command.iterations != 1 {
        return Err(illegal_config(
            "--logical-row-digest requires --iterations 1",
        ));
    }
    if command.enable_wal {
        return Err(illegal_config(
            "--logical-row-digest requires WAL to be disabled",
        ));
    }
    if command.pprof_file.is_some() || command.pprof_after_warmup {
        return Err(illegal_config(
            "--logical-row-digest cannot be used with profiling",
        ));
    }
    if command.digest_labels.is_empty() {
        return Err(illegal_config(
            "--logical-row-digest requires at least one --digest-label",
        ));
    }
    let timestamp = command
        .digest_timestamp_column
        .as_deref()
        .ok_or_else(|| illegal_config("--logical-row-digest requires --digest-timestamp-column"))?;
    let value = command
        .digest_value_column
        .as_deref()
        .ok_or_else(|| illegal_config("--logical-row-digest requires --digest-value-column"))?;
    let mut names = HashSet::new();
    for name in command
        .digest_labels
        .iter()
        .map(String::as_str)
        .chain([timestamp, value])
    {
        if !is_safe_identifier(name) {
            return Err(illegal_config(format!(
                "Digest column '{name}' is not a safe identifier"
            )));
        }
        if !names.insert(name) {
            return Err(illegal_config(format!(
                "Digest column '{name}' is duplicated"
            )));
        }
    }
    Ok(())
}

fn scan_distribution(
    scanner: &str,
    logical_row_digest: bool,
) -> error::Result<Option<TimeSeriesDistribution>> {
    if logical_row_digest {
        // Validation requires `series`; make the Mito SeriesScan selection explicit.
        return Ok(Some(TimeSeriesDistribution::PerSeries));
    }

    match scanner {
        "seq" => Ok(None),
        "unordered" => Ok(Some(TimeSeriesDistribution::TimeWindowed)),
        "series" => Ok(Some(TimeSeriesDistribution::PerSeries)),
        other => Err(illegal_config(format!(
            "Unknown scanner type '{other}', expected: seq, unordered, series"
        ))),
    }
}

fn build_logical_row_digest(
    command: &ScanbenchCommand,
    scan_config: &ScanConfig,
    metadata: &RegionMetadata,
) -> error::Result<LogicalRowDigest> {
    if scan_config.projection.is_some() || scan_config.projection_names.is_some() {
        return Err(illegal_config(
            "--logical-row-digest supplies its own projection; remove projection from scan config",
        ));
    }
    if scan_config
        .filters
        .as_ref()
        .is_some_and(|filters| !filters.is_empty())
    {
        return Err(illegal_config(
            "--logical-row-digest does not allow scan filters",
        ));
    }
    if scan_config.series_row_selector.is_some() {
        return Err(illegal_config(
            "--logical-row-digest does not allow series_row_selector",
        ));
    }

    let labels = command
        .digest_labels
        .iter()
        .map(|name| {
            validate_digest_column(metadata, name, ConcreteDataType::string_datatype(), "label")
        })
        .collect::<error::Result<Vec<_>>>()?;
    let timestamp = validate_digest_column(
        metadata,
        command
            .digest_timestamp_column
            .as_deref()
            .unwrap_or_default(),
        ConcreteDataType::timestamp_millisecond_datatype(),
        "timestamp",
    )?;
    let value = validate_digest_column(
        metadata,
        command.digest_value_column.as_deref().unwrap_or_default(),
        ConcreteDataType::float64_datatype(),
        "value",
    )?;
    Ok(LogicalRowDigest::new(labels, timestamp, value))
}

fn resolve_projection(
    scan_config: &ScanConfig,
    metadata: Option<&RegionMetadata>,
) -> error::Result<Option<Vec<usize>>> {
    if scan_config.projection.is_some() && scan_config.projection_names.is_some() {
        return Err(error::IllegalConfigSnafu {
            msg: "scan config cannot contain both 'projection' and 'projection_names'".to_string(),
        }
        .build());
    }

    if let Some(projection) = &scan_config.projection {
        return Ok(Some(projection.clone()));
    }

    if let Some(projection_names) = &scan_config.projection_names {
        let metadata = metadata.context(error::IllegalConfigSnafu {
            msg: "Missing region metadata while resolving 'projection_names'".to_string(),
        })?;
        let available_columns = metadata
            .column_metadatas
            .iter()
            .map(|column| column.column_schema.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let projection = projection_names
            .iter()
            .map(|name| {
                metadata
                    .column_index_by_name(name)
                    .with_context(|| error::IllegalConfigSnafu {
                        msg: format!(
                            "Unknown column '{}' in projection_names, available columns: [{}]",
                            name, available_columns
                        ),
                    })
            })
            .collect::<error::Result<Vec<_>>>()?;
        return Ok(Some(projection));
    }

    Ok(None)
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn parse_region_id(s: &str) -> error::Result<RegionId> {
    if s.contains(':') {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        let table_id: u32 = parts[0].parse().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("invalid table_id in region_id '{}': {}", s, e),
            }
            .build()
        })?;
        let region_num: u32 = parts[1].parse().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("invalid region_num in region_id '{}': {}", s, e),
            }
            .build()
        })?;
        Ok(RegionId::new(table_id, region_num))
    } else {
        let id: u64 = s.parse().map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("invalid region_id '{}': {}", s, e),
            }
            .build()
        })?;
        Ok(RegionId::from_u64(id))
    }
}

fn parse_path_type(s: &str) -> error::Result<PathType> {
    match s.to_lowercase().as_str() {
        "bare" => Ok(PathType::Bare),
        "data" => Ok(PathType::Data),
        "metadata" => Ok(PathType::Metadata),
        _ => Err(error::IllegalConfigSnafu {
            msg: format!("invalid path_type '{}', expected: bare, data, metadata", s),
        }
        .build()),
    }
}

/// Rewrites literal values in comparison expressions to match the column's arrow type.
struct LiteralTypeCaster {
    schema: DFSchemaRef,
}

impl TreeNodeRewriter for LiteralTypeCaster {
    type Node = DfExpr;

    fn f_up(&mut self, expr: DfExpr) -> datafusion_common::Result<Transformed<DfExpr>> {
        let DfExpr::BinaryExpr(BinaryExpr { left, op, right }) = &expr else {
            return Ok(Transformed::no(expr));
        };

        if !matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ) {
            return Ok(Transformed::no(expr));
        }

        let (col_expr, lit_expr, col_left) = match (left.as_ref(), right.as_ref()) {
            (col @ DfExpr::Column(_), lit @ DfExpr::Literal(_, _)) => (col, lit, true),
            (lit @ DfExpr::Literal(_, _), col @ DfExpr::Column(_)) => (col, lit, false),
            _ => return Ok(Transformed::no(expr)),
        };

        let col_type = col_expr.get_type(self.schema.as_ref())?;
        let DfExpr::Literal(scalar, _) = lit_expr else {
            unreachable!()
        };

        if scalar.data_type() == col_type {
            return Ok(Transformed::no(expr));
        }

        let lit_array = scalar.to_array()?;
        let casted = compute::cast(lit_array.as_ref(), &col_type).map_err(|e| {
            datafusion_common::DataFusionError::Internal(format!(
                "Failed to cast literal {:?} to {:?}: {}",
                scalar, col_type, e
            ))
        })?;
        let casted_scalar = ScalarValue::try_from_array(&casted, 0)?;

        let new_lit = DfExpr::Literal(casted_scalar, None);
        let (new_left, new_right) = if col_left {
            (left.clone(), Box::new(new_lit))
        } else {
            (Box::new(new_lit), right.clone())
        };

        Ok(Transformed::yes(DfExpr::BinaryExpr(BinaryExpr {
            left: new_left,
            op: *op,
            right: new_right,
        })))
    }
}

fn convert_literal_types(
    exprs: Vec<DfExpr>,
    schema: &DFSchemaRef,
) -> datafusion_common::Result<Vec<DfExpr>> {
    use datafusion_common::tree_node::TreeNode;

    let mut caster = LiteralTypeCaster {
        schema: schema.clone(),
    };
    exprs
        .into_iter()
        .map(|e| e.rewrite(&mut caster).map(|x| x.data))
        .collect()
}

fn resolve_filters(
    scan_config: &ScanConfig,
    metadata: &RegionMetadata,
) -> error::Result<Vec<DfExpr>> {
    let Some(filters) = &scan_config.filters else {
        return Ok(Vec::new());
    };

    let df_schema = metadata
        .schema
        .arrow_schema()
        .clone()
        .to_dfschema()
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Failed to convert region schema to DataFusion schema: {e}"),
            }
            .build()
        })?;

    let state = SessionStateBuilder::new()
        .with_config(Default::default())
        .with_runtime_env(Default::default())
        .with_default_features()
        .build();

    let exprs: Vec<DfExpr> = filters
        .iter()
        .enumerate()
        .map(|(idx, filter)| {
            let mut parser = SqlParser::new(&GenericDialect {})
                .try_with_sql(filter)
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("Invalid filter at index {idx} ('{filter}'): {e}"),
                    }
                    .build()
                })?;

            let sql_expr = parser.parse_expr().map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("Invalid filter at index {idx} ('{filter}'): {e}"),
                }
                .build()
            })?;

            state
                .create_logical_expr_from_sql_expr(
                    SqlExprWithAlias {
                        expr: sql_expr,
                        alias: None,
                    },
                    &df_schema,
                )
                .map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!(
                            "Failed to convert filter at index {idx} ('{filter}') to logical expr: {e}"
                        ),
                    }
                    .build()
                })
        })
        .collect::<error::Result<Vec<_>>>()?;

    let df_schema_ref = Arc::new(df_schema);
    convert_literal_types(exprs, &df_schema_ref).map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("Failed to convert filter expression types: {e}"),
        }
        .build()
    })
}

fn noop_partition_expr_fetcher() -> mito2::region::opener::PartitionExprFetcherRef {
    struct NoopPartitionExprFetcher;

    #[async_trait::async_trait]
    impl mito2::region::opener::PartitionExprFetcher for NoopPartitionExprFetcher {
        async fn fetch_expr(&self, _region_id: RegionId) -> Option<String> {
            None
        }
    }

    Arc::new(NoopPartitionExprFetcher)
}

struct EngineComponents {
    data_home: String,
    mito_config: MitoConfig,
    object_store_manager: Arc<ObjectStoreManager>,
    schema_metadata_manager: Arc<SchemaMetadataManager>,
    file_ref_manager: Arc<FileReferenceManager>,
    partition_expr_fetcher: mito2::region::opener::PartitionExprFetcherRef,
}

impl EngineComponents {
    async fn build<S: store_api::logstore::LogStore>(
        self,
        log_store: Arc<S>,
    ) -> error::Result<MitoEngine> {
        MitoEngine::new(
            &self.data_home,
            self.mito_config,
            log_store,
            self.object_store_manager,
            self.schema_metadata_manager,
            self.file_ref_manager,
            self.partition_expr_fetcher,
            Plugins::default(),
        )
        .await
        .map_err(BoxedError::new)
        .context(error::BuildCliSnafu)
    }
}

fn mock_schema_metadata_manager() -> Arc<SchemaMetadataManager> {
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let table_schema_cache = Arc::new(new_table_schema_cache(
        "table_schema_name_cache".to_string(),
        CacheBuilder::default().build(),
        kv_backend.clone(),
    ));
    let schema_cache = Arc::new(new_schema_cache(
        "schema_cache".to_string(),
        CacheBuilder::default().build(),
        kv_backend.clone(),
    ));
    Arc::new(SchemaMetadataManager::new(table_schema_cache, schema_cache))
}

impl ScanbenchCommand {
    pub async fn run(&self) -> error::Result<()> {
        validate_logical_row_digest_options(self)?;

        if self.verbose {
            common_telemetry::init_default_ut_logging();
        }

        println!("{}", "Starting scanbench...".cyan().bold());

        let region_id = parse_region_id(&self.region_id)?;
        let path_type = parse_path_type(&self.path_type)?;
        println!(
            "{} Region ID: {} (u64: {})",
            "✓".green(),
            self.region_id,
            region_id.as_u64()
        );

        // Parse config and build object store
        let (store_cfg, mito_config, wal_config) = parse_config(&self.config)?;
        println!("{} Config parsed", "✓".green());

        let object_store = build_object_store(&store_cfg).await?;
        println!("{} Object store initialized", "✓".green());

        let object_store_manager =
            Arc::new(ObjectStoreManager::new("default", object_store.clone()));

        // Create mock dependencies
        let schema_metadata_manager = mock_schema_metadata_manager();
        let file_ref_manager = Arc::new(FileReferenceManager::new(None));
        let partition_expr_fetcher = noop_partition_expr_fetcher();

        // Create MitoEngine with appropriate log store
        let components = EngineComponents {
            data_home: store_cfg.data_home.clone(),
            mito_config,
            object_store_manager,
            schema_metadata_manager,
            file_ref_manager,
            partition_expr_fetcher,
        };

        let engine = match &wal_config {
            DatanodeWalConfig::RaftEngine(raft_engine_config) if self.enable_wal => {
                let data_home = normalize_dir(&store_cfg.data_home);
                let wal_dir = match &raft_engine_config.dir {
                    Some(dir) => dir.clone(),
                    None => format!("{}{WAL_DIR}", data_home),
                };
                fs::create_dir_all(&wal_dir).await.map_err(|e| {
                    error::IllegalConfigSnafu {
                        msg: format!("failed to create WAL directory {}: {e}", wal_dir),
                    }
                    .build()
                })?;
                let log_store = Arc::new(
                    RaftEngineLogStore::try_new(wal_dir, raft_engine_config)
                        .await
                        .map_err(BoxedError::new)
                        .context(error::BuildCliSnafu)?,
                );
                println!("{} Using RaftEngine WAL", "✓".green());
                components.build(log_store).await?
            }
            DatanodeWalConfig::Kafka(kafka_config) if self.enable_wal => {
                let log_store = Arc::new(
                    KafkaLogStore::try_new(kafka_config, None)
                        .await
                        .map_err(BoxedError::new)
                        .context(error::BuildCliSnafu)?,
                );
                println!("{} Using Kafka WAL", "✓".green());
                components.build(log_store).await?
            }
            _ => {
                let log_store = Arc::new(NoopLogStore);
                println!(
                    "{} Using NoopLogStore (enable_wal={})",
                    "✓".green(),
                    self.enable_wal
                );
                components.build(log_store).await?
            }
        };

        // Open region
        let open_request = RegionOpenRequest {
            engine: "mito".to_string(),
            table_dir: self.table_dir.clone(),
            path_type,
            options: HashMap::default(),
            skip_wal_replay: !self.enable_wal,
            checkpoint: None,
            requirements: Default::default(),
        };

        engine
            .handle_request(region_id, RegionRequest::Open(open_request))
            .await
            .map_err(BoxedError::new)
            .context(error::BuildCliSnafu)?;
        println!("{} Region opened", "✓".green());

        // Load scan config
        let scan_config = if let Some(path) = &self.scan_config {
            let content = tokio::fs::read_to_string(path)
                .await
                .context(error::FileIoSnafu)?;
            serde_json::from_str::<ScanConfig>(&content).context(error::SerdeJsonSnafu)?
        } else {
            ScanConfig::default()
        };
        let metadata = engine
            .get_metadata(region_id)
            .await
            .map_err(BoxedError::new)
            .context(error::BuildCliSnafu)?;
        let mut logical_digest = if self.logical_row_digest {
            Some(build_logical_row_digest(self, &scan_config, &metadata)?)
        } else {
            None
        };
        let projection = match &logical_digest {
            Some(digest) => Some(digest.projection(&metadata)?),
            None => resolve_projection(&scan_config, Some(&metadata))?,
        };
        let filters = if logical_digest.is_some() {
            Vec::new()
        } else {
            resolve_filters(&scan_config, &metadata)?
        };

        // Build scan request
        let distribution = scan_distribution(&self.scanner, self.logical_row_digest)?;

        let series_row_selector = match scan_config.series_row_selector.as_deref() {
            Some("last_row") => Some(TimeSeriesRowSelector::LastRow),
            Some(other) => {
                return Err(error::IllegalConfigSnafu {
                    msg: format!("Unknown series_row_selector '{}'", other),
                }
                .build());
            }
            None => None,
        };

        println!(
            "{} Scanner: {}, Parallelism: {}, Iterations: {}",
            "ℹ".blue(),
            self.scanner,
            self.parallelism,
            self.iterations,
        );

        // Start profiling if pprof_file is specified (unless pprof_after_warmup is set)
        #[cfg(unix)]
        let mut profiler_guard = if self.pprof_file.is_some() && !self.pprof_after_warmup {
            println!("{} Starting profiling...", "⚡".yellow());
            Some(
                pprof::ProfilerGuardBuilder::default()
                    .frequency(99)
                    .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                    .build()
                    .map_err(|e| {
                        BoxedError::new(PlainError::new(
                            format!("Failed to start profiler: {e}"),
                            StatusCode::Unexpected,
                        ))
                    })
                    .context(error::BuildCliSnafu)?,
            )
        } else {
            None
        };

        #[cfg(not(unix))]
        if self.pprof_file.is_some() {
            eprintln!(
                "{}: Profiling is not supported on this platform",
                "Warning".yellow()
            );
        }

        let mut total_rows_all = 0u64;
        let mut total_elapsed_all = std::time::Duration::ZERO;

        let projection_input = projection.map(ProjectionInput::new);
        for iteration in 0..self.iterations {
            let request = ScanRequest {
                projection_input: projection_input.clone(),
                filters: filters.clone(),
                series_row_selector,
                distribution,
                ..Default::default()
            };

            let start = Instant::now();

            // Get scanner
            let mut scanner = engine
                .handle_query(region_id, request)
                .await
                .map_err(BoxedError::new)
                .context(error::BuildCliSnafu)?;

            // Get partition ranges and apply parallelism
            let original_partitions = scanner.properties().partitions.clone();
            let total_ranges: usize = original_partitions.iter().map(|p| p.len()).sum();

            if self.verbose {
                println!(
                    "  {} Original partitions: {}, total ranges: {}",
                    "ℹ".blue(),
                    original_partitions.len(),
                    total_ranges
                );
            }

            if self.logical_row_digest {
                // SeriesScan creates one deduped stream for every range in all prepared
                // partitions and then performs a final global merge across those streams.
                // Keeping every original range in this sole partition preserves that merge;
                // ordering the ranges themselves is neither needed nor sufficient.
                let ranges: Vec<_> = original_partitions.into_iter().flatten().collect();
                scanner
                    .prepare(
                        PrepareRequest::default()
                            .with_ranges(vec![ranges])
                            .with_target_partitions(1),
                    )
                    .map_err(BoxedError::new)
                    .context(error::BuildCliSnafu)?;
            } else if self.parallelism > 1 {
                // Flatten all ranges
                let all_ranges: Vec<_> = original_partitions.into_iter().flatten().collect();

                // Distribute ranges across partitions
                let mut partitions =
                    ParallelizeScan::assign_partition_range(all_ranges, self.parallelism);

                // Sort ranges within each partition by start time ascending
                for partition in &mut partitions {
                    partition.sort_by_key(|a| a.start);
                }

                scanner
                    .prepare(
                        PrepareRequest::default()
                            .with_ranges(partitions)
                            .with_target_partitions(self.parallelism),
                    )
                    .map_err(BoxedError::new)
                    .context(error::BuildCliSnafu)?;
            }

            // Scan all partitions
            let num_partitions = scanner.properties().partitions.len();
            if self.logical_row_digest && num_partitions != 1 {
                return Err(illegal_config(format!(
                    "Logical row digest scanner prepared {num_partitions} partitions, expected exactly 1"
                )));
            }
            let ctx = QueryScanContext {
                explain_verbose: self.verbose,
            };
            let metrics_set = ExecutionPlanMetricsSet::new();

            let mut total_rows = 0u64;
            let mut total_array_mem_size = 0u64;
            let mut total_estimated_size = 0u64;
            if let Some(digest) = logical_digest.as_mut() {
                let mut stream = scanner
                    .scan_partition(&ctx, &metrics_set, 0)
                    .map_err(BoxedError::new)
                    .context(error::BuildCliSnafu)?;
                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result
                        .map_err(BoxedError::new)
                        .context(error::BuildCliSnafu)?;
                    total_rows += batch.num_rows() as u64;
                    digest.update(batch.df_record_batch())?;
                    let df_batch = batch.df_record_batch();
                    total_array_mem_size += df_batch.get_array_memory_size() as u64;
                    total_estimated_size +=
                        mito2::memtable::record_batch_estimated_size(df_batch) as u64;
                }
            } else {
                let mut scan_futures = FuturesUnordered::new();
                for partition_idx in 0..num_partitions {
                    let mut stream = scanner
                        .scan_partition(&ctx, &metrics_set, partition_idx)
                        .map_err(BoxedError::new)
                        .context(error::BuildCliSnafu)?;

                    scan_futures.push(tokio::spawn(async move {
                        let mut rows = 0u64;
                        let mut array_mem_size = 0u64;
                        let mut estimated_size = 0u64;
                        while let Some(batch_result) = stream.next().await {
                            match batch_result {
                                Ok(batch) => {
                                    rows += batch.num_rows() as u64;
                                    let df_batch = batch.df_record_batch();
                                    array_mem_size += df_batch.get_array_memory_size() as u64;
                                    estimated_size +=
                                        mito2::memtable::record_batch_estimated_size(df_batch)
                                            as u64;
                                }
                                Err(e) => return Err(BoxedError::new(e)),
                            }
                        }
                        Ok::<(u64, u64, u64), BoxedError>((rows, array_mem_size, estimated_size))
                    }));
                }
                while let Some(task) = scan_futures.next().await {
                    let result = task
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("scan task failed: {e}"),
                                StatusCode::Unexpected,
                            ))
                        })
                        .context(error::BuildCliSnafu)?;
                    let (rows, array_mem_size, estimated_size) =
                        result.context(error::BuildCliSnafu)?;
                    total_rows += rows;
                    total_array_mem_size += array_mem_size;
                    total_estimated_size += estimated_size;
                }
            }

            let elapsed = start.elapsed();
            total_rows_all += total_rows;
            total_elapsed_all += elapsed;

            println!(
                "  [iter {}] {} rows in {:?} ({} partitions), array_mem_size: {}, estimated_size: {}",
                iteration + 1,
                total_rows.to_string().cyan(),
                elapsed,
                num_partitions,
                format_bytes(total_array_mem_size),
                format_bytes(total_estimated_size),
            );

            // Start profiling after the first iteration (warmup) if pprof_after_warmup is set
            #[cfg(unix)]
            if iteration == 0
                && self.pprof_after_warmup
                && self.pprof_file.is_some()
                && profiler_guard.is_none()
            {
                println!(
                    "{} Starting profiling after warmup iteration...",
                    "⚡".yellow()
                );
                profiler_guard = Some(
                    pprof::ProfilerGuardBuilder::default()
                        .frequency(99)
                        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                        .build()
                        .map_err(|e| {
                            BoxedError::new(PlainError::new(
                                format!("Failed to start profiler: {e}"),
                                StatusCode::Unexpected,
                            ))
                        })
                        .context(error::BuildCliSnafu)?,
                );
            }
        }

        // Stop profiling and generate flamegraph if enabled
        #[cfg(unix)]
        if let (Some(guard), Some(pprof_file)) = (profiler_guard, &self.pprof_file) {
            println!("{} Generating flamegraph...", "🔥".yellow());
            match guard.report().build() {
                Ok(report) => {
                    let mut flamegraph_data = Vec::new();
                    if let Err(e) = report.flamegraph(&mut flamegraph_data) {
                        println!("{}: Failed to generate flamegraph: {}", "Error".red(), e);
                    } else if let Err(e) = std::fs::write(pprof_file, flamegraph_data) {
                        println!(
                            "{}: Failed to write flamegraph to {}: {}",
                            "Error".red(),
                            pprof_file.display(),
                            e
                        );
                    } else {
                        println!(
                            "{} Flamegraph saved to {}",
                            "✓".green(),
                            pprof_file.display().to_string().cyan()
                        );
                    }
                }
                Err(e) => {
                    println!("{}: Failed to generate pprof report: {}", "Error".red(), e);
                }
            }
        }

        // Summary
        if self.iterations > 1 {
            let avg_elapsed = total_elapsed_all / self.iterations as u32;
            let avg_rows = total_rows_all / self.iterations as u64;
            println!(
                "\n{} Average: {} rows in {:?} over {} iterations",
                "Summary".green().bold(),
                avg_rows.to_string().cyan(),
                avg_elapsed,
                self.iterations,
            );
        }

        println!("\n{}", "Benchmark completed!".green().bold());
        if let Some(digest) = logical_digest {
            let output = serde_json::json!({
                "status": "ok",
                "schema_version": LOGICAL_ROW_DIGEST_SCHEMA_VERSION,
                "algorithm": "sha256",
                "canonicalization": "logical-metric-row-v1",
                "region_id": region_id.as_u64(),
                "scanner": "series",
                "parallelism": 1,
                "rows": digest.rows,
                "digest": digest.digest_hex(),
                "labels": digest.labels.iter().map(|column| serde_json::json!({
                    "name": column.name,
                    "column_id": column.column_id,
                    "type": "string",
                })).collect::<Vec<_>>(),
                "timestamp": {
                    "name": digest.timestamp.name,
                    "column_id": digest.timestamp.column_id,
                    "unit": "millisecond",
                },
                "value": {
                    "name": digest.value.name,
                    "column_id": digest.value.column_id,
                    "type": "float64",
                    "encoding": "ieee754-bits",
                },
            });
            println!("LOGICAL_ROW_DIGEST_JSON={output}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use datatypes::arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use sqlparser::ast::{BinaryOperator, Expr};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::{RegionId, TimeSeriesDistribution};

    use super::{
        DigestColumn, LogicalRowDigest, ScanConfig, ScanbenchCommand, resolve_filters,
        resolve_projection, scan_distribution, validate_digest_column,
        validate_logical_row_digest_options,
    };
    use crate::error;

    #[test]
    fn test_parse_scan_config_projection_names() {
        let json = r#"{"projection_names":["host","ts"]}"#;
        let config: ScanConfig = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.projection_names,
            Some(vec!["host".to_string(), "ts".to_string()])
        );
        assert_eq!(config.projection, None);
    }

    #[test]
    fn test_resolve_projection_by_indexes() -> error::Result<()> {
        let config = ScanConfig {
            projection: Some(vec![0, 2]),
            projection_names: None,
            filters: None,
            series_row_selector: None,
        };

        let projection = resolve_projection(&config, None)?;
        assert_eq!(projection, Some(vec![0, 2]));
        Ok(())
    }

    #[test]
    fn test_resolve_projection_by_names_without_metadata() {
        let config = ScanConfig {
            projection: None,
            projection_names: Some(vec!["cpu".to_string(), "host".to_string()]),
            filters: None,
            series_row_selector: None,
        };

        let err = resolve_projection(&config, None).unwrap_err();
        assert!(
            err.to_string()
                .contains("Missing region metadata while resolving 'projection_names'")
        );
    }

    #[test]
    fn test_resolve_projection_conflict_fields() {
        let config = ScanConfig {
            projection: Some(vec![0]),
            projection_names: Some(vec!["host".to_string()]),
            filters: None,
            series_row_selector: None,
        };

        let err = resolve_projection(&config, None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("projection"));
        assert!(msg.contains("projection_names"));
    }

    #[test]
    fn test_sqlparser_parse_expr_string() {
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect)
            .try_with_sql("host = 'web-1' AND cpu > 80")
            .unwrap();

        let expr = parser.parse_expr().unwrap();

        match expr {
            Expr::BinaryOp { op, .. } => assert_eq!(op, BinaryOperator::And),
            other => panic!("expected BinaryOp, got: {other:?}"),
        }
    }

    #[test]
    fn test_resolve_filters_uint32_type_conversion() {
        use api::v1::SemanticType;

        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 0));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "table_id",
                    ConcreteDataType::uint32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![1]);
        let metadata = builder.build().unwrap();

        let config = ScanConfig {
            projection: None,
            projection_names: None,
            filters: Some(vec!["table_id = 1117".to_string()]),
            series_row_selector: None,
        };

        let exprs = resolve_filters(&config, &metadata).unwrap();
        assert_eq!(exprs.len(), 1);
        // The expression should contain a UInt32 literal after type conversion.
        let expr_str = format!("{}", exprs[0]);
        assert!(
            expr_str.contains("UInt32(1117)"),
            "Expected UInt32(1117) in expression, got: {expr_str}"
        );
    }

    #[test]
    fn test_digest_column_accepts_nullable_schema_and_checks_rows_later() {
        use api::v1::SemanticType;

        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 0));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 7,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 8,
            })
            .primary_key(vec![7]);
        let metadata = builder.build().unwrap();

        let column = validate_digest_column(
            &metadata,
            "host",
            ConcreteDataType::string_datatype(),
            "label",
        )
        .unwrap();
        assert_eq!(column.column_id, 7);
    }

    #[test]
    fn test_parse_scan_config_filters() {
        let json = r#"{"filters":["host = 'web-1'","cpu > 80"]}"#;
        let config: ScanConfig = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.filters,
            Some(vec!["host = 'web-1'".to_string(), "cpu > 80".to_string()])
        );
    }

    fn digest() -> LogicalRowDigest {
        LogicalRowDigest::new(
            vec![DigestColumn {
                name: "host".to_string(),
                column_id: 1,
            }],
            DigestColumn {
                name: "ts".to_string(),
                column_id: 2,
            },
            DigestColumn {
                name: "value".to_string(),
                column_id: 3,
            },
        )
    }

    fn batch(
        hosts: Vec<Option<&str>>,
        timestamps: Vec<Option<i64>>,
        values: Vec<Option<f64>>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Utf8, true),
                Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                Field::new("value", DataType::Float64, true),
            ])),
            vec![
                Arc::new(StringArray::from(hosts)),
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_logical_row_digest_canonical_framing_golden() {
        let mut digest = digest();
        digest
            .update(&batch(
                vec![Some("web-1"), Some("web-2")],
                vec![Some(1_700_000_000_000), Some(1_700_000_000_001)],
                vec![Some(42.5), Some(-0.0)],
            ))
            .unwrap();
        assert_eq!(digest.rows, 2);
        assert_eq!(
            digest.digest_hex(),
            "1b5f01e9d05e32974a2237ba87d6098d8935024f7513222b577e4681a2f6a179"
        );
    }

    #[test]
    fn test_logical_row_digest_is_independent_of_batch_boundaries() {
        let mut one_batch = digest();
        one_batch
            .update(&batch(
                vec![Some("a"), Some("b")],
                vec![Some(1), Some(2)],
                vec![Some(1.0), Some(2.0)],
            ))
            .unwrap();

        let mut split_batches = digest();
        split_batches
            .update(&batch(vec![Some("a")], vec![Some(1)], vec![Some(1.0)]))
            .unwrap();
        split_batches
            .update(&batch(vec![Some("b")], vec![Some(2)], vec![Some(2.0)]))
            .unwrap();

        assert_eq!(one_batch.rows, split_batches.rows);
        assert_eq!(one_batch.digest_hex(), split_batches.digest_hex());
    }

    #[test]
    fn test_logical_row_digest_models_series_scan_cross_range_merge() {
        type Row = (&'static str, i64, f64);

        // SeriesScan creates one stream per range and its final reader merges them globally by
        // primary key/time. Model that output before varying the RecordBatch grouping presented
        // to the digest, rather than merely splitting an already ordered input batch.
        let update_from_series_output =
            |digest: &mut LogicalRowDigest, range_groups: Vec<Vec<Row>>, batch_sizes: &[usize]| {
                let mut rows: Vec<_> = range_groups.into_iter().flatten().collect();
                rows.sort_by(|left, right| left.0.cmp(right.0).then(left.1.cmp(&right.1)));

                let mut offset = 0;
                for &batch_size in batch_sizes {
                    let batch_rows = &rows[offset..offset + batch_size];
                    digest
                        .update(&batch(
                            batch_rows.iter().map(|row| Some(row.0)).collect(),
                            batch_rows.iter().map(|row| Some(row.1)).collect(),
                            batch_rows.iter().map(|row| Some(row.2)).collect(),
                        ))
                        .unwrap();
                    offset += batch_size;
                }
                assert_eq!(offset, rows.len());
            };

        let mut one_range = digest();
        update_from_series_output(
            &mut one_range,
            vec![vec![
                ("a", 1, 1.0),
                ("a", 2, 2.0),
                ("b", 1, 3.0),
                ("b", 2, 4.0),
            ]],
            &[4],
        );

        let mut disjoint_ranges = digest();
        update_from_series_output(
            &mut disjoint_ranges,
            vec![
                vec![("b", 1, 3.0), ("b", 2, 4.0)],
                vec![("a", 1, 1.0), ("a", 2, 2.0)],
            ],
            &[1, 3],
        );

        assert_eq!(one_range.rows, disjoint_ranges.rows);
        assert_eq!(one_range.digest_hex(), disjoint_ranges.digest_hex());
    }

    #[test]
    fn test_logical_row_digest_label_order_and_framing_are_significant() {
        let make_digest = |labels: Vec<&str>| {
            LogicalRowDigest::new(
                labels
                    .into_iter()
                    .enumerate()
                    .map(|(column_id, name)| DigestColumn {
                        name: name.to_string(),
                        column_id: column_id as u32,
                    })
                    .collect(),
                DigestColumn {
                    name: "ts".to_string(),
                    column_id: 2,
                },
                DigestColumn {
                    name: "value".to_string(),
                    column_id: 3,
                },
            )
        };
        let two_labels = |first: &str, second: &str| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::Utf8, false),
                    Field::new("b", DataType::Utf8, false),
                    Field::new(
                        "ts",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("value", DataType::Float64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![first])),
                    Arc::new(StringArray::from(vec![second])),
                    Arc::new(TimestampMillisecondArray::from(vec![1])),
                    Arc::new(Float64Array::from(vec![1.0])),
                ],
            )
            .unwrap()
        };

        let mut ordered = make_digest(vec!["a", "b"]);
        ordered.update(&two_labels("a", "bc")).unwrap();
        let mut reordered = make_digest(vec!["b", "a"]);
        let reordered_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("b", DataType::Utf8, false),
                Field::new("a", DataType::Utf8, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Float64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["bc"])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(TimestampMillisecondArray::from(vec![1])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        reordered.update(&reordered_batch).unwrap();
        let mut different_boundaries = make_digest(vec!["a", "b"]);
        different_boundaries.update(&two_labels("ab", "c")).unwrap();

        assert_ne!(ordered.digest_hex(), reordered.digest_hex());
        assert_ne!(ordered.digest_hex(), different_boundaries.digest_hex());
    }

    #[test]
    fn test_logical_row_digest_distinguishes_positive_and_negative_zero() {
        let mut positive = digest();
        positive
            .update(&batch(vec![Some("a")], vec![Some(1)], vec![Some(0.0)]))
            .unwrap();
        let mut negative = digest();
        negative
            .update(&batch(vec![Some("a")], vec![Some(1)], vec![Some(-0.0)]))
            .unwrap();

        assert_ne!(positive.digest_hex(), negative.digest_hex());
    }

    #[test]
    fn test_logical_row_digest_rejects_null_missing_and_wrong_types() {
        assert!(
            digest()
                .update(&batch(vec![None], vec![Some(1)], vec![Some(1.0)]))
                .is_err()
        );
        assert!(
            digest()
                .update(&batch(vec![Some("a")], vec![None], vec![Some(1.0)]))
                .is_err()
        );
        assert!(
            digest()
                .update(&batch(vec![Some("a")], vec![Some(1)], vec![None]))
                .is_err()
        );

        let missing = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Utf8, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(TimestampMillisecondArray::from(vec![1])),
            ],
        )
        .unwrap();
        assert!(digest().update(&missing).is_err());

        let wrong_label = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Int64, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Float64, false),
            ])),
            vec![
                Arc::new(datatypes::arrow::array::Int64Array::from(vec![1])),
                Arc::new(TimestampMillisecondArray::from(vec![1])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        assert!(digest().update(&wrong_label).is_err());

        let wrong_timestamp = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(datatypes::arrow::array::Int64Array::from(vec![1])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        assert!(digest().update(&wrong_timestamp).is_err());

        let wrong_value = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Utf8, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(TimestampMillisecondArray::from(vec![1])),
                Arc::new(datatypes::arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        assert!(digest().update(&wrong_value).is_err());
    }

    fn digest_command() -> ScanbenchCommand {
        ScanbenchCommand {
            config: PathBuf::from("config.toml"),
            region_id: "1".to_string(),
            table_dir: "table".to_string(),
            scanner: "series".to_string(),
            scan_config: None,
            parallelism: 1,
            iterations: 1,
            path_type: "bare".to_string(),
            verbose: false,
            pprof_file: None,
            enable_wal: false,
            pprof_after_warmup: false,
            logical_row_digest: true,
            digest_labels: vec!["host".to_string()],
            digest_timestamp_column: Some("ts".to_string()),
            digest_value_column: Some("value".to_string()),
        }
    }

    #[test]
    fn test_logical_row_digest_rejects_invalid_mode_combinations() {
        let mut command = digest_command();
        assert!(validate_logical_row_digest_options(&command).is_ok());

        command.scanner = "seq".to_string();
        assert!(validate_logical_row_digest_options(&command).is_err());
        command.scanner = "unordered".to_string();
        assert!(validate_logical_row_digest_options(&command).is_err());
        command.scanner = "series".to_string();
        command.parallelism = 2;
        assert!(validate_logical_row_digest_options(&command).is_err());
        command.parallelism = 1;
        command.digest_labels = vec!["host".to_string(), "host".to_string()];
        assert!(validate_logical_row_digest_options(&command).is_err());
        command.digest_labels = vec!["bad-name".to_string()];
        assert!(validate_logical_row_digest_options(&command).is_err());
        command.digest_labels = vec!["ts".to_string()];
        assert!(validate_logical_row_digest_options(&command).is_err());
    }

    #[test]
    fn test_logical_row_digest_selects_series_distribution() {
        let command = digest_command();
        assert!(matches!(
            scan_distribution(&command.scanner, command.logical_row_digest).unwrap(),
            Some(TimeSeriesDistribution::PerSeries)
        ));
    }
}
