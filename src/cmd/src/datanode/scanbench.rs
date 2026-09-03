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
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
use datafusion_physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::arrow::compute;
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
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use sqlparser::ast::ExprWithAlias as SqlExprWithAlias;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use store_api::metadata::RegionMetadata;
use store_api::path_utils::WAL_DIR;
use store_api::region_engine::{PrepareRequest, QueryScanContext, RegionEngine};
use store_api::region_request::{RegionOpenRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest, TimeSeriesDistribution, TimeSeriesRowSelector};
use tokio::fs;

use crate::datanode::tool_util::{
    build_object_store, format_bytes, parse_config, parse_path_type, parse_region_id,
};
use crate::error;

/// Displays a scanner using DataFusion's verbose explain format.
struct VerboseScannerDisplay<'a, T: ?Sized>(&'a T);

impl<T: DisplayAs + ?Sized> fmt::Display for VerboseScannerDisplay<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Verbose, f)
    }
}

struct PartitionScanStats {
    partition: usize,
    rows: u64,
    batches: u64,
    array_mem_size: u64,
    estimated_size: u64,
    first_batch_elapsed: Option<Duration>,
    elapsed: Duration,
}

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
    #[clap(long, value_name = "FILE", conflicts_with = "scan_configs")]
    scan_config: Option<PathBuf>,

    /// Path to a JSON array of scan request configs, each executed once
    #[clap(long, value_name = "FILE", conflicts_with = "scan_config")]
    scan_configs: Option<PathBuf>,

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

    /// Output structured benchmark and analyze results as JSON
    #[clap(long, value_name = "FILE")]
    result_file: Option<PathBuf>,

    /// Enable WAL replay when opening the region.
    #[clap(long, default_value_t = false)]
    enable_wal: bool,

    /// Start pprof after the first iteration (use first iteration as warmup).
    #[clap(long, default_value_t = false)]
    pprof_after_warmup: bool,
}

/// JSON config for scan request parameters.
#[derive(Debug, Clone, Deserialize, Default)]
struct ScanConfig {
    name: Option<String>,
    projection: Option<Vec<usize>>,
    projection_names: Option<Vec<String>>,
    filters: Option<Vec<String>>,
    series_row_selector: Option<String>,
}

struct ScanConfigSet {
    configs: Vec<ScanConfig>,
    is_suite: bool,
}

impl ScanConfigSet {
    fn run_count(&self, iterations: usize) -> usize {
        if self.is_suite {
            self.configs.len()
        } else {
            iterations
        }
    }

    fn query_index(&self, iteration: usize) -> usize {
        if self.is_suite { iteration } else { 0 }
    }
}

struct ResolvedScanConfig {
    name: String,
    projection: Option<Vec<usize>>,
    filters: Vec<DfExpr>,
    series_row_selector: Option<TimeSeriesRowSelector>,
}

struct QueryRunSummary {
    name: String,
    runs: u64,
    total_rows: u64,
    total_elapsed: Duration,
}

impl QueryRunSummary {
    fn new(name: String) -> Self {
        Self {
            name,
            runs: 0,
            total_rows: 0,
            total_elapsed: Duration::ZERO,
        }
    }

    fn record(&mut self, rows: u64, elapsed: Duration) {
        self.runs += 1;
        self.total_rows += rows;
        self.total_elapsed += elapsed;
    }

    fn mean_rows(&self) -> u64 {
        self.total_rows.checked_div(self.runs).unwrap_or_default()
    }

    fn mean_elapsed(&self) -> Duration {
        self.total_elapsed
            .checked_div(self.runs as u32)
            .unwrap_or_default()
    }
}

const RESULT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
struct ScanbenchResult {
    format_version: u32,
    started_at_unix_ms: u64,
    benchmark: BenchmarkMetadata,
    runs: Vec<ScanRunResult>,
    summary: BenchmarkResultSummary,
}

#[derive(Debug, Serialize)]
struct BenchmarkMetadata {
    scanner: String,
    region_id: String,
    region_id_u64: u64,
    table_dir: String,
    path_type: String,
    parallelism: usize,
    enable_wal: bool,
    config_mode: String,
    run_count: usize,
}

#[derive(Debug, Serialize)]
struct NormalizedScanConfig {
    name: String,
    projection: Option<Vec<usize>>,
    filters: Vec<String>,
    series_row_selector: Option<String>,
}

#[derive(Debug, Serialize)]
struct ScanRunResult {
    iteration: usize,
    query_index: usize,
    name: String,
    config: NormalizedScanConfig,
    rows: u64,
    batches: u64,
    setup_elapsed_ns: u64,
    scan_elapsed_ns: u64,
    elapsed_ns: u64,
    array_mem_size_bytes: u64,
    estimated_size_bytes: u64,
    partitions: Vec<PartitionResult>,
    scanner_explain: String,
}

#[derive(Debug, Serialize)]
struct PartitionResult {
    partition: usize,
    rows: u64,
    batches: u64,
    array_mem_size_bytes: u64,
    estimated_size_bytes: u64,
    first_batch_elapsed_ns: Option<u64>,
    elapsed_ns: u64,
}

#[derive(Debug, Serialize)]
struct BenchmarkResultSummary {
    runs: u64,
    total_rows: u64,
    total_elapsed_ns: u64,
    mean_rows: u64,
    mean_elapsed_ns: u64,
    queries: Vec<QueryResultSummary>,
}

#[derive(Debug, Serialize)]
struct QueryResultSummary {
    query_index: usize,
    name: String,
    runs: u64,
    total_rows: u64,
    total_elapsed_ns: u64,
    mean_rows: u64,
    mean_elapsed_ns: u64,
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

fn started_at_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn result_summary(
    total_rows: u64,
    total_elapsed: Duration,
    summaries: &[QueryRunSummary],
) -> BenchmarkResultSummary {
    let runs = summaries.iter().map(|summary| summary.runs).sum::<u64>();
    BenchmarkResultSummary {
        runs,
        total_rows,
        total_elapsed_ns: duration_ns(total_elapsed),
        mean_rows: total_rows.checked_div(runs).unwrap_or_default(),
        mean_elapsed_ns: duration_ns(total_elapsed.checked_div(runs as u32).unwrap_or_default()),
        queries: summaries
            .iter()
            .enumerate()
            .map(|(index, summary)| QueryResultSummary {
                query_index: index + 1,
                name: summary.name.clone(),
                runs: summary.runs,
                total_rows: summary.total_rows,
                total_elapsed_ns: duration_ns(summary.total_elapsed),
                mean_rows: summary.mean_rows(),
                mean_elapsed_ns: duration_ns(summary.mean_elapsed()),
            })
            .collect(),
    }
}

async fn write_result_file(path: &PathBuf, result: &ScanbenchResult) -> error::Result<()> {
    let content = serde_json::to_vec_pretty(result).context(error::SerdeJsonSnafu)?;
    fs::write(path, content).await.context(error::FileIoSnafu)
}

fn validate_scan_config_suite(
    mut configs: Vec<ScanConfig>,
    iterations: usize,
) -> error::Result<Vec<ScanConfig>> {
    if iterations != 1 {
        return Err(error::IllegalConfigSnafu {
            msg: format!(
                "--iterations cannot be used with --scan-configs (got {iterations}); the array length defines the run count"
            ),
        }
        .build());
    }
    if configs.is_empty() {
        return Err(error::IllegalConfigSnafu {
            msg: "--scan-configs file must contain at least one scan config".to_string(),
        }
        .build());
    }

    let mut names = HashSet::with_capacity(configs.len());
    for (index, config) in configs.iter_mut().enumerate() {
        let name = match config.name.take() {
            Some(name) => {
                let name = name.trim();
                if name.is_empty() {
                    return Err(error::IllegalConfigSnafu {
                        msg: format!("scan config at index {index} has an empty name"),
                    }
                    .build());
                }
                name.to_string()
            }
            None => format!("query-{:03}", index + 1),
        };
        if !names.insert(name.clone()) {
            return Err(error::IllegalConfigSnafu {
                msg: format!("duplicate scan config name '{name}' at index {index}"),
            }
            .build());
        }
        config.name = Some(name);
    }

    Ok(configs)
}

fn resolve_series_row_selector(
    scan_config: &ScanConfig,
) -> error::Result<Option<TimeSeriesRowSelector>> {
    match scan_config.series_row_selector.as_deref() {
        Some("last_row") => Ok(Some(TimeSeriesRowSelector::LastRow)),
        Some(other) => Err(error::IllegalConfigSnafu {
            msg: format!("Unknown series_row_selector '{other}'"),
        }
        .build()),
        None => Ok(None),
    }
}

fn resolve_scan_configs(
    config_set: &ScanConfigSet,
    metadata: &RegionMetadata,
) -> error::Result<Vec<ResolvedScanConfig>> {
    config_set
        .configs
        .iter()
        .enumerate()
        .map(|(index, config)| {
            let name = config
                .name
                .clone()
                .unwrap_or_else(|| format!("query-{:03}", index + 1));
            let projection = resolve_projection(config, Some(metadata)).map_err(|err| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid scan config at index {index} ('{name}'): {err}"),
                }
                .build()
            })?;
            let filters = resolve_filters(config, metadata).map_err(|err| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid scan config at index {index} ('{name}'): {err}"),
                }
                .build()
            })?;
            let series_row_selector = resolve_series_row_selector(config).map_err(|err| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid scan config at index {index} ('{name}'): {err}"),
                }
                .build()
            })?;
            Ok(ResolvedScanConfig {
                name,
                projection,
                filters,
                series_row_selector,
            })
        })
        .collect()
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
    async fn load_scan_config_set(&self) -> error::Result<ScanConfigSet> {
        match (&self.scan_config, &self.scan_configs) {
            (Some(_), Some(_)) => Err(error::IllegalConfigSnafu {
                msg: "--scan-config and --scan-configs are mutually exclusive".to_string(),
            }
            .build()),
            (Some(path), None) => {
                let content = tokio::fs::read_to_string(path)
                    .await
                    .context(error::FileIoSnafu)?;
                let config =
                    serde_json::from_str::<ScanConfig>(&content).context(error::SerdeJsonSnafu)?;
                Ok(ScanConfigSet {
                    configs: vec![config],
                    is_suite: false,
                })
            }
            (None, Some(path)) => {
                if self.iterations != 1 {
                    return Err(error::IllegalConfigSnafu {
                        msg: format!(
                            "--iterations cannot be used with --scan-configs (got {}); the array length defines the run count",
                            self.iterations
                        ),
                    }
                    .build());
                }
                let content = tokio::fs::read_to_string(path)
                    .await
                    .context(error::FileIoSnafu)?;
                let configs = serde_json::from_str::<Vec<ScanConfig>>(&content)
                    .context(error::SerdeJsonSnafu)?;
                Ok(ScanConfigSet {
                    configs: validate_scan_config_suite(configs, self.iterations)?,
                    is_suite: true,
                })
            }
            (None, None) => Ok(ScanConfigSet {
                configs: vec![ScanConfig::default()],
                is_suite: false,
            }),
        }
    }

    pub async fn run(&self) -> error::Result<()> {
        if self.verbose {
            common_telemetry::init_default_ut_logging();
        }

        println!("{}", "Starting scanbench...".cyan().bold());
        let benchmark_started_at_unix_ms = started_at_unix_ms();

        let scan_config_set = self.load_scan_config_set().await?;

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

        let metadata = engine
            .get_metadata(region_id)
            .await
            .map_err(BoxedError::new)
            .context(error::BuildCliSnafu)?;
        let scan_configs = resolve_scan_configs(&scan_config_set, &metadata)?;

        // Build scan request
        let distribution = match self.scanner.as_str() {
            "seq" => None,
            "unordered" => Some(TimeSeriesDistribution::TimeWindowed),
            "series" => Some(TimeSeriesDistribution::PerSeries),
            other => {
                return Err(error::IllegalConfigSnafu {
                    msg: format!(
                        "Unknown scanner type '{}', expected: seq, unordered, series",
                        other
                    ),
                }
                .build());
            }
        };

        let run_count = scan_config_set.run_count(self.iterations);
        if scan_config_set.is_suite {
            println!(
                "{} Scanner: {}, Parallelism: {}, Queries: {}",
                "ℹ".blue(),
                self.scanner,
                self.parallelism,
                run_count,
            );
        } else {
            println!(
                "{} Scanner: {}, Parallelism: {}, Iterations: {}",
                "ℹ".blue(),
                self.scanner,
                self.parallelism,
                run_count,
            );
        }

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
        let mut run_results = Vec::with_capacity(if self.result_file.is_some() {
            run_count
        } else {
            0
        });
        let mut query_summaries = scan_configs
            .iter()
            .map(|config| QueryRunSummary::new(config.name.clone()))
            .collect::<Vec<_>>();
        let collect_scanner_explain = self.verbose || self.result_file.is_some();

        for iteration in 0..run_count {
            let query_index = scan_config_set.query_index(iteration);
            let scan_config = &scan_configs[query_index];
            let request = ScanRequest {
                projection: scan_config.projection.clone(),
                filters: scan_config.filters.clone(),
                series_row_selector: scan_config.series_row_selector,
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

            if self.parallelism > 1 {
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
            let ctx = QueryScanContext {
                explain_verbose: collect_scanner_explain,
            };
            let metrics_set = ExecutionPlanMetricsSet::new();

            let mut scan_futures = FuturesUnordered::new();
            let setup_elapsed = start.elapsed();
            let scan_start = Instant::now();

            for partition_idx in 0..num_partitions {
                let mut stream = scanner
                    .scan_partition(&ctx, &metrics_set, partition_idx)
                    .map_err(BoxedError::new)
                    .context(error::BuildCliSnafu)?;

                scan_futures.push(tokio::spawn(async move {
                    let partition_start = Instant::now();
                    let mut rows = 0u64;
                    let mut batches = 0u64;
                    let mut array_mem_size = 0u64;
                    let mut estimated_size = 0u64;
                    let mut first_batch_elapsed = None;
                    while let Some(batch_result) = stream.next().await {
                        match batch_result {
                            Ok(batch) => {
                                if first_batch_elapsed.is_none() {
                                    first_batch_elapsed = Some(partition_start.elapsed());
                                }
                                batches += 1;
                                rows += batch.num_rows() as u64;
                                let df_batch = batch.df_record_batch();
                                array_mem_size += df_batch.get_array_memory_size() as u64;
                                estimated_size +=
                                    mito2::memtable::record_batch_estimated_size(df_batch) as u64;
                            }
                            Err(e) => {
                                return Err(BoxedError::new(e));
                            }
                        }
                    }
                    Ok::<PartitionScanStats, BoxedError>(PartitionScanStats {
                        partition: partition_idx,
                        rows,
                        batches,
                        array_mem_size,
                        estimated_size,
                        first_batch_elapsed,
                        elapsed: partition_start.elapsed(),
                    })
                }));
            }

            let mut total_rows = 0u64;
            let mut total_batches = 0u64;
            let mut total_array_mem_size = 0u64;
            let mut total_estimated_size = 0u64;
            let mut partition_stats = Vec::with_capacity(num_partitions);
            while let Some(task) = scan_futures.next().await {
                let result = task
                    .map_err(|e| {
                        BoxedError::new(PlainError::new(
                            format!("scan task failed: {e}"),
                            StatusCode::Unexpected,
                        ))
                    })
                    .context(error::BuildCliSnafu)?;
                let stats = result.context(error::BuildCliSnafu)?;
                total_rows += stats.rows;
                total_batches += stats.batches;
                total_array_mem_size += stats.array_mem_size;
                total_estimated_size += stats.estimated_size;
                partition_stats.push(stats);
            }
            let scan_elapsed = scan_start.elapsed();

            let elapsed = start.elapsed();
            total_rows_all += total_rows;
            total_elapsed_all += elapsed;
            query_summaries[query_index].record(total_rows, elapsed);

            let query_display = if scan_config_set.is_suite {
                format!(
                    " [query {}/{}: {}]",
                    query_index + 1,
                    scan_configs.len(),
                    scan_config.name
                )
            } else {
                String::new()
            };

            println!(
                "  [iter {}]{} {} rows in {:?} ({} partitions), array_mem_size: {}, estimated_size: {}",
                iteration + 1,
                query_display,
                total_rows.to_string().cyan(),
                elapsed,
                num_partitions,
                format_bytes(total_array_mem_size),
                format_bytes(total_estimated_size),
            );

            if collect_scanner_explain {
                partition_stats.sort_unstable_by_key(|stats| stats.partition);
            }

            if self.verbose {
                for stats in &partition_stats {
                    let first_batch = stats
                        .first_batch_elapsed
                        .map(|elapsed| format!("{elapsed:?}"))
                        .unwrap_or_else(|| "n/a".to_string());
                    println!(
                        "    partition {}: rows={}, batches={}, first_batch={}, elapsed={:?}, array_mem_size={}, estimated_size={}",
                        stats.partition,
                        stats.rows,
                        stats.batches,
                        first_batch,
                        stats.elapsed,
                        format_bytes(stats.array_mem_size),
                        format_bytes(stats.estimated_size),
                    );
                }
                if !partition_stats.is_empty() {
                    let total_partition_elapsed = partition_stats
                        .iter()
                        .map(|stats| stats.elapsed)
                        .sum::<Duration>();
                    let mean_partition_elapsed =
                        total_partition_elapsed / partition_stats.len() as u32;
                    let max_partition_elapsed = partition_stats
                        .iter()
                        .map(|stats| stats.elapsed)
                        .max()
                        .unwrap_or_default();
                    let skew = max_partition_elapsed.as_secs_f64()
                        / mean_partition_elapsed.as_secs_f64().max(f64::EPSILON);
                    println!(
                        "  {} Timing: setup={:?}, scan={:?}, mean_partition={:?}, max_partition={:?}, partition_skew={:.2}x",
                        "ℹ".blue(),
                        setup_elapsed,
                        scan_elapsed,
                        mean_partition_elapsed,
                        max_partition_elapsed,
                        skew,
                    );
                }
            }

            let scanner_explain = if collect_scanner_explain {
                format!("{}", VerboseScannerDisplay(scanner.as_ref()))
            } else {
                String::new()
            };
            if self.verbose {
                println!("  {} Scanner explain: {}", "ℹ".blue(), scanner_explain);
            }

            if self.result_file.is_some() {
                let source_config = &scan_config_set.configs[query_index];
                run_results.push(ScanRunResult {
                    iteration: iteration + 1,
                    query_index: query_index + 1,
                    name: scan_config.name.clone(),
                    config: NormalizedScanConfig {
                        name: scan_config.name.clone(),
                        projection: scan_config.projection.clone(),
                        filters: source_config.filters.clone().unwrap_or_default(),
                        series_row_selector: source_config.series_row_selector.clone(),
                    },
                    rows: total_rows,
                    batches: total_batches,
                    setup_elapsed_ns: duration_ns(setup_elapsed),
                    scan_elapsed_ns: duration_ns(scan_elapsed),
                    elapsed_ns: duration_ns(elapsed),
                    array_mem_size_bytes: total_array_mem_size,
                    estimated_size_bytes: total_estimated_size,
                    partitions: partition_stats
                        .iter()
                        .map(|stats| PartitionResult {
                            partition: stats.partition,
                            rows: stats.rows,
                            batches: stats.batches,
                            array_mem_size_bytes: stats.array_mem_size,
                            estimated_size_bytes: stats.estimated_size,
                            first_batch_elapsed_ns: stats.first_batch_elapsed.map(duration_ns),
                            elapsed_ns: duration_ns(stats.elapsed),
                        })
                        .collect(),
                    scanner_explain,
                });
            }

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
        if scan_config_set.is_suite {
            let avg_elapsed = total_elapsed_all / run_count as u32;
            let avg_rows = total_rows_all / run_count as u64;
            println!(
                "\n{} Overall average: {} rows in {:?} over {} queries",
                "Summary".green().bold(),
                avg_rows.to_string().cyan(),
                avg_elapsed,
                run_count,
            );
            println!("{} Per-query:", "Summary".green().bold());
            for (index, summary) in query_summaries.iter().enumerate() {
                println!(
                    "  [{}] {}: runs={}, mean_rows={}, mean_elapsed={:?}",
                    index + 1,
                    summary.name,
                    summary.runs,
                    summary.mean_rows(),
                    summary.mean_elapsed(),
                );
            }
        } else if run_count > 1 {
            let avg_elapsed = total_elapsed_all / run_count as u32;
            let avg_rows = total_rows_all / run_count as u64;
            println!(
                "\n{} Average: {} rows in {:?} over {} iterations",
                "Summary".green().bold(),
                avg_rows.to_string().cyan(),
                avg_elapsed,
                run_count,
            );
        }

        if let Some(result_file) = &self.result_file {
            let result = ScanbenchResult {
                format_version: RESULT_FORMAT_VERSION,
                started_at_unix_ms: benchmark_started_at_unix_ms,
                benchmark: BenchmarkMetadata {
                    scanner: self.scanner.clone(),
                    region_id: self.region_id.clone(),
                    region_id_u64: region_id.as_u64(),
                    table_dir: self.table_dir.clone(),
                    path_type: self.path_type.clone(),
                    parallelism: self.parallelism,
                    enable_wal: self.enable_wal,
                    config_mode: if scan_config_set.is_suite {
                        "suite".to_string()
                    } else {
                        "single".to_string()
                    },
                    run_count,
                },
                runs: run_results,
                summary: result_summary(total_rows_all, total_elapsed_all, &query_summaries),
            };
            write_result_file(result_file, &result).await?;
            println!(
                "{} Results saved to {}",
                "✓".green(),
                result_file.display().to_string().cyan()
            );
        }

        println!("\n{}", "Benchmark completed!".green().bold());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use sqlparser::ast::{BinaryOperator, Expr};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::{
        BenchmarkMetadata, BenchmarkResultSummary, NormalizedScanConfig, PartitionResult,
        ScanConfig, ScanRunResult, ScanbenchResult, resolve_filters, resolve_projection,
        validate_scan_config_suite, write_result_file,
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
            name: None,
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
            name: None,
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
            name: None,
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
            name: None,
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
    fn test_parse_scan_config_filters() {
        let json = r#"{"filters":["host = 'web-1'","cpu > 80"]}"#;
        let config: ScanConfig = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.filters,
            Some(vec!["host = 'web-1'".to_string(), "cpu > 80".to_string()])
        );
    }

    #[test]
    fn test_parse_and_validate_scan_config_suite() {
        let json = r#"[
            {"name":" cold ","filters":["host = 'web-1'"]},
            {"projection_names":["host","cpu"]}
        ]"#;
        let configs: Vec<ScanConfig> = serde_json::from_str(json).unwrap();
        let configs = validate_scan_config_suite(configs, 1).unwrap();

        assert_eq!(2, configs.len());
        assert_eq!(Some("cold"), configs[0].name.as_deref());
        assert_eq!(Some("query-002"), configs[1].name.as_deref());
        assert_eq!(
            Some(&vec!["host = 'web-1'".to_string()]),
            configs[0].filters.as_ref()
        );
    }

    #[test]
    fn test_validate_scan_config_suite_rejects_invalid_input() {
        let empty = validate_scan_config_suite(Vec::new(), 1)
            .unwrap_err()
            .to_string();
        assert!(empty.contains("at least one"));

        let iterations = validate_scan_config_suite(vec![ScanConfig::default()], 2)
            .unwrap_err()
            .to_string();
        assert!(iterations.contains("--iterations"));

        let configs: Vec<ScanConfig> =
            serde_json::from_str(r#"[{"name":"query"},{"name":" query "}]"#).unwrap();
        let duplicate = validate_scan_config_suite(configs, 1)
            .unwrap_err()
            .to_string();
        assert!(duplicate.contains("duplicate"));

        let configs: Vec<ScanConfig> = serde_json::from_str(r#"[{"name":"  "}]"#).unwrap();
        let blank = validate_scan_config_suite(configs, 1)
            .unwrap_err()
            .to_string();
        assert!(blank.contains("empty name"));
    }

    #[tokio::test]
    async fn test_write_result_file_overwrites_complete_result() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scanbench-result.json");
        tokio::fs::write(&path, b"old result").await.unwrap();

        let result = ScanbenchResult {
            format_version: 1,
            started_at_unix_ms: 42,
            benchmark: BenchmarkMetadata {
                scanner: "seq".to_string(),
                region_id: "1024:0".to_string(),
                region_id_u64: RegionId::new(1024, 0).as_u64(),
                table_dir: "greptime/public/1024".to_string(),
                path_type: "bare".to_string(),
                parallelism: 8,
                enable_wal: false,
                config_mode: "suite".to_string(),
                run_count: 1,
            },
            runs: vec![ScanRunResult {
                iteration: 1,
                query_index: 1,
                name: "cpu-host-1".to_string(),
                config: NormalizedScanConfig {
                    name: "cpu-host-1".to_string(),
                    projection: Some(vec![1, 2]),
                    filters: vec!["hostname = 'host_1'".to_string()],
                    series_row_selector: None,
                },
                rows: 100,
                batches: 2,
                setup_elapsed_ns: 10,
                scan_elapsed_ns: 80,
                elapsed_ns: 90,
                array_mem_size_bytes: 1024,
                estimated_size_bytes: 512,
                partitions: vec![PartitionResult {
                    partition: 0,
                    rows: 100,
                    batches: 2,
                    array_mem_size_bytes: 1024,
                    estimated_size_bytes: 512,
                    first_batch_elapsed_ns: Some(20),
                    elapsed_ns: 80,
                }],
                scanner_explain: "SeqScanExec: prefilter_columns_read=[hostname]".to_string(),
            }],
            summary: BenchmarkResultSummary {
                runs: 1,
                total_rows: 100,
                total_elapsed_ns: 90,
                mean_rows: 100,
                mean_elapsed_ns: 90,
                queries: vec![],
            },
        };

        write_result_file(&path, &result).await.unwrap();
        let content = tokio::fs::read(&path).await.unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&content).unwrap();

        assert_eq!(1, actual["format_version"]);
        assert_eq!("suite", actual["benchmark"]["config_mode"]);
        assert_eq!(2, actual["runs"][0]["config"]["projection"][1]);
        assert_eq!(100, actual["runs"][0]["partitions"][0]["rows"]);
        assert_eq!(
            "SeqScanExec: prefilter_columns_read=[hostname]",
            actual["runs"][0]["scanner_explain"]
        );
        assert_eq!(90, actual["summary"]["mean_elapsed_ns"]);
    }
}
