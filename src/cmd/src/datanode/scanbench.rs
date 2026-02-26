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

use std::collections::HashMap;
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
use datafusion::logical_expr::Expr as DfExpr;
use datafusion_common::ToDFSchema;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
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
use snafu::{OptionExt, ResultExt};
use sqlparser::ast::ExprWithAlias as SqlExprWithAlias;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use store_api::metadata::RegionMetadata;
use store_api::path_utils::WAL_DIR;
use store_api::region_engine::{PrepareRequest, QueryScanContext, RegionEngine};
use store_api::region_request::{PathType, RegionOpenRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest, TimeSeriesDistribution, TimeSeriesRowSelector};
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

    /// Force reading the region in flat format.
    #[clap(long, default_value_t = false)]
    force_flat_format: bool,

    /// Enable WAL replay when opening the region.
    #[clap(long, default_value_t = false)]
    enable_wal: bool,
}

/// JSON config for scan request parameters.
#[derive(Debug, Deserialize, Default)]
struct ScanConfig {
    projection: Option<Vec<usize>>,
    projection_names: Option<Vec<String>>,
    filters: Option<Vec<String>>,
    series_row_selector: Option<String>,
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

    filters
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
        .collect()
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
        let projection = resolve_projection(&scan_config, Some(&metadata))?;
        let filters = resolve_filters(&scan_config, &metadata)?;

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
            "{} Scanner: {}, Parallelism: {}, Iterations: {}, Force flat format: {}",
            "ℹ".blue(),
            self.scanner,
            self.parallelism,
            self.iterations,
            self.force_flat_format,
        );

        // Start profiling if pprof_file is specified
        #[cfg(unix)]
        let profiler_guard = if self.pprof_file.is_some() {
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

        for iteration in 0..self.iterations {
            let request = ScanRequest {
                projection: projection.clone(),
                filters: filters.clone(),
                series_row_selector,
                distribution,
                force_flat_format: self.force_flat_format,
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
                    partition.sort_by(|a, b| a.start.cmp(&b.start));
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
            let ctx = QueryScanContext::default();
            let metrics_set = ExecutionPlanMetricsSet::new();

            let mut scan_futures = FuturesUnordered::new();

            for partition_idx in 0..num_partitions {
                let mut stream = scanner
                    .scan_partition(&ctx, &metrics_set, partition_idx)
                    .map_err(BoxedError::new)
                    .context(error::BuildCliSnafu)?;

                scan_futures.push(tokio::spawn(async move {
                    let mut rows = 0u64;
                    while let Some(batch_result) = stream.next().await {
                        match batch_result {
                            Ok(batch) => {
                                rows += batch.num_rows() as u64;
                            }
                            Err(e) => {
                                return Err(BoxedError::new(e));
                            }
                        }
                    }
                    Ok::<u64, BoxedError>(rows)
                }));
            }

            let mut total_rows = 0u64;
            while let Some(task) = scan_futures.next().await {
                let result = task
                    .map_err(|e| {
                        BoxedError::new(PlainError::new(
                            format!("scan task failed: {e}"),
                            StatusCode::Unexpected,
                        ))
                    })
                    .context(error::BuildCliSnafu)?;
                let rows = result.context(error::BuildCliSnafu)?;
                total_rows += rows;
            }

            let elapsed = start.elapsed();
            total_rows_all += total_rows;
            total_elapsed_all += elapsed;

            println!(
                "  [iter {}] {} rows in {:?} ({} partitions)",
                iteration + 1,
                total_rows.to_string().cyan(),
                elapsed,
                num_partitions,
            );
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{BinaryOperator, Expr};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::{ScanConfig, resolve_projection};
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
    fn test_parse_scan_config_filters() {
        let json = r#"{"filters":["host = 'web-1'","cpu > 80"]}"#;
        let config: ScanConfig = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.filters,
            Some(vec!["host = 'web-1'".to_string(), "cpu > 80".to_string()])
        );
    }
}
