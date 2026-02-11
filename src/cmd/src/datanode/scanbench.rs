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
use common_meta::cache::{new_schema_cache, new_table_schema_cache};
use common_meta::key::SchemaMetadataManager;
use common_meta::kv_backend::memory::MemoryKvBackend;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log_store::noop::log_store::NoopLogStore;
use mito2::engine::MitoEngine;
use mito2::sst::file_ref::FileReferenceManager;
use moka::future::CacheBuilder;
use object_store::manager::ObjectStoreManager;
use query::optimizer::parallelize_scan::ParallelizeScan;
use serde::Deserialize;
use store_api::region_engine::{PrepareRequest, QueryScanContext, RegionEngine};
use store_api::region_request::{PathType, RegionOpenRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest, TimeSeriesDistribution, TimeSeriesRowSelector};

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
}

/// JSON config for scan request parameters.
#[derive(Debug, Deserialize, Default)]
struct ScanConfig {
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    series_row_selector: Option<String>,
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
        let (store_cfg, mito_config) = parse_config(&self.config)?;
        println!("{} Config parsed", "✓".green());

        let object_store = build_object_store(&store_cfg).await?;
        println!("{} Object store initialized", "✓".green());

        let object_store_manager =
            Arc::new(ObjectStoreManager::new("default", object_store.clone()));

        // Create mock dependencies
        let schema_metadata_manager = mock_schema_metadata_manager();
        let file_ref_manager = Arc::new(FileReferenceManager::new(None));
        let partition_expr_fetcher = noop_partition_expr_fetcher();

        // Create MitoEngine with NoopLogStore (skip WAL)
        let log_store = Arc::new(NoopLogStore);
        let engine = MitoEngine::new(
            &store_cfg.data_home,
            mito_config,
            log_store,
            object_store_manager,
            schema_metadata_manager,
            file_ref_manager,
            partition_expr_fetcher,
            Plugins::default(),
        )
        .await
        .map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("Failed to create MitoEngine: {e:?}"),
            }
            .build()
        })?;
        println!("{} MitoEngine created (NoopLogStore)", "✓".green());

        // Open region
        let open_request = RegionOpenRequest {
            engine: "mito".to_string(),
            table_dir: self.table_dir.clone(),
            path_type,
            options: HashMap::default(),
            skip_wal_replay: true,
            checkpoint: None,
        };

        engine
            .handle_request(region_id, RegionRequest::Open(open_request))
            .await
            .map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("Failed to open region: {e:?}"),
                }
                .build()
            })?;
        println!("{} Region opened", "✓".green());

        // Load scan config
        let scan_config = if let Some(path) = &self.scan_config {
            let content = std::fs::read_to_string(path).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("Failed to read scan config {}: {e}", path.display()),
                }
                .build()
            })?;
            serde_json::from_str::<ScanConfig>(&content).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("Failed to parse scan config: {e}"),
                }
                .build()
            })?
        } else {
            ScanConfig::default()
        };

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
            "{} Scanner: {}, Parallelism: {}, Iterations: {}",
            "ℹ".blue(),
            self.scanner,
            self.parallelism,
            self.iterations,
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
                        error::IllegalConfigSnafu {
                            msg: format!("Failed to start profiler: {e}"),
                        }
                        .build()
                    })?,
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
                projection: scan_config.projection.clone(),
                limit: scan_config.limit,
                series_row_selector,
                distribution,
                ..Default::default()
            };

            let start = Instant::now();

            // Get scanner
            let mut scanner = engine.handle_query(region_id, request).await.map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("Failed to get scanner: {e:?}"),
                }
                .build()
            })?;

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
                    .map_err(|e| {
                        error::IllegalConfigSnafu {
                            msg: format!("Failed to prepare scanner: {e:?}"),
                        }
                        .build()
                    })?;
            }

            // Scan all partitions
            let num_partitions = scanner.properties().partitions.len();
            let ctx = QueryScanContext::default();
            let metrics_set = ExecutionPlanMetricsSet::new();

            let mut scan_futures = FuturesUnordered::new();

            for partition_idx in 0..num_partitions {
                let mut stream = scanner
                    .scan_partition(&ctx, &metrics_set, partition_idx)
                    .map_err(|e| {
                        error::IllegalConfigSnafu {
                            msg: format!("scan_partition failed: {e:?}"),
                        }
                        .build()
                    })?;

                scan_futures.push(async move {
                    let mut rows = 0u64;
                    while let Some(batch_result) = stream.next().await {
                        match batch_result {
                            Ok(batch) => {
                                rows += batch.num_rows() as u64;
                            }
                            Err(e) => {
                                return Err(format!("Error reading batch: {e:?}"));
                            }
                        }
                    }
                    Ok::<u64, String>(rows)
                });
            }

            let mut total_rows = 0u64;
            while let Some(result) = scan_futures.next().await {
                let rows = result.map_err(|e| error::IllegalConfigSnafu { msg: e }.build())?;
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
