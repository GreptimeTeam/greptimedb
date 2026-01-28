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

//! Import V2 CLI command.

use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
use common_telemetry::info;
use snafu::ResultExt;

use super::ddl_generator::DdlGenerator;
use super::error::{ExportSnafu, ManifestVersionMismatchSnafu, Result, SchemaNotInSnapshotSnafu};
use super::executor::DdlExecutor;
use crate::Tool;
use crate::common::ObjectStoreConfig;
use crate::data::export_v2::manifest::MANIFEST_VERSION;
use crate::data::snapshot_storage::{OpenDalStorage, SnapshotStorage, validate_uri};
use crate::database::{DatabaseClient, parse_proxy_opts};

/// Import from a snapshot.
#[derive(Debug, Parser)]
pub struct ImportV2Command {
    /// Server address to connect (e.g., 127.0.0.1:4000).
    #[clap(long)]
    addr: String,

    /// Source snapshot location (e.g., s3://bucket/path, file:///tmp/backup).
    #[clap(long)]
    from: String,

    /// Target catalog name.
    #[clap(long, default_value = "greptime")]
    catalog: String,

    /// Schema list to import (default: all in snapshot).
    /// Can be specified multiple times or comma-separated.
    #[clap(long, value_delimiter = ',')]
    schemas: Vec<String>,

    /// Verify without importing (dry-run).
    #[clap(long)]
    dry_run: bool,

    /// Concurrency level (for future use).
    #[clap(long, default_value = "1")]
    parallelism: usize,

    /// Basic authentication (user:password).
    #[clap(long)]
    auth_basic: Option<String>,

    /// Request timeout.
    #[clap(long, value_parser = humantime::parse_duration)]
    timeout: Option<Duration>,

    /// Proxy server address.
    ///
    /// If set, it overrides the system proxy unless `--no-proxy` is specified.
    /// If neither `--proxy` nor `--no-proxy` is set, system proxy (env) may be used.
    #[clap(long)]
    proxy: Option<String>,

    /// Disable all proxy usage (ignores `--proxy` and system proxy).
    ///
    /// When set and `--proxy` is not provided, this explicitly disables system proxy.
    #[clap(long)]
    no_proxy: bool,

    /// Object store configuration for remote storage backends.
    #[clap(flatten)]
    storage: ObjectStoreConfig,
}

impl ImportV2Command {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        // Validate URI format
        validate_uri(&self.from)
            .context(ExportSnafu)
            .map_err(BoxedError::new)?;

        // Parse schemas (empty vec means all schemas)
        let schemas = if self.schemas.is_empty() {
            None
        } else {
            Some(self.schemas.clone())
        };

        // Build storage
        let storage = OpenDalStorage::from_uri(&self.from, &self.storage)
            .context(ExportSnafu)
            .map_err(BoxedError::new)?;

        // Build database client
        let proxy = parse_proxy_opts(self.proxy.clone(), self.no_proxy)?;
        let database_client = DatabaseClient::new(
            self.addr.clone(),
            self.catalog.clone(),
            self.auth_basic.clone(),
            self.timeout.unwrap_or(Duration::from_secs(60)),
            proxy,
            self.no_proxy,
        );

        Ok(Box::new(Import {
            schemas,
            dry_run: self.dry_run,
            _parallelism: self.parallelism,
            storage: Box::new(storage),
            database_client,
        }))
    }
}

/// Import tool implementation.
pub struct Import {
    schemas: Option<Vec<String>>,
    dry_run: bool,
    _parallelism: usize,
    storage: Box<dyn SnapshotStorage>,
    database_client: DatabaseClient,
}

#[async_trait]
impl Tool for Import {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.run().await.map_err(BoxedError::new)
    }
}

impl Import {
    async fn run(&self) -> Result<()> {
        // 1. Read manifest
        let manifest = self.storage.read_manifest().await.context(ExportSnafu)?;

        info!(
            "Loading snapshot: {} (version: {}, schema_only: {})",
            manifest.snapshot_id, manifest.version, manifest.schema_only
        );

        // Check version compatibility
        if manifest.version != MANIFEST_VERSION {
            return ManifestVersionMismatchSnafu {
                expected: MANIFEST_VERSION,
                found: manifest.version,
            }
            .fail();
        }

        // 2. Read schema snapshot
        let schema_snapshot = self.storage.read_schema().await.context(ExportSnafu)?;

        info!(
            "Snapshot contains {} schemas, {} tables, {} views",
            schema_snapshot.schemas.len(),
            schema_snapshot.tables.len(),
            schema_snapshot.views.len()
        );

        // 3. Determine schemas to import
        let schemas_to_import = match &self.schemas {
            Some(filter) => {
                // Validate that all specified schemas exist in snapshot
                for schema in filter {
                    if !manifest
                        .schemas
                        .iter()
                        .any(|s| s.eq_ignore_ascii_case(schema))
                    {
                        return SchemaNotInSnapshotSnafu {
                            schema: schema.clone(),
                        }
                        .fail();
                    }
                }
                filter.clone()
            }
            None => manifest.schemas.clone(),
        };

        info!("Importing schemas: {:?}", schemas_to_import);

        // 4. Generate DDL statements
        let generator = DdlGenerator::new(&schema_snapshot);
        let ddl_statements = generator.generate(&schemas_to_import)?;

        info!("Generated {} DDL statements", ddl_statements.len());

        // 5. Dry-run mode: print DDL and exit
        if self.dry_run {
            info!("Dry-run mode - DDL statements to execute:");
            println!();
            for (i, stmt) in ddl_statements.iter().enumerate() {
                println!("-- Statement {}", i + 1);
                println!("{};", stmt);
                println!();
            }
            return Ok(());
        }

        // 6. Execute DDL
        let executor = DdlExecutor::new(&self.database_client);
        let result = executor.execute(&ddl_statements).await?;

        info!(
            "Import completed: {} succeeded, {} failed out of {} total",
            result.succeeded, result.failed, result.total
        );

        if result.has_failures() {
            info!(
                "Warning: {} statements failed. Check logs for details.",
                result.failed
            );
        }

        // 7. Data import would happen here for non-schema-only snapshots (M2/M3)
        if !manifest.schema_only && !manifest.chunks.is_empty() {
            info!(
                "Data import not yet implemented (M3). {} chunks pending.",
                manifest.chunks.len()
            );
        }

        Ok(())
    }
}
