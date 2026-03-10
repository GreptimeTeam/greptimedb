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

//! Verify snapshot integrity command.

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
use common_telemetry::{info, warn};
use snafu::ResultExt;

use super::error::{Result, StorageOperationSnafu, VerificationFailedSnafu};
use super::manifest::{ChunkStatus, MANIFEST_VERSION, Manifest};
use super::schema::{DDL_DIR, SCHEMA_DIR, SCHEMAS_FILE, TABLES_FILE, VIEWS_FILE};
use super::storage::{OpenDalStorage, SnapshotStorage, validate_snapshot_target_uri};
use crate::Tool;
use crate::common::ObjectStoreConfig;

/// Verify snapshot integrity.
#[derive(Debug, Parser)]
pub struct ExportVerifyCommand {
    /// Snapshot location to verify (e.g., s3://bucket/snapshots/prod-20250101).
    #[clap(long)]
    snapshot: String,

    /// Object store configuration for remote storage backends.
    #[clap(flatten)]
    storage: ObjectStoreConfig,
}

impl ExportVerifyCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        validate_snapshot_target_uri(&self.snapshot).map_err(BoxedError::new)?;
        let storage =
            OpenDalStorage::from_uri(&self.snapshot, &self.storage).map_err(BoxedError::new)?;

        Ok(Box::new(ExportVerify {
            storage: Box::new(storage),
        }))
    }
}

pub struct ExportVerify {
    storage: Box<dyn SnapshotStorage>,
}

impl ExportVerify {
    /// Creates a new verify tool (used by tests).
    pub fn new(storage: Box<dyn SnapshotStorage>) -> Self {
        Self { storage }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProblemLevel {
    Error,
    Warn,
}

#[derive(Debug)]
pub struct VerifyProblem {
    pub level: ProblemLevel,
    pub message: String,
}

#[async_trait]
impl Tool for ExportVerify {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.run().await.map_err(BoxedError::new)
    }
}

impl ExportVerify {
    async fn run(&self) -> Result<()> {
        let manifest = self.storage.read_manifest().await?;
        info!("Verifying snapshot: {}", manifest.snapshot_id);

        let problems = self.verify(&manifest).await?;

        let error_count = problems
            .iter()
            .filter(|p| p.level == ProblemLevel::Error)
            .count();
        let warn_count = problems
            .iter()
            .filter(|p| p.level == ProblemLevel::Warn)
            .count();

        if problems.is_empty() {
            info!("\nSnapshot is valid.");
            Ok(())
        } else {
            info!("\nProblems found:");
            for problem in &problems {
                let tag = match problem.level {
                    ProblemLevel::Error => "[ERROR]",
                    ProblemLevel::Warn => "[WARN] ",
                };
                warn!("  {} {}", tag, problem.message);
            }

            if error_count > 0 && warn_count > 0 {
                warn!(
                    "\nSnapshot has {} error(s) and {} warning(s).",
                    error_count, warn_count
                );
            } else if error_count > 0 {
                warn!("\nSnapshot has {} error(s).", error_count);
            } else {
                warn!("\nSnapshot has {} warning(s).", warn_count);
            }

            VerificationFailedSnafu {
                error_count,
                warn_count,
            }
            .fail()
        }
    }

    async fn verify(&self, manifest: &Manifest) -> Result<Vec<VerifyProblem>> {
        let mut problems = Vec::new();

        // 1. Check manifest version
        if manifest.version != MANIFEST_VERSION {
            problems.push(VerifyProblem {
                level: ProblemLevel::Error,
                message: format!(
                    "Manifest version mismatch: expected {}, found {}",
                    MANIFEST_VERSION, manifest.version
                ),
            });
        } else {
            info!("  Manifest:     OK (version {})", manifest.version);
        }

        // 2. Check schema files
        self.verify_schema_files(&mut problems).await?;

        // 3. Check DDL files (before schema_only gate)
        self.verify_ddl_files(manifest, &mut problems).await?;

        // 4. If schema_only, skip chunk/data checks
        if manifest.schema_only {
            info!("  Chunks:       N/A (schema-only)");
            return Ok(problems);
        }

        // 5. Check chunk statuses
        let mut completed_count = 0usize;
        let mut skipped_count = 0usize;
        for chunk in &manifest.chunks {
            match chunk.status {
                ChunkStatus::Completed => {
                    completed_count += 1;
                    if chunk.files.is_empty() {
                        problems.push(VerifyProblem {
                            level: ProblemLevel::Error,
                            message: format!(
                                "Chunk {}: status is 'completed' but files list is empty",
                                chunk.id
                            ),
                        });
                    }
                }
                ChunkStatus::Skipped => {
                    skipped_count += 1;
                    if !chunk.files.is_empty() {
                        problems.push(VerifyProblem {
                            level: ProblemLevel::Error,
                            message: format!(
                                "Chunk {}: status is 'skipped' but files list is not empty ({})",
                                chunk.id,
                                chunk.files.len()
                            ),
                        });
                    }
                }
                ChunkStatus::Pending => {
                    problems.push(VerifyProblem {
                        level: ProblemLevel::Error,
                        message: format!(
                            "Chunk {}: status is 'pending' (export not started)",
                            chunk.id
                        ),
                    });
                }
                ChunkStatus::InProgress => {
                    problems.push(VerifyProblem {
                        level: ProblemLevel::Error,
                        message: format!(
                            "Chunk {}: status is 'in_progress' (incomplete export)",
                            chunk.id
                        ),
                    });
                }
                ChunkStatus::Failed => {
                    let error_msg = chunk.error.as_deref().unwrap_or("unknown");
                    problems.push(VerifyProblem {
                        level: ProblemLevel::Error,
                        message: format!(
                            "Chunk {}: status is 'failed' (error: \"{}\")",
                            chunk.id, error_msg
                        ),
                    });
                }
            }
        }
        info!(
            "  Chunks:       {} total ({} completed, {} skipped)",
            manifest.chunks.len(),
            completed_count,
            skipped_count,
        );

        // 6. Check data file existence for completed chunks
        let mut verified_count = 0usize;
        let mut total_files = 0usize;
        let store = self.storage.object_store();

        for chunk in &manifest.chunks {
            if chunk.status != ChunkStatus::Completed {
                continue;
            }
            for file_path in &chunk.files {
                total_files += 1;
                let full_path = storage_path(self.storage.as_ref(), file_path);

                match store.stat(&full_path).await {
                    Ok(_) => verified_count += 1,
                    Err(e) if e.kind() == object_store::ErrorKind::NotFound => {
                        problems.push(VerifyProblem {
                            level: ProblemLevel::Error,
                            message: format!("Chunk {}: missing file '{}'", chunk.id, file_path),
                        });
                    }
                    Err(e) => {
                        return Err(e).context(StorageOperationSnafu {
                            operation: format!("stat file '{}'", file_path),
                        });
                    }
                }
            }
        }

        if total_files > 0 {
            if verified_count == total_files {
                info!("  Data files:   {} files verified", verified_count);
            } else {
                info!(
                    "  Data files:   {}/{} files verified",
                    verified_count, total_files
                );
            }
        }

        Ok(problems)
    }

    async fn verify_schema_files(&self, problems: &mut Vec<VerifyProblem>) -> Result<()> {
        let schema_files = [
            (SCHEMAS_FILE, "schemas.json"),
            (TABLES_FILE, "tables.json"),
            (VIEWS_FILE, "views.json"),
        ];

        let mut all_ok = true;
        let store = self.storage.object_store();

        for (file, display_name) in &schema_files {
            let path = format!("{}/{}", SCHEMA_DIR, file);
            let full_path = storage_path(self.storage.as_ref(), &path);

            match store.stat(&full_path).await {
                Ok(_) => {}
                Err(e) if e.kind() == object_store::ErrorKind::NotFound => {
                    all_ok = false;
                    problems.push(VerifyProblem {
                        level: ProblemLevel::Warn,
                        message: format!("Schema file missing: {}", display_name),
                    });
                }
                Err(e) => {
                    return Err(e).context(StorageOperationSnafu {
                        operation: format!("stat schema file '{}'", display_name),
                    });
                }
            }
        }

        if all_ok {
            info!("  Schema files: OK (schemas.json, tables.json, views.json)");
        }

        Ok(())
    }

    async fn verify_ddl_files(
        &self,
        manifest: &Manifest,
        problems: &mut Vec<VerifyProblem>,
    ) -> Result<()> {
        let store = self.storage.object_store();
        let ddl_dir = format!("{}/{}", SCHEMA_DIR, DDL_DIR);
        let ddl_dir_full = storage_path(self.storage.as_ref(), &format!("{ddl_dir}/"));

        // Check if any DDL files exist by listing the directory.
        let entries = match store.list(&ddl_dir_full).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == object_store::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(e).context(StorageOperationSnafu {
                    operation: "list DDL directory",
                });
            }
        };

        let has_sql_files = entries.iter().any(|e| e.path().ends_with(".sql"));
        if !has_sql_files {
            return Ok(());
        }

        // DDL directory exists with .sql files — verify each schema has its DDL file.
        info!("  DDL files:    checking...");
        for schema in &manifest.schemas {
            let ddl_path = format!("{}/{}.sql", ddl_dir, schema);
            let ddl_full = storage_path(self.storage.as_ref(), &ddl_path);

            match store.stat(&ddl_full).await {
                Ok(_) => {}
                Err(e) if e.kind() == object_store::ErrorKind::NotFound => {
                    problems.push(VerifyProblem {
                        level: ProblemLevel::Error,
                        message: format!(
                            "DDL file missing for schema '{}': expected '{}'",
                            schema, ddl_path
                        ),
                    });
                }
                Err(e) => {
                    return Err(e).context(StorageOperationSnafu {
                        operation: format!("stat DDL file for schema '{}'", schema),
                    });
                }
            }
        }

        Ok(())
    }
}

fn storage_path(storage: &dyn SnapshotStorage, path: &str) -> String {
    if storage.root_path().is_empty() {
        path.to_string()
    } else {
        format!("{}/{}", storage.root_path().trim_end_matches('/'), path)
    }
}
