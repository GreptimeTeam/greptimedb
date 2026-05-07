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

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

use async_trait::async_trait;
use common_telemetry::{info, warn};

use crate::data::export_v2::manifest::{ChunkMeta, ChunkStatus};
use crate::data::import_v2::error::{
    ImportStateDdlIncompleteSnafu, ImportStateMismatchSnafu, Result,
};
use crate::data::import_v2::state::{
    ImportState, ImportStateLockGuard, ImportTaskKey, ImportTaskStatus, canonical_schema_selection,
    delete_import_state, load_import_state, save_import_state, try_acquire_import_state_lock,
};
use crate::data::path::data_dir_for_schema_chunk;

#[async_trait]
pub(crate) trait ImportTaskExecutor {
    async fn import_task(&self, task: &ImportTaskKey) -> Result<()>;
}

pub(crate) struct ImportResumeConfig {
    pub(crate) snapshot_id: String,
    pub(crate) target_addr: String,
    pub(crate) catalog: String,
    pub(crate) schemas: Vec<String>,
    pub(crate) state_path: PathBuf,
    pub(crate) tasks: Vec<ImportTaskKey>,
}

pub(crate) struct ImportResumeSession {
    config: ImportResumeConfig,
    state: ImportState,
    lock: ImportStateLockGuard,
}

impl ImportResumeSession {
    pub(crate) fn should_skip_ddl(&self) -> bool {
        self.state.ddl_completed
    }

    /// Marks DDL as completed and persists the state. Must be called after a
    /// successful DDL run on a fresh session, so that crashes after this point
    /// resume into the data-import phase instead of replaying DDL.
    pub(crate) async fn mark_ddl_completed(&mut self) -> Result<()> {
        self.state.mark_ddl_completed();
        save_import_state(&self.config.state_path, &self.state).await
    }
}

pub(crate) fn chunk_has_schema_files(chunk: &ChunkMeta, schema: &str) -> bool {
    let prefix = data_dir_for_schema_chunk(schema, chunk.id);
    chunk.files.iter().any(|path| {
        let normalized = path.trim_start_matches('/');
        normalized.starts_with(&prefix)
    })
}

pub(crate) fn build_import_tasks(chunks: &[ChunkMeta], schemas: &[String]) -> Vec<ImportTaskKey> {
    let mut tasks = Vec::new();
    for chunk in chunks {
        if chunk.status == ChunkStatus::Skipped {
            continue;
        }
        // TODO: build a per-chunk schema index if chunk file manifests become large.
        for schema in schemas {
            if chunk_has_schema_files(chunk, schema) {
                tasks.push(ImportTaskKey::new(chunk.id, schema.clone()));
            }
        }
    }
    tasks
}

pub(crate) async fn prepare_import_resume(
    config: ImportResumeConfig,
) -> Result<ImportResumeSession> {
    // Validate the request before touching the state file or acquiring the
    // lock. Duplicate task keys would corrupt the resume bookkeeping because
    // status lookups use linear `find()` and only ever see the first match.
    validate_config_tasks(&config)?;

    let lock = try_acquire_import_state_lock(&config.state_path)?;
    let state = match load_import_state(&config.state_path).await? {
        Some(loaded) => {
            validate_state_matches(&loaded, &config)?;
            loaded
        }
        None => {
            // Persist a fresh state immediately so that any crash after this
            // point is recoverable as a resume. `ddl_completed=false` on a
            // loaded state therefore means a previous run reached this point
            // but did not confirm DDL completion - DDL must be (re-)run before
            // data import is allowed.
            let fresh = ImportState::new(
                &config.snapshot_id,
                &config.target_addr,
                &config.catalog,
                &config.schemas,
                config.tasks.clone(),
            );
            save_import_state(&config.state_path, &fresh).await?;
            fresh
        }
    };

    Ok(ImportResumeSession {
        config,
        state,
        lock,
    })
}

pub(crate) async fn import_with_resume_session<E>(
    session: ImportResumeSession,
    executor: &E,
) -> Result<()>
where
    E: ImportTaskExecutor + Sync,
{
    let ImportResumeSession {
        config,
        mut state,
        lock,
    } = session;

    // The state machine requires DDL to be explicitly marked completed before
    // data import; otherwise a caller could import data and leave a state that
    // replays DDL on the next resume. Surface the misuse instead of silently
    // importing.
    if !state.ddl_completed {
        return ImportStateDdlIncompleteSnafu {
            path: config.state_path.display().to_string(),
        }
        .fail();
    }

    let completed = state
        .tasks
        .iter()
        .filter(|task| task.status == ImportTaskStatus::Completed)
        .count();
    info!(
        "Import resume state: {} completed, {} pending, path: {}",
        completed,
        state.tasks.len().saturating_sub(completed),
        config.state_path.display()
    );

    let import_start = Instant::now();
    for (idx, task) in config.tasks.iter().enumerate() {
        if state.task_status(task.chunk_id, &task.schema) == Some(ImportTaskStatus::Completed) {
            info!(
                "[{}/{}] Chunk {} schema {}: already completed, skipped",
                idx + 1,
                config.tasks.len(),
                task.chunk_id,
                task.schema
            );
            continue;
        }

        info!(
            "[{}/{}] Chunk {} schema {}: importing...",
            idx + 1,
            config.tasks.len(),
            task.chunk_id,
            task.schema
        );
        state.set_task_status(
            task.chunk_id,
            &task.schema,
            ImportTaskStatus::InProgress,
            None,
        )?;
        save_import_state(&config.state_path, &state).await?;

        let task_start = Instant::now();
        let result = executor.import_task(task).await;

        match result {
            Ok(()) => {
                // The task itself succeeded. If we cannot persist the
                // Completed marker, the next resume will replay it (potentially
                // duplicating data depending on engine semantics), but we must
                // not pretend the import as a whole failed - return the persist
                // error so the operator notices, after logging the success.
                update_status_and_save(
                    &config,
                    &mut state,
                    task,
                    ImportTaskStatus::Completed,
                    None,
                )
                .await?;
                info!(
                    "[{}/{}] Chunk {} schema {}: done in {:?}",
                    idx + 1,
                    config.tasks.len(),
                    task.chunk_id,
                    task.schema,
                    task_start.elapsed()
                );
            }
            Err(task_error) => {
                // Persist Failed best-effort, but always surface the original
                // task error to the caller. State persistence problems are
                // logged so they are not silently lost.
                if let Err(persist_error) = update_status_and_save(
                    &config,
                    &mut state,
                    task,
                    ImportTaskStatus::Failed,
                    Some(task_error.to_string()),
                )
                .await
                {
                    warn!(
                        "Failed to persist Failed status for chunk {} schema {} after task error ({}); state file may be out of date: {}",
                        task.chunk_id, task.schema, task_error, persist_error
                    );
                }
                return Err(task_error);
            }
        }
    }

    delete_import_state(&config.state_path).await?;
    info!("Data import finished in {:?}", import_start.elapsed());
    drop(lock);
    Ok(())
}

async fn update_status_and_save(
    config: &ImportResumeConfig,
    state: &mut ImportState,
    task: &ImportTaskKey,
    status: ImportTaskStatus,
    error_message: Option<String>,
) -> Result<()> {
    // set_task_status only fails if the task isn't in the state; that would
    // indicate a logic bug since `task` came from the same config. Surface it
    // instead of swallowing.
    state.set_task_status(task.chunk_id, &task.schema, status, error_message)?;
    save_import_state(&config.state_path, state).await
}

fn validate_state_matches(state: &ImportState, config: &ImportResumeConfig) -> Result<()> {
    if state.snapshot_id != config.snapshot_id {
        return state_mismatch(
            config,
            format!(
                "snapshot_id differs (state: {}, requested: {})",
                state.snapshot_id, config.snapshot_id
            ),
        );
    }
    // Target addresses are compared literally; hostname normalization is left to the caller.
    if state.target_addr != config.target_addr {
        return state_mismatch(
            config,
            format!(
                "target_addr differs (state: {}, requested: {})",
                state.target_addr, config.target_addr
            ),
        );
    }
    if state.catalog != config.catalog {
        return state_mismatch(
            config,
            format!(
                "catalog differs (state: {}, requested: {})",
                state.catalog, config.catalog
            ),
        );
    }

    let requested_schemas = canonical_schema_selection(&config.schemas);
    if state.schemas != requested_schemas {
        return state_mismatch(
            config,
            format!(
                "schemas differ (state: {:?}, requested: {:?})",
                state.schemas, requested_schemas
            ),
        );
    }

    if task_set_from_state(state, &config.state_path)? != task_set_from_config(config)? {
        return state_mismatch(config, "task set differs".to_string());
    }

    Ok(())
}

fn state_mismatch(config: &ImportResumeConfig, reason: String) -> Result<()> {
    ImportStateMismatchSnafu {
        path: config.state_path.display().to_string(),
        reason,
    }
    .fail()
}

fn task_set_from_state(state: &ImportState, state_path: &Path) -> Result<BTreeSet<(u32, String)>> {
    let mut tasks = BTreeSet::new();
    for task in &state.tasks {
        if !tasks.insert((task.chunk_id, task.schema.clone())) {
            return ImportStateMismatchSnafu {
                path: state_path.display().to_string(),
                reason: format!(
                    "duplicate task key in state (chunk_id: {}, schema: {})",
                    task.chunk_id, task.schema
                ),
            }
            .fail();
        }
    }
    Ok(tasks)
}

fn task_set_from_config(config: &ImportResumeConfig) -> Result<BTreeSet<(u32, String)>> {
    let mut tasks = BTreeSet::new();
    for task in &config.tasks {
        if !tasks.insert((task.chunk_id, task.schema.clone())) {
            return ImportStateMismatchSnafu {
                path: config.state_path.display().to_string(),
                reason: format!(
                    "duplicate task key in request (chunk_id: {}, schema: {})",
                    task.chunk_id, task.schema
                ),
            }
            .fail();
        }
    }
    Ok(tasks)
}

fn validate_config_tasks(config: &ImportResumeConfig) -> Result<()> {
    task_set_from_config(config).map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::data::export_v2::manifest::{ChunkMeta, TimeRange};
    use crate::data::import_v2::error::TestTaskFailedSnafu;

    #[derive(Debug, Clone, Copy)]
    enum FailureMode {
        Fatal,
        RetryableThenSuccess { failures: usize },
    }

    struct RecordingExecutor {
        imported: Arc<Mutex<Vec<ImportTaskKey>>>,
        fail_task: Option<ImportTaskKey>,
        failure_mode: Option<FailureMode>,
        attempts: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ImportTaskExecutor for RecordingExecutor {
        async fn import_task(&self, task: &ImportTaskKey) -> Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail_task.as_ref() == Some(task) {
                match self.failure_mode {
                    Some(FailureMode::Fatal) => {
                        return TestTaskFailedSnafu {
                            message: "fatal failure".to_string(),
                            retryable: false,
                        }
                        .fail();
                    }
                    Some(FailureMode::RetryableThenSuccess { failures }) if attempt < failures => {
                        return TestTaskFailedSnafu {
                            message: "retryable failure".to_string(),
                            retryable: true,
                        }
                        .fail();
                    }
                    _ => {}
                }
            }
            self.imported.lock().unwrap().push(task.clone());
            Ok(())
        }
    }

    fn recording_executor(imported: Arc<Mutex<Vec<ImportTaskKey>>>) -> RecordingExecutor {
        RecordingExecutor {
            imported,
            fail_task: None,
            failure_mode: None,
            attempts: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn config(path: PathBuf, tasks: Vec<ImportTaskKey>) -> ImportResumeConfig {
        ImportResumeConfig {
            snapshot_id: "snapshot-1".to_string(),
            target_addr: "127.0.0.1:4000".to_string(),
            catalog: "greptime".to_string(),
            schemas: vec!["public".to_string(), "analytics".to_string()],
            state_path: path,
            tasks,
        }
    }

    async fn run_import_with_resume<E>(config: ImportResumeConfig, executor: &E) -> Result<()>
    where
        E: ImportTaskExecutor + Sync,
    {
        // Mirror the production caller: mark DDL completed for fresh sessions
        // so the data-import guard is satisfied. Tests that want to exercise
        // the unsafe path drive prepare/import directly.
        let mut session = prepare_import_resume(config).await?;
        if !session.should_skip_ddl() {
            session.mark_ddl_completed().await?;
        }
        import_with_resume_session(session, executor).await
    }

    #[test]
    fn test_build_import_tasks_skips_skipped_chunks_and_missing_schema_files() {
        let mut completed = ChunkMeta::new(1, TimeRange::unbounded());
        completed.status = ChunkStatus::Completed;
        completed.files = vec!["data/public/1/file.parquet".to_string()];
        let mut skipped = ChunkMeta::new(2, TimeRange::unbounded());
        skipped.status = ChunkStatus::Skipped;
        skipped.files = vec!["data/public/2/file.parquet".to_string()];

        let tasks = build_import_tasks(
            &[completed, skipped],
            &["public".to_string(), "analytics".to_string()],
        );

        assert_eq!(tasks, vec![ImportTaskKey::new(1, "public")]);
    }

    #[tokio::test]
    async fn test_import_with_resume_skips_completed_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let tasks = vec![
            ImportTaskKey::new(1, "public"),
            ImportTaskKey::new(2, "analytics"),
        ];
        let mut state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &["public".to_string(), "analytics".to_string()],
            tasks.clone(),
        );
        state.mark_ddl_completed();
        state
            .set_task_status(1, "public", ImportTaskStatus::Completed, None)
            .unwrap();
        save_import_state(&path, &state).await.unwrap();

        let imported = Arc::new(Mutex::new(Vec::new()));
        let executor = recording_executor(imported.clone());

        run_import_with_resume(config(path.clone(), tasks), &executor)
            .await
            .unwrap();

        assert_eq!(
            imported.lock().unwrap().clone(),
            vec![ImportTaskKey::new(2, "analytics")]
        );
        assert!(load_import_state(&path).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_import_with_resume_persists_failed_task() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let failed_task = ImportTaskKey::new(1, "public");
        let tasks = vec![failed_task.clone()];
        let imported = Arc::new(Mutex::new(Vec::new()));
        let executor = RecordingExecutor {
            imported,
            fail_task: Some(failed_task.clone()),
            failure_mode: Some(FailureMode::Fatal),
            attempts: Arc::new(AtomicUsize::new(0)),
        };

        let error = run_import_with_resume(config(path.clone(), tasks), &executor)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::data::import_v2::error::Error::TestTaskFailed {
                retryable: false,
                ..
            }
        ));

        let state = load_import_state(&path).await.unwrap().unwrap();
        assert_eq!(
            state.task_status(failed_task.chunk_id, &failed_task.schema),
            Some(ImportTaskStatus::Failed)
        );
    }

    #[tokio::test]
    async fn test_import_with_resume_rejects_mismatched_state_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let tasks = vec![ImportTaskKey::new(1, "public")];
        let state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4001",
            "greptime",
            &["public".to_string(), "analytics".to_string()],
            tasks.clone(),
        );
        save_import_state(&path, &state).await.unwrap();

        let imported = Arc::new(Mutex::new(Vec::new()));
        let executor = recording_executor(imported);

        let error = run_import_with_resume(config(path, tasks), &executor)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            crate::data::import_v2::error::Error::ImportStateMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn test_prepare_import_resume_reports_existing_state_before_ddl() {
        let dir = tempfile::tempdir().unwrap();
        let tasks = vec![ImportTaskKey::new(1, "public")];

        let fresh_session =
            prepare_import_resume(config(dir.path().join("fresh_state.json"), tasks.clone()))
                .await
                .unwrap();
        assert!(!fresh_session.should_skip_ddl());
        drop(fresh_session);

        let existing_path = dir.path().join("existing_state.json");
        let mut state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &["public".to_string(), "analytics".to_string()],
            tasks.clone(),
        );
        state.mark_ddl_completed();
        save_import_state(&existing_path, &state).await.unwrap();

        let resume_session = prepare_import_resume(config(existing_path, tasks))
            .await
            .unwrap();
        assert!(resume_session.should_skip_ddl());
    }

    #[tokio::test]
    async fn test_import_with_resume_rejects_duplicate_state_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let tasks = vec![ImportTaskKey::new(1, "public")];
        let mut state = ImportState::new(
            "snapshot-1",
            "127.0.0.1:4000",
            "greptime",
            &["public".to_string(), "analytics".to_string()],
            tasks.clone(),
        );
        state.tasks.push(state.tasks[0].clone());
        save_import_state(&path, &state).await.unwrap();

        let imported = Arc::new(Mutex::new(Vec::new()));
        let executor = recording_executor(imported);

        let error = run_import_with_resume(config(path, tasks), &executor)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            crate::data::import_v2::error::Error::ImportStateMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn test_import_with_resume_rejects_data_import_when_ddl_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let tasks = vec![ImportTaskKey::new(1, "public")];

        // prepare creates fresh state with ddl_completed=false; calling
        // import_with_resume_session directly (without mark_ddl_completed)
        // must be rejected.
        let session = prepare_import_resume(config(path, tasks)).await.unwrap();
        let imported = Arc::new(Mutex::new(Vec::new()));
        let executor = recording_executor(imported.clone());

        let error = import_with_resume_session(session, &executor)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            crate::data::import_v2::error::Error::ImportStateDdlIncomplete { .. }
        ));
        assert!(imported.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_prepare_import_resume_rejects_duplicate_request_tasks_on_fresh_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let task = ImportTaskKey::new(1, "public");
        // No state file yet - duplicate detection must run before the fresh
        // state is persisted, otherwise corrupted bookkeeping would be
        // written to disk and observed only on a later resume.
        let error =
            match prepare_import_resume(config(path.clone(), vec![task.clone(), task])).await {
                Ok(_) => panic!("duplicate request tasks should be rejected"),
                Err(error) => error,
            };

        assert!(matches!(
            error,
            crate::data::import_v2::error::Error::ImportStateMismatch { .. }
        ));
        assert!(load_import_state(&path).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_import_with_resume_does_not_retry_retryable_task_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("import_state.json");
        let failed_task = ImportTaskKey::new(1, "public");
        let tasks = vec![failed_task.clone()];
        let imported = Arc::new(Mutex::new(Vec::new()));
        let attempts = Arc::new(AtomicUsize::new(0));
        let executor = RecordingExecutor {
            imported: imported.clone(),
            fail_task: Some(failed_task.clone()),
            // If task import were retried, the second attempt would succeed.
            // COPY DATABASE FROM failures are ambiguous, so retryable errors
            // must still stop immediately to avoid duplicate rows.
            failure_mode: Some(FailureMode::RetryableThenSuccess { failures: 1 }),
            attempts: attempts.clone(),
        };

        let error = run_import_with_resume(config(path.clone(), tasks), &executor)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            crate::data::import_v2::error::Error::TestTaskFailed {
                retryable: true,
                ..
            }
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(imported.lock().unwrap().is_empty());

        let state = load_import_state(&path).await.unwrap().unwrap();
        assert_eq!(
            state.task_status(failed_task.chunk_id, &failed_task.schema),
            Some(ImportTaskStatus::Failed)
        );
    }
}
