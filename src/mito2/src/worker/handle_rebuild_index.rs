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

//! Handles index build requests.

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::region::build_index_request;
use common_telemetry::{debug, warn};
use snafu::{IntoError, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::RegionBuildIndexRequest;
use store_api::storage::{FileId, RegionId};
use tokio::sync::mpsc;

use crate::cache::CacheStrategy;
use crate::error::{
    BuildIndexAsyncSnafu, ExternalSnafu, IndexBuildIncompleteSnafu, InvalidRequestSnafu,
    RegionStateSnafu, Result, WorkerStoppedSnafu,
};
use crate::manifest::action::RegionEdit;
use crate::metrics::INDEX_PUBLICATION_STALE_TOTAL;
use crate::region::options::IndexOptions;
use crate::region::{IndexBuildSource, MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{BuildIndexRequest, IndexBuildFinished, IndexBuildStopped, OptionOutputTx};
use crate::sst::file::{self, FileHandle, FileMeta, RegionFileId, RegionIndexId};
use crate::sst::index::build::IndexBuildInputs;
use crate::sst::index::handler::{CurrencyContext, IndexKindProviderRef, PlanContext};
use crate::sst::index::registry::{IndexRegistry, IndexRegistryRef};
use crate::sst::index::{
    IndexBuildOutcome, IndexBuildTask, IndexBuildType, IndexerBuilderImpl, ResultMpscSender,
    cleanup_stale_index_caches,
};
use crate::worker::RegionWorkerLoop;

/// Parts of a region version that index build tasks need.
///
/// Holding this instead of the whole version avoids pinning memtables and the
/// SST version while the worker awaits.
pub(crate) struct IndexBuildSnapshot {
    metadata: RegionMetadataRef,
    index_options: IndexOptions,
}

impl<S> RegionWorkerLoop<S> {
    pub(crate) fn new_index_build_task(
        &self,
        region: &MitoRegionRef,
        snapshot: &IndexBuildSnapshot,
        registry: IndexRegistryRef,
        source: (FileHandle, FileMeta),
        build_type: IndexBuildType,
        result_senders: Vec<ResultMpscSender>,
    ) -> IndexBuildTask {
        let (file, file_meta) = source;
        let access_layer = region.access_layer.clone();

        let puffin_manager = if let Some(write_cache) = self.cache_manager.write_cache() {
            write_cache.build_puffin_manager()
        } else {
            access_layer.build_puffin_manager()
        };

        let intermediate_manager = if let Some(write_cache) = self.cache_manager.write_cache() {
            write_cache.intermediate_manager().clone()
        } else {
            access_layer.intermediate_manager().clone()
        };

        let plan = Arc::new(
            registry
                .plan(&PlanContext {
                    metadata: &snapshot.metadata,
                    index_options: &snapshot.index_options,
                    build_type: build_type.clone(),
                    existing_indexes: &file_meta.indexes,
                })
                .unwrap_or_default(),
        );
        let index_build_inputs = Some(IndexBuildInputs {
            output_rows: Some(file_meta.num_rows),
            plan,
            inputs: vec![FileHandle::new(file_meta.clone(), file.file_purger())].into(),
            input_puffin_manager: access_layer.build_puffin_manager(),
            can_merge_inputs: snapshot
                .metadata
                .partition_expr
                .as_deref()
                .is_none_or(str::is_empty)
                && file_meta.region_id == region.region_id,
            memory_limit: None,
        });
        let indexer_builder_ref = Arc::new(IndexerBuilderImpl {
            index_build_inputs,
            build_type: build_type.clone(),
            metadata: snapshot.metadata.clone(),
            registry: registry.clone(),
            index_options: snapshot.index_options.clone(),
            intermediate_manager,
            puffin_manager,
            write_cache_enabled: self.cache_manager.write_cache().is_some(),
        });

        IndexBuildTask {
            region_id: region.region_id,
            file: file.clone(),
            target_region_metadata: snapshot.metadata.clone(),
            source: IndexBuildSource::new(file_meta, snapshot.metadata.schema_version),
            index_options: snapshot.index_options.clone(),
            reason: build_type,
            access_layer: access_layer.clone(),
            listener: self.listener.clone(),
            manifest_ctx: region.manifest_ctx.clone(),
            write_cache: self.cache_manager.write_cache().cloned(),
            cache_manager: Some(self.cache_manager.clone()),
            file_purger: file.file_purger(),
            request_sender: self.sender.clone(),
            indexer_builder: indexer_builder_ref.clone(),
            result_senders,
        }
    }

    /// Handles manual build index requests.
    pub(crate) async fn handle_build_index_request(
        &mut self,
        region_id: RegionId,
        req: RegionBuildIndexRequest,
        sender: OptionOutputTx,
    ) {
        match req.options {
            Some(build_index_request::Options::SeriesIndex(_)) => {
                self.handle_build_series_index_request(region_id, sender);
            }
            None | Some(build_index_request::Options::SstIndex(_)) => {
                self.handle_rebuild_index(
                    BuildIndexRequest {
                        region_id,
                        build_type: IndexBuildType::Manual,
                        file_metas: Vec::new(),
                        result_senders: Vec::new(),
                    },
                    sender,
                )
                .await;
            }
        }
    }

    /// Submits manual reconciliation without blocking the region worker on index I/O.
    fn handle_build_series_index_request(&self, region_id: RegionId, mut sender: OptionOutputTx) {
        let Some(region) = self.regions.writable_region_or(region_id, &mut sender) else {
            return;
        };
        let Some(state) = &self.series_index_task_state else {
            sender.send(
                InvalidRequestSnafu {
                    region_id,
                    reason: "series index is disabled; enable experimental_enable_series_index",
                }
                .fail(),
            );
            return;
        };
        let receiver = match state.try_reconcile(region) {
            Ok(receiver) => receiver,
            Err(error) => {
                sender.send(Err(error));
                return;
            }
        };
        let worker_id = self.id;
        common_runtime::spawn_global(async move {
            let result = receiver
                .await
                .unwrap_or_else(|_| WorkerStoppedSnafu { id: worker_id }.fail());
            sender.send(result.map(|_| 0));
        });
    }

    pub(crate) async fn handle_rebuild_index(
        &mut self,
        request: BuildIndexRequest,
        mut sender: OptionOutputTx,
    ) {
        let region_id = request.region_id;
        let Some(region) = self.regions.writable_region_or(region_id, &mut sender) else {
            return;
        };
        // Staging regions are writable but must not publish indexes.
        if region.should_abort_index() {
            sender.send(
                RegionStateSnafu {
                    region_id,
                    state: region.state(),
                    expect: RegionRoleState::Leader(RegionLeaderState::Writable),
                }
                .fail(),
            );
            return;
        }

        // Only keep the parts of the version that index builds need, so the
        // memtables and SST version are not pinned across the awaits below.
        let (snapshot, all_files) = {
            let version = region.version_control.current().version;
            let all_files: HashMap<FileId, FileHandle> = version
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files.iter())
                .filter(|(_, handle)| {
                    !handle.is_deleted()
                        && !handle.compacting()
                        && handle.level() < file::LEVEL_SHARED
                })
                .map(|(id, handle)| (*id, handle.clone()))
                .collect();
            let snapshot = IndexBuildSnapshot {
                metadata: version.metadata.clone(),
                index_options: version.options.index_options.clone(),
            };
            (snapshot, all_files)
        };
        // A committed index publication may not have reached version control
        // yet. Use the manifest's FileMeta as the conditional publication
        // source while keeping the builder's schema generation from the version.
        let manifest = region.manifest_ctx.manifest().await;
        let current_file_meta = |file_id| {
            let file_meta = manifest.files.get(&file_id);
            if file_meta.is_none() {
                debug!(
                    "Skipping index build because file is absent from manifest, region: {}, file_id: {}",
                    region_id, file_id
                );
            }
            file_meta.cloned()
        };

        let registry = match index_registry(
            &self.config,
            self.plugins.get::<IndexKindProviderRef>(),
        )
        .and_then(|registry| {
            registry
                .validate_required_kinds(&snapshot.metadata, manifest.files.values())
                .with_context(|_| ExternalSnafu {
                    context: "checking required index handlers for rebuild".to_string(),
                })?;
            validate_rebuild_plan(&registry, &snapshot, request.build_type.clone())?;
            Ok(registry)
        }) {
            Ok(registry) => registry,
            Err(err) => {
                let err = Arc::new(err);
                for result_sender in &request.result_senders {
                    let _ = result_sender
                        .send(Err(
                            BuildIndexAsyncSnafu { region_id }.into_error(err.clone())
                        ))
                        .await;
                }
                sender.send(Err(BuildIndexAsyncSnafu { region_id }.into_error(err)));
                return;
            }
        };
        let build_tasks = if request.file_metas.is_empty() {
            // If no specific files are provided, find files whose index is inconsistent with the region metadata.
            // Check the version rather than the manifest: a publication committed
            // to the manifest but not yet applied is still invisible to queries.
            // Its build still holds the file, so the manual task waits for it.
            all_files
                .values()
                .filter(|file| {
                    index_needs_rebuild(
                        &registry,
                        &snapshot,
                        file.meta_ref(),
                        request.build_type.clone(),
                    )
                })
                .filter_map(|file| {
                    let file_meta = current_file_meta(file.meta_ref().file_id)?;
                    Some((file.clone(), file_meta))
                })
                .collect::<Vec<_>>()
        } else {
            request
                .file_metas
                .iter()
                .filter_map(|meta| {
                    let file = all_files.get(&meta.file_id)?;
                    let file_meta = current_file_meta(meta.file_id)?;
                    Some((file.clone(), file_meta))
                })
                .collect::<Vec<_>>()
        };

        if build_tasks.is_empty() {
            debug!(
                "No files need to build index for region {}, request: {:?}",
                region_id, request
            );
            sender.send(Ok(0));
            return;
        }

        let file_ids = build_tasks
            .iter()
            .map(|(file, _)| file.meta_ref().file_id)
            .collect::<Vec<_>>();
        let build_type = request.build_type.clone();
        // Each task holds a sender and reports at most once, so sends never block.
        let (tx, mut rx) = mpsc::channel::<Result<IndexBuildOutcome>>(build_tasks.len());
        for (file_handle, file_meta) in build_tasks {
            debug!(
                "Scheduling index build for region {}, file_id {}",
                region_id,
                file_handle.meta_ref().file_id
            );

            // `set_region_role` can change the state while tasks are scheduled.
            if region.should_abort_index() {
                warn!(
                    "Region {} is in state {:?}, abort index rebuild process for file_id {}",
                    region_id,
                    region.state(),
                    file_handle.meta_ref().file_id
                );
                break;
            }

            let mut result_senders = request.result_senders.clone();
            result_senders.push(tx.clone());
            let task = self.new_index_build_task(
                &region,
                &snapshot,
                registry.clone(),
                (file_handle, file_meta),
                request.build_type.clone(),
                result_senders,
            );
            self.index_build_scheduler
                .schedule_build(&region.version_control, task)
                .await;
        }
        drop(tx);

        // Wait for all index build tasks to finish and notify the caller.
        common_runtime::spawn_global(async move {
            let mut abort_reasons = Vec::new();
            // The channel closes once every task has reported or been dropped.
            while let Some(result) = rx.recv().await {
                match result {
                    Ok(IndexBuildOutcome::Finished) => {}
                    Ok(IndexBuildOutcome::Aborted(reason)) => abort_reasons.push(reason),
                    Err(e) => {
                        warn!(e; "Index build task failed for region: {}", region_id);
                        sender.send(Err(e));
                        return;
                    }
                }
            }

            // The build covers the SSTs that existed when it was requested. A file
            // removed meanwhile, e.g. replaced by compaction, counts as done: its
            // replacement is indexed according to the compaction index options,
            // as any later compaction would be.
            let manifest = region.manifest_ctx.manifest().await;
            let (present, removed): (Vec<_>, Vec<_>) = file_ids
                .into_iter()
                .partition(|file_id| manifest.files.contains_key(file_id));
            if !removed.is_empty() && build_type == IndexBuildType::Manual {
                warn!(
                    "Files were removed during the manual index build, their replacements follow the compaction index options, region: {}, files: {:?}",
                    region_id, removed
                );
            }
            let incomplete = present
                .into_iter()
                .filter(|file_id| {
                    !manifest.files[file_id].is_index_consistent_with_region(
                        &snapshot.metadata.column_metadatas,
                        &snapshot.index_options,
                    )
                })
                .collect::<Vec<_>>();
            let result = if incomplete.is_empty() {
                Ok(0)
            } else {
                // Background builds have no caller, and files they leave
                // unindexed by configuration are expected.
                if build_type == IndexBuildType::Manual {
                    warn!(
                        "Manual index build did not complete, region: {}, files: {:?}, aborted: {:?}",
                        region_id, incomplete, abort_reasons
                    );
                }
                IndexBuildIncompleteSnafu {
                    region_id,
                    file_ids: incomplete,
                }
                .fail()
            };
            sender.send(result);
        });
    }

    pub(crate) async fn handle_index_build_finished(
        &mut self,
        region_id: RegionId,
        request: IndexBuildFinished,
    ) {
        let region = match self.regions.get_region(region_id) {
            Some(region) => region,
            None => {
                warn!(
                    "Region not found for index build finished, region_id: {}",
                    region_id
                );
                return;
            }
        };

        let file_meta = request.file_meta;
        let region_file_id = RegionFileId::new(file_meta.region_id, file_meta.file_id);
        let index_id = RegionIndexId::new(region_file_id, file_meta.index_version);

        // Clean old puffin-related cache before making the new metadata visible.
        let cache_strategy = CacheStrategy::EnableAll(self.cache_manager.clone());
        cache_strategy.evict_puffin_cache(index_id).await;

        let manager = region.manifest_ctx.manifest_manager.read().await;
        let manifest = manager.manifest();
        let is_current = manifest.files.get(&file_meta.file_id) == Some(&file_meta);
        if is_current {
            region.version_control.apply_edit(
                Some(RegionEdit {
                    files_to_add: vec![file_meta.clone()],
                    files_to_remove: Vec::new(),
                    timestamp_ms: None,
                    flushed_sequence: None,
                    flushed_entry_id: None,
                    committed_sequence: None,
                    compaction_time_window: None,
                }),
                &[],
                region.file_purger.clone(),
            );
        }
        drop(manager);

        let outcome = if is_current {
            self.listener.on_index_build_finish(region_file_id).await;
            IndexBuildOutcome::Finished
        } else {
            INDEX_PUBLICATION_STALE_TOTAL
                .with_label_values(&["worker_apply"])
                .inc();
            warn!(
                "Ignores stale index build result, region: {}, file_id: {}, index_version: {}, committed manifest version: {}",
                region_id, file_meta.file_id, file_meta.index_version, request.manifest_version
            );
            cleanup_stale_index_caches(
                index_id,
                &region.access_layer,
                Some(&self.cache_manager),
                self.cache_manager.write_cache(),
            )
            .await;
            self.listener.on_index_build_abort(region_file_id).await;
            IndexBuildOutcome::Aborted(format!(
                "Index publication was superseded before worker apply, region: {}, file_id: {}",
                region_id, file_meta.file_id
            ))
        };
        // Notify waiters after the version is updated, so queries issued after a
        // manual build returns can use the new index. `try_send` keeps the worker
        // loop from waiting on a caller; a missing result is caught by the
        // waiter's manifest check.
        for sender in &request.result_senders {
            let _ = sender.try_send(Ok(outcome.clone()));
        }
    }

    pub(crate) async fn handle_index_build_stopped(
        &mut self,
        region_id: RegionId,
        request: IndexBuildStopped,
    ) {
        self.index_build_scheduler
            .on_task_stopped(region_id, request.file_id);
    }
}

fn index_registry(
    config: &crate::config::MitoConfig,
    provider: Option<IndexKindProviderRef>,
) -> Result<IndexRegistryRef> {
    IndexRegistry::new(
        config.inverted_index.clone(),
        config.fulltext_index.clone(),
        config.bloom_filter_index.clone(),
    )
    .with_provider(provider)
    .map(Arc::new)
    .with_context(|_| ExternalSnafu {
        context: "registering index handlers for rebuild".to_string(),
    })
}

/// Rejects invalid current options before selecting files or scheduling any task.
fn validate_rebuild_plan(
    registry: &IndexRegistry,
    snapshot: &IndexBuildSnapshot,
    build_type: IndexBuildType,
) -> Result<()> {
    let plan = registry
        .plan(&PlanContext {
            metadata: &snapshot.metadata,
            index_options: &snapshot.index_options,
            build_type,
            existing_indexes: &[],
        })
        .with_context(|_| ExternalSnafu {
            context: "planning index rebuild".to_string(),
        })?;
    plan.check_success().context(BuildIndexAsyncSnafu {
        region_id: snapshot.metadata.region_id,
    })
}

fn index_needs_rebuild(
    registry: &IndexRegistry,
    snapshot: &IndexBuildSnapshot,
    file: &FileMeta,
    build_type: IndexBuildType,
) -> bool {
    let plan = match registry.plan(&PlanContext {
        metadata: &snapshot.metadata,
        index_options: &snapshot.index_options,
        build_type: build_type.clone(),
        existing_indexes: &file.indexes,
    }) {
        Ok(plan) => plan,
        // Schedule the task so its preparation reports the error to the caller.
        Err(_) => return true,
    };
    if plan.check_success().is_err() {
        return true;
    }
    !plan.is_current(
        file,
        &CurrencyContext {
            metadata: &snapshot.metadata,
            index_options: &snapshot.index_options,
            build_type,
        },
    )
}

#[cfg(test)]
mod tests {
    use store_api::metadata::RegionMetadataBuilder;

    use super::*;
    use crate::sst::file::ColumnIndexMetadata;
    use crate::sst::index::handler::{
        BuildContext, IndexCreator, IndexKindHandler, PlanEntry, PlannedIndex,
    };
    use crate::sst::index::kind::{ExtensionIndexKind, IndexKind};
    use crate::test_util::CreateRequestBuilder;

    #[derive(Debug)]
    struct MaintenanceHandler;

    impl IndexKindHandler for MaintenanceHandler {
        fn kinds(&self) -> &[IndexKind] {
            &[IndexKind::Extension(ExtensionIndexKind::Hll)]
        }

        fn plan(
            &self,
            context: &PlanContext<'_>,
        ) -> std::result::Result<Option<Arc<dyn PlannedIndex>>, common_error::ext::BoxedError>
        {
            let existing = context.existing_indexes.iter().any(|column| {
                column
                    .created_indexes
                    .contains(&crate::sst::file::IndexType::HllIndex)
            });
            Ok((existing
                && matches!(
                    context.build_type,
                    IndexBuildType::Manual | IndexBuildType::SchemaChange
                ))
            .then(|| Arc::new(Self) as Arc<dyn PlannedIndex>))
        }
    }

    #[async_trait::async_trait]
    impl PlannedIndex for MaintenanceHandler {
        fn kinds(&self) -> &[IndexKind] {
            IndexKindHandler::kinds(self)
        }

        fn entries(&self) -> &[PlanEntry] {
            &[]
        }

        async fn build(
            &self,
            _context: &BuildContext<'_>,
        ) -> std::result::Result<Option<Box<dyn IndexCreator>>, common_error::ext::BoxedError>
        {
            Ok(None)
        }

        fn is_current(&self, _file: &FileMeta, _context: &CurrencyContext<'_>) -> bool {
            false
        }
    }

    fn snapshot_without_builtin_indexes() -> IndexBuildSnapshot {
        let request = CreateRequestBuilder::new().build();
        let mut metadata = RegionMetadataBuilder::new(RegionId::new(1, 1));
        metadata.primary_key(request.primary_key);
        for mut column in request.column_metadatas {
            column.column_schema.set_inverted_index(false);
            column.column_schema.unset_skipping_options().unwrap();
            metadata.push_column_metadata(column);
        }
        IndexBuildSnapshot {
            metadata: Arc::new(metadata.build().unwrap()),
            index_options: IndexOptions::default(),
        }
    }

    #[test]
    fn test_rebuild_selection_uses_registered_currency_and_empty_maintenance_plans() {
        let snapshot = snapshot_without_builtin_indexes();
        let builtin =
            IndexRegistry::new(Default::default(), Default::default(), Default::default());
        let mut registered = builtin.clone();
        registered.register(Arc::new(MaintenanceHandler)).unwrap();
        let empty = FileMeta::default();
        let mut existing = empty.clone();
        existing.indexes.push(ColumnIndexMetadata {
            column_id: 0,
            created_indexes: smallvec::smallvec![crate::sst::file::IndexType::HllIndex],
            ..Default::default()
        });
        for reason in [IndexBuildType::Manual, IndexBuildType::SchemaChange] {
            assert!(
                index_needs_rebuild(&builtin, &snapshot, &existing, reason.clone()),
                "missing required handlers cannot be classified as current"
            );
            assert!(!index_needs_rebuild(
                &registered,
                &snapshot,
                &empty,
                reason.clone()
            ));
            assert!(
                index_needs_rebuild(&registered, &snapshot, &existing, reason),
                "an explicit empty maintenance plan still requests publication"
            );
        }
        for reason in [IndexBuildType::Flush, IndexBuildType::Compact] {
            assert!(!index_needs_rebuild(
                &registered,
                &snapshot,
                &existing,
                reason
            ));
        }
    }

    #[derive(Debug)]
    struct FailingPlanHandler;

    impl IndexKindHandler for FailingPlanHandler {
        fn kinds(&self) -> &[IndexKind] {
            &[IndexKind::Extension(ExtensionIndexKind::Hll)]
        }

        fn plan(
            &self,
            _context: &PlanContext<'_>,
        ) -> std::result::Result<Option<Arc<dyn PlannedIndex>>, common_error::ext::BoxedError>
        {
            Err(common_error::ext::BoxedError::new(
                common_error::ext::PlainError::new(
                    "injected plan failure".into(),
                    common_error::status_code::StatusCode::Internal,
                ),
            ))
        }
    }

    #[test]
    fn test_failed_plan_requires_rebuild_even_without_planned_entries() {
        let snapshot = snapshot_without_builtin_indexes();
        let mut registry =
            IndexRegistry::new(Default::default(), Default::default(), Default::default());
        registry.register(Arc::new(FailingPlanHandler)).unwrap();
        let file = FileMeta {
            indexes: vec![ColumnIndexMetadata {
                column_id: 0,
                created_indexes: smallvec::smallvec![crate::sst::file::IndexType::HllIndex],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(
            registry
                .validate_required_kinds(&snapshot.metadata, [&file])
                .is_ok()
        );
        for reason in [IndexBuildType::Manual, IndexBuildType::SchemaChange] {
            let plan = registry
                .plan(&PlanContext {
                    metadata: &snapshot.metadata,
                    index_options: &snapshot.index_options,
                    build_type: reason.clone(),
                    existing_indexes: &file.indexes,
                })
                .unwrap();
            assert!(plan.entries().next().is_none());
            assert!(plan.check_success().is_err());
            assert!(index_needs_rebuild(&registry, &snapshot, &file, reason));
        }
    }

    #[derive(Debug)]
    struct DuplicateKindProvider;

    impl crate::sst::index::handler::IndexKindProvider for DuplicateKindProvider {
        fn handlers(&self) -> Vec<Arc<dyn IndexKindHandler>> {
            vec![Arc::new(MaintenanceHandler), Arc::new(MaintenanceHandler)]
        }
    }

    #[test]
    fn test_rebuild_registry_registration_failure_is_not_provider_absence() {
        let config = crate::config::MitoConfig::default();
        assert!(index_registry(&config, None).is_ok());
        let error = index_registry(&config, Some(Arc::new(DuplicateKindProvider))).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("registering index handlers for rebuild")
        );
    }

    #[test]
    fn test_rebuild_preflight_rejects_plan_failure_before_file_selection() {
        let snapshot = snapshot_without_builtin_indexes();
        let mut registry =
            IndexRegistry::new(Default::default(), Default::default(), Default::default());
        assert!(validate_rebuild_plan(&registry, &snapshot, IndexBuildType::Manual).is_ok());
        registry.register(Arc::new(FailingPlanHandler)).unwrap();
        assert!(validate_rebuild_plan(&registry, &snapshot, IndexBuildType::Manual).is_err());
    }

    #[test]
    fn test_required_handlers_cover_schema_and_both_persisted_locations() {
        use crate::sst::file::IndexType;
        let registry =
            IndexRegistry::new(Default::default(), Default::default(), Default::default());
        for kind in [IndexType::HllIndex, IndexType::KllIndex, IndexType::MgIndex] {
            let mut snapshot = snapshot_without_builtin_indexes();
            let schema =
                &mut Arc::make_mut(&mut snapshot.metadata).column_metadatas[0].column_schema;
            let (key, value) = match kind {
                IndexType::HllIndex => (datatypes::schema::HLL_KEY, r#"{"precision":10}"#),
                IndexType::KllIndex => (datatypes::schema::KLL_KEY, r#"{"capacity":64}"#),
                IndexType::MgIndex => (datatypes::schema::MG_KEY, r#"{"max-counters":64}"#),
                _ => unreachable!(),
            };
            schema.mut_metadata().insert(key.into(), value.into());
            assert!(
                registry
                    .validate_required_kinds(&snapshot.metadata, [])
                    .is_err()
            );
            assert!(validate_rebuild_plan(&registry, &snapshot, IndexBuildType::Manual).is_err());
            assert!(
                registry
                    .plan(&PlanContext {
                        metadata: &snapshot.metadata,
                        index_options: &snapshot.index_options,
                        build_type: IndexBuildType::Flush,
                        existing_indexes: &[],
                    })
                    .is_ok()
            );
            let snapshot = snapshot_without_builtin_indexes();
            for column_level in [false, true] {
                let mut file = FileMeta::default();
                if column_level {
                    file.indexes.push(ColumnIndexMetadata {
                        column_id: 0,
                        created_indexes: smallvec::smallvec![kind],
                        ..Default::default()
                    });
                } else {
                    file.available_indexes.push(kind);
                }
                assert!(
                    registry
                        .validate_required_kinds(&snapshot.metadata, [&file])
                        .is_err()
                );
            }
        }
        let snapshot = snapshot_without_builtin_indexes();
        let legacy = FileMeta {
            available_indexes: smallvec::smallvec![IndexType::VectorIndex],
            ..Default::default()
        };
        assert!(
            registry
                .validate_required_kinds(&snapshot.metadata, [&legacy])
                .is_ok()
        );
    }
}
