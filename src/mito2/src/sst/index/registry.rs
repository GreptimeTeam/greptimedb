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

//! Ordered registration and planning of index implementations.

use std::collections::HashSet;
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_telemetry::warn;
use snafu::IntoError;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::config::{BloomFilterConfig, FulltextIndexConfig, InvertedIndexConfig};
use crate::error::{Error, ExternalSnafu, InvalidConfigSnafu};
use crate::metrics::INDEX_CREATE_DISCARDED_TOTAL;
use crate::sst::file::{FileMeta, IndexType};
use crate::sst::index::handler::builtin::BuiltinIndexHandler;
use crate::sst::index::handler::{
    CurrencyContext, IndexKindHandler, IndexKindProviderRef, PlanContext, PlanEntry, PlannedIndex,
};
use crate::sst::index::kind::IndexKind;

/// Shared registry installed by the engine and its embedding application.
pub type IndexRegistryRef = Arc<IndexRegistry>;

/// One handler per owner; related algorithms may share one creator and row traversal.
#[derive(Debug, Clone)]
pub struct IndexRegistry {
    handlers: Vec<Arc<dyn IndexKindHandler>>,
}

impl IndexRegistry {
    /// Registers the existing built-in creators in their established order.
    pub fn new(
        inverted: InvertedIndexConfig,
        fulltext: FulltextIndexConfig,
        bloom: BloomFilterConfig,
    ) -> Self {
        Self {
            handlers: vec![
                Arc::new(BuiltinIndexHandler::Inverted(inverted)),
                Arc::new(BuiltinIndexHandler::Fulltext(fulltext)),
                Arc::new(BuiltinIndexHandler::Bloom(bloom)),
            ],
        }
    }

    /// Adds the embedding application's handlers after the built-in configuration.
    pub fn with_provider(
        mut self,
        provider: Option<IndexKindProviderRef>,
    ) -> Result<Self, BoxedError> {
        if let Some(provider) = provider {
            for handler in provider.handlers() {
                self.register(handler)?;
            }
        }
        Ok(self)
    }

    /// Strictly validates configured options before a region is opened.
    pub fn validate(&self, context: &PlanContext<'_>) -> Result<(), BoxedError> {
        for handler in &self.handlers {
            handler.validate(context)?;
        }
        Ok(())
    }

    /// Installs a handler, rejecting empty or overlapping kind ownership.
    pub fn register(&mut self, handler: Arc<dyn IndexKindHandler>) -> Result<(), BoxedError> {
        self.validate_registration(handler.kinds())?;
        self.handlers.push(handler);
        self.handlers
            .sort_by_key(|handler| handler.kinds().iter().map(|kind| kind.sort_key()).min());
        Ok(())
    }

    /// Rejects required extension indexes whose implementation is unavailable.
    /// Checks current schema options and both file-level and column-level metadata.
    pub fn validate_required_kinds<'a>(
        &self,
        metadata: &RegionMetadataRef,
        files: impl IntoIterator<Item = &'a FileMeta>,
    ) -> Result<(), BoxedError> {
        self.validate_schema_kinds(metadata)?;
        for file in files {
            for kind in file.available_indexes.iter().chain(
                file.indexes
                    .iter()
                    .flat_map(|column| &column.created_indexes),
            ) {
                self.validate_required_kind(*kind)?;
            }
        }
        Ok(())
    }

    /// Rejects kinds the current schema requires but no handler implements.
    pub fn validate_configured_kinds(
        &self,
        metadata: &RegionMetadataRef,
    ) -> Result<(), BoxedError> {
        self.validate_schema_kinds(metadata)
    }

    /// Reports persisted kinds without handlers once per kind in this call.
    /// Compaction calls this after validating the configuration and publishes
    /// only the indexes its registered handlers produce.
    pub fn report_unavailable_persisted_kinds<'a>(
        &self,
        region_id: RegionId,
        files: impl IntoIterator<Item = &'a FileMeta>,
    ) {
        let unavailable: HashSet<_> = files
            .into_iter()
            .flat_map(|file| {
                file.available_indexes.iter().chain(
                    file.indexes
                        .iter()
                        .flat_map(|column| &column.created_indexes),
                )
            })
            .copied()
            .filter(|kind| !self.has_handler(*kind))
            .collect();
        for kind in unavailable {
            warn!(
                "Omitting persisted index without a provider, region_id: {}, kind: {}, reason: missing_provider",
                region_id,
                kind.as_str()
            );
            INDEX_CREATE_DISCARDED_TOTAL
                .with_label_values(&[kind.as_str(), "missing_provider"])
                .inc();
        }
    }

    /// Prepares each handler once. Construction failures retain the existing
    /// built-in test panic and production skip policy. Other failures are retained
    /// for the caller to check before publishing rebuilt indexes.
    pub fn plan(&self, context: &PlanContext<'_>) -> Result<IndexBuildPlan, BoxedError> {
        self.validate_plan_requirements(context)?;
        let mut planned = Vec::new();
        let mut failure = None;
        for handler in &self.handlers {
            match handler.plan(context) {
                Ok(Some(plan)) => planned.push(plan),
                Ok(None) => {}
                Err(err) => {
                    Self::record_planning_error(handler.as_ref(), context, err, &mut failure);
                }
            }
        }
        Ok(IndexBuildPlan { planned, failure })
    }

    fn validate_registration(&self, kinds: &[IndexKind]) -> Result<(), BoxedError> {
        if kinds.is_empty()
            || kinds
                .iter()
                .enumerate()
                .any(|(i, kind)| kinds[..i].contains(kind))
        {
            return Err(BoxedError::new(
                InvalidConfigSnafu {
                    reason: "index handler kinds must be nonempty and unique",
                }
                .build(),
            ));
        }
        if kinds.iter().any(|kind| self.has_handler(kind.index_type())) {
            return Err(BoxedError::new(
                InvalidConfigSnafu {
                    reason: "index handler kind is already registered",
                }
                .build(),
            ));
        }
        Ok(())
    }

    fn has_handler(&self, kind: IndexType) -> bool {
        self.handlers.iter().any(|handler| {
            handler
                .kinds()
                .iter()
                .any(|owned| owned.index_type() == kind)
        })
    }

    fn validate_plan_requirements(&self, context: &PlanContext<'_>) -> Result<(), BoxedError> {
        // Flush has no extension producer; required handlers are checked when
        // compaction or maintenance plans actually build these indexes.
        if context.build_type != crate::sst::index::IndexBuildType::Flush {
            self.validate_schema_kinds(context.metadata)?;
            for kind in context
                .existing_indexes
                .iter()
                .flat_map(|column| &column.created_indexes)
            {
                self.validate_required_kind(*kind)?;
            }
        }
        Ok(())
    }

    fn record_planning_error(
        handler: &dyn IndexKindHandler,
        context: &PlanContext<'_>,
        err: BoxedError,
        failure: &mut Option<Arc<Error>>,
    ) {
        if handler.kinds().iter().all(|kind| kind.is_builtin()) && cfg!(any(test, feature = "test"))
        {
            panic!(
                "Failed to prepare built-in index, region_id: {}, err: {err}",
                context.metadata.region_id
            );
        }
        warn!(err; "Failed to prepare index, region_id: {}, kinds: {:?}", context.metadata.region_id, handler.kinds());
        failure.get_or_insert_with(|| {
            Arc::new(
                ExternalSnafu {
                    context: "index planning".to_string(),
                }
                .into_error(err),
            )
        });
    }

    fn validate_schema_kinds(&self, metadata: &RegionMetadataRef) -> Result<(), BoxedError> {
        for column in &metadata.column_metadatas {
            let schema = &column.column_schema;
            for (key, kind) in [
                (datatypes::schema::HLL_KEY, IndexType::HllIndex),
                (datatypes::schema::KLL_KEY, IndexType::KllIndex),
                (datatypes::schema::MG_KEY, IndexType::MgIndex),
            ] {
                if schema.metadata().contains_key(key) {
                    self.validate_required_kind(kind)?;
                }
            }
        }
        Ok(())
    }

    fn validate_required_kind(&self, kind: IndexType) -> Result<(), BoxedError> {
        if matches!(
            kind,
            IndexType::HllIndex | IndexType::KllIndex | IndexType::MgIndex
        ) && !self.has_handler(kind)
        {
            return Err(BoxedError::new(
                InvalidConfigSnafu {
                    reason: format!("required index {kind:?} has no registered handler"),
                }
                .build(),
            ));
        }
        Ok(())
    }
}

/// Typed plans retained across output files of one operation.
#[derive(Debug, Clone, Default)]
pub struct IndexBuildPlan {
    planned: Vec<Arc<dyn PlannedIndex>>,
    failure: Option<Arc<Error>>,
}

impl IndexBuildPlan {
    /// Reports preparation failure separately from a successful empty plan.
    pub fn check_success(&self) -> Result<(), Arc<Error>> {
        self.failure.clone().map_or(Ok(()), Err)
    }

    /// Plans in execution order, grouped by owner rather than by column.
    pub fn planned(&self) -> &[Arc<dyn PlannedIndex>] {
        &self.planned
    }
    /// Whether no handler requested work, including metadata-only maintenance.
    pub fn is_empty(&self) -> bool {
        self.planned.is_empty()
    }
    /// All requested column formats.
    pub fn entries(&self) -> impl Iterator<Item = &PlanEntry> {
        self.planned.iter().flat_map(|plan| plan.entries())
    }
    /// Whether any planned implementation can reuse persisted input indexes.
    pub fn needs_input_reuse(&self) -> bool {
        self.planned.iter().any(|plan| plan.needs_input_reuse())
    }

    /// Omits optional extension work when the caller cannot admit its memory budget.
    /// Records each requested kind once per target plan, rather than once per column.
    pub fn omit_extensions_for_budget(&mut self, region_id: RegionId) {
        let omitted: HashSet<_> = self
            .entries()
            .filter(|entry| !entry.kind.is_builtin())
            .map(|entry| entry.kind)
            .collect();
        for kind in omitted {
            warn!(
                "Omitting index before construction, region_id: {}, kind: {}, reason: admission_budget",
                region_id,
                kind.as_str()
            );
            INDEX_CREATE_DISCARDED_TOTAL
                .with_label_values(&[kind.as_str(), "admission_budget"])
                .inc();
        }
        self.planned
            .retain(|plan| plan.kinds().iter().all(|kind| kind.is_builtin()));
    }

    /// Whether every handler considers the published file current.
    pub fn is_current(&self, file: &FileMeta, context: &CurrencyContext<'_>) -> bool {
        self.failure.is_none()
            && self
                .planned
                .iter()
                .all(|plan| plan.is_current(file, context))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::index::kind::ExtensionIndexKind;

    #[derive(Debug)]
    struct TestHandler(Vec<IndexKind>);

    impl IndexKindHandler for TestHandler {
        fn kinds(&self) -> &[IndexKind] {
            &self.0
        }
        fn plan(&self, _: &PlanContext<'_>) -> Result<Option<Arc<dyn PlannedIndex>>, BoxedError> {
            Ok(None)
        }
    }

    #[derive(Debug)]
    struct TestPlan {
        kinds: Vec<IndexKind>,
        entries: Vec<PlanEntry>,
    }

    #[async_trait::async_trait]
    impl PlannedIndex for TestPlan {
        fn kinds(&self) -> &[IndexKind] {
            &self.kinds
        }
        fn entries(&self) -> &[PlanEntry] {
            &self.entries
        }
        async fn build(
            &self,
            _: &crate::sst::index::handler::BuildContext<'_>,
        ) -> Result<Option<Box<dyn crate::sst::index::handler::IndexCreator>>, BoxedError> {
            unreachable!("admission must not construct creators")
        }
        fn is_current(&self, _: &FileMeta, _: &CurrencyContext<'_>) -> bool {
            false
        }
    }

    #[test]
    fn test_unavailable_persisted_kinds_reported_once_per_call() {
        let mut registry =
            IndexRegistry::new(Default::default(), Default::default(), Default::default());
        let kind = IndexType::KllIndex;
        let file = FileMeta {
            available_indexes: smallvec::smallvec![kind],
            indexes: vec![crate::sst::file::ColumnIndexMetadata {
                column_id: 0,
                created_indexes: smallvec::smallvec![kind],
                fulltext_bloom_version: 0,
            }],
            ..Default::default()
        };
        let counter =
            INDEX_CREATE_DISCARDED_TOTAL.with_label_values(&[kind.as_str(), "missing_provider"]);
        let before = counter.get();
        registry.report_unavailable_persisted_kinds(file.region_id, [&file, &file]);
        assert_eq!(counter.get(), before + 1);
        registry.report_unavailable_persisted_kinds(file.region_id, [&file]);
        assert_eq!(counter.get(), before + 2);

        registry
            .register(Arc::new(TestHandler(vec![IndexKind::Extension(
                ExtensionIndexKind::Kll,
            )])))
            .unwrap();
        registry.report_unavailable_persisted_kinds(file.region_id, [&file]);
        assert_eq!(counter.get(), before + 2);
    }

    #[test]
    fn test_admission_omission_records_each_requested_kind_once() {
        let builtin = IndexKind::Bloom;
        let enabled = IndexKind::Extension(ExtensionIndexKind::Mg);
        let disabled = IndexKind::Extension(ExtensionIndexKind::Kll);
        let entry = |kind, column_id| PlanEntry {
            kind,
            column_id,
            format_version: 1,
        };
        let mut plan = IndexBuildPlan {
            planned: vec![
                Arc::new(TestPlan {
                    kinds: vec![builtin],
                    entries: vec![entry(builtin, 0)],
                }),
                Arc::new(TestPlan {
                    kinds: vec![enabled, disabled],
                    entries: vec![entry(enabled, 0), entry(enabled, 1)],
                }),
            ],
            failure: None,
        };
        let counter =
            INDEX_CREATE_DISCARDED_TOTAL.with_label_values(&[enabled.as_str(), "admission_budget"]);
        let disabled_counter = INDEX_CREATE_DISCARDED_TOTAL
            .with_label_values(&[disabled.as_str(), "admission_budget"]);
        let before = counter.get();
        let disabled_before = disabled_counter.get();
        plan.omit_extensions_for_budget(RegionId::new(1, 1));
        assert_eq!(
            plan.entries().cloned().collect::<Vec<_>>(),
            vec![entry(builtin, 0)]
        );
        assert_eq!(counter.get(), before + 1);
        assert_eq!(disabled_counter.get(), disabled_before);
        assert!(plan.check_success().is_ok());
        plan.omit_extensions_for_budget(RegionId::new(1, 1));
        assert_eq!(counter.get(), before + 1);
    }

    #[test]
    fn test_registration_preserves_grouped_owner_and_rejects_overlap() {
        let mut registry =
            IndexRegistry::new(Default::default(), Default::default(), Default::default());
        let kinds = vec![
            IndexKind::Extension(ExtensionIndexKind::Hll),
            IndexKind::Extension(ExtensionIndexKind::Kll),
            IndexKind::Extension(ExtensionIndexKind::Mg),
        ];
        registry
            .register(Arc::new(TestHandler(kinds.clone())))
            .unwrap();
        assert_eq!(registry.handlers.len(), 4);
        assert_eq!(registry.handlers[3].kinds(), kinds);
        for invalid in [vec![], vec![kinds[0]], vec![IndexKind::Fulltext]] {
            assert!(registry.register(Arc::new(TestHandler(invalid))).is_err());
            assert_eq!(registry.handlers.len(), 4);
        }
    }

    #[test]
    fn test_registration_rejects_duplicates_within_one_owner() {
        let mut registry =
            IndexRegistry::new(Default::default(), Default::default(), Default::default());
        let kind = IndexKind::Extension(ExtensionIndexKind::Hll);
        assert!(
            registry
                .register(Arc::new(TestHandler(vec![kind, kind])))
                .is_err()
        );
        assert_eq!(registry.handlers.len(), 3);
    }
}
