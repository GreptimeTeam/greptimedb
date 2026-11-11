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
use crate::metrics::{INDEX_CREATE_DISCARDED_TOTAL, INDEX_DISCARD_ADMISSION_REJECTED};
use crate::sst::file::{FileMeta, IndexType};
use crate::sst::index::IndexBuildType;
use crate::sst::index::handler::builtin::BuiltinIndexHandler;
use crate::sst::index::handler::{
    IndexKindHandler, IndexKindProviderRef, PlanContext, PlanEntry, PlannedIndex,
};
use crate::sst::index::kind::{ExtensionIndexKind, IndexKind};

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
            .sort_by_key(|handler| handler.kinds().iter().min().copied());
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

    /// Prepares each handler once. Construction failures retain the existing
    /// built-in test panic and production skip policy. Other failures are retained
    /// for the caller to check before publishing rebuilt indexes. Required-provider
    /// validation belongs to the operation entry point, before calling this method.
    pub fn plan(&self, context: &PlanContext<'_>) -> IndexBuildPlan {
        let mut planned = Vec::new();
        let mut failure = None;
        for handler in &self.handlers {
            if context.build_type == IndexBuildType::Flush
                && !handler.kinds().iter().all(|kind| kind.is_builtin())
            {
                continue;
            }
            let result = handler.plan(context).and_then(|plan| {
                if let Some(plan) = &plan {
                    Self::validate_plan(handler.as_ref(), plan.as_ref())?;
                }
                Ok(plan)
            });
            match result {
                Ok(Some(plan)) => planned.push(plan),
                Ok(None) => {}
                Err(err) => {
                    Self::record_planning_error(handler.as_ref(), context, err, &mut failure);
                }
            }
        }
        IndexBuildPlan { planned, failure }
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
        if kinds.iter().any(|kind| kind.is_builtin()) && kinds.iter().any(|kind| !kind.is_builtin())
        {
            return Err(BoxedError::new(
                InvalidConfigSnafu {
                    reason: "index handler cannot mix built-in and extension kinds",
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

    fn validate_plan(
        handler: &dyn IndexKindHandler,
        plan: &dyn PlannedIndex,
    ) -> Result<(), BoxedError> {
        let kinds = plan.kinds();
        if kinds.is_empty()
            || kinds
                .iter()
                .enumerate()
                .any(|(i, kind)| kinds[..i].contains(kind) || !handler.kinds().contains(kind))
            || plan
                .entries()
                .iter()
                .any(|entry| !kinds.contains(&entry.kind))
        {
            return Err(BoxedError::new(
                InvalidConfigSnafu {
                    reason: "index plan kinds must be nonempty, unique, owned by the handler, and include every entry kind",
                }
                .build(),
            ));
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
            for kind in ExtensionIndexKind::ALL {
                if schema.metadata().contains_key(kind.required_metadata_key()) {
                    self.validate_required_kind(IndexKind::Extension(kind).index_type())?;
                }
            }
        }
        Ok(())
    }

    fn validate_required_kind(&self, kind: IndexType) -> Result<(), BoxedError> {
        if ExtensionIndexKind::ALL
            .iter()
            .any(|extension| IndexKind::Extension(*extension).index_type() == kind)
            && !self.has_handler(kind)
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
                "Omitting index before construction, region_id: {}, kind: {}, reason: admission_rejected",
                region_id,
                kind.as_str()
            );
            INDEX_CREATE_DISCARDED_TOTAL
                .with_label_values(&[kind.as_str(), INDEX_DISCARD_ADMISSION_REJECTED])
                .inc();
        }
        self.planned
            .retain(|plan| plan.kinds().iter().all(|kind| kind.is_builtin()));
    }

    /// Whether every handler considers the published file current.
    pub fn is_current(&self, file: &FileMeta, context: &PlanContext<'_>) -> bool {
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

        fn is_current(&self, _: &FileMeta, _: &PlanContext<'_>) -> bool {
            false
        }
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
        let counter = INDEX_CREATE_DISCARDED_TOTAL
            .with_label_values(&[enabled.as_str(), "admission_rejected"]);
        let disabled_counter = INDEX_CREATE_DISCARDED_TOTAL
            .with_label_values(&[disabled.as_str(), "admission_rejected"]);
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

    #[derive(Debug)]
    struct PlannedHandler {
        owned: Vec<IndexKind>,
        plan: Arc<TestPlan>,
    }

    impl IndexKindHandler for PlannedHandler {
        fn kinds(&self) -> &[IndexKind] {
            &self.owned
        }

        fn plan(&self, _: &PlanContext<'_>) -> Result<Option<Arc<dyn PlannedIndex>>, BoxedError> {
            Ok(Some(self.plan.clone()))
        }
    }

    #[test]
    fn test_plans_validate_ownership_without_removing_valid_plans() {
        let hll = IndexKind::Extension(ExtensionIndexKind::Hll);
        let kll = IndexKind::Extension(ExtensionIndexKind::Kll);
        let metadata = Arc::new(crate::test_util::sst_util::sst_region_metadata());
        let options = Default::default();
        let context = PlanContext {
            metadata: &metadata,
            index_options: &options,
            build_type: crate::sst::index::IndexBuildType::Compact,
            existing_indexes: &[],
        };
        for (kinds, entry_kind, invalid) in [
            (vec![], hll, true),
            (vec![hll, hll], hll, true),
            (vec![kll], kll, true),
            (vec![hll], kll, true),
            (vec![hll], hll, false),
        ] {
            let mut registry =
                IndexRegistry::new(Default::default(), Default::default(), Default::default());
            registry
                .register(Arc::new(PlannedHandler {
                    owned: vec![hll, IndexKind::Extension(ExtensionIndexKind::Mg)],
                    plan: Arc::new(TestPlan {
                        kinds,
                        entries: vec![PlanEntry {
                            column_id: 0,
                            kind: entry_kind,
                            format_version: 1,
                        }],
                    }),
                }))
                .unwrap();
            registry
                .register(Arc::new(PlannedHandler {
                    owned: vec![kll],
                    plan: Arc::new(TestPlan {
                        kinds: vec![kll],
                        entries: vec![PlanEntry {
                            column_id: 0,
                            kind: kll,
                            format_version: 1,
                        }],
                    }),
                }))
                .unwrap();
            let plan = registry.plan(&context);
            assert_eq!(plan.check_success().is_err(), invalid);
            assert_eq!(
                plan.entries()
                    .filter(|entry| !entry.kind.is_builtin())
                    .map(|entry| entry.kind)
                    .collect::<Vec<_>>(),
                if invalid { vec![kll] } else { vec![hll, kll] }
            );
            assert_eq!(registry.handlers.len(), 5);
        }
    }

    #[test]
    fn test_registration_rejects_mixed_builtin_and_extension_owner() {
        let mut registry = IndexRegistry {
            handlers: Vec::new(),
        };
        assert!(
            registry
                .register(Arc::new(TestHandler(vec![
                    IndexKind::Inverted,
                    IndexKind::Extension(ExtensionIndexKind::Hll),
                ])))
                .is_err()
        );
        assert!(registry.handlers.is_empty());
    }
}
