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

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_telemetry::warn;

use crate::config::{BloomFilterConfig, FulltextIndexConfig, InvertedIndexConfig};
use crate::error::InvalidConfigSnafu;
use crate::sst::file::FileMeta;
use crate::sst::index::handler::builtin::BuiltinIndexHandler;
use crate::sst::index::handler::{
    CurrencyContext, IndexKindHandler, IndexKindProviderRef, PlanContext, PlanEntry, PlannedIndex,
};

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
        let kinds = handler.kinds();
        if kinds.is_empty()
            || kinds.iter().any(|kind| {
                matches!(
                    kind,
                    crate::sst::index::kind::IndexKind::Builtin(
                        crate::sst::file::IndexType::Hll
                            | crate::sst::file::IndexType::Kll
                            | crate::sst::file::IndexType::Mg
                    )
                )
            })
            || kinds.iter().enumerate().any(|(i, kind)| {
                kinds[..i]
                    .iter()
                    .any(|previous| previous.index_type() == kind.index_type())
            })
        {
            return Err(BoxedError::new(
                InvalidConfigSnafu {
                    reason: "index handler kinds must be nonempty, unique, and use extension ownership for sketches",
                }
                .build(),
            ));
        }
        if self.handlers.iter().any(|existing| {
            existing.kinds().iter().any(|kind| {
                kinds
                    .iter()
                    .any(|new_kind| new_kind.index_type() == kind.index_type())
            })
        }) {
            return Err(BoxedError::new(
                InvalidConfigSnafu {
                    reason: "index handler kind is already registered",
                }
                .build(),
            ));
        }
        self.handlers.push(handler);
        self.handlers
            .sort_by_key(|handler| handler.kinds().iter().map(|kind| kind.sort_key()).min());
        Ok(())
    }

    /// Prepares each handler once. Construction failures retain the existing
    /// built-in test panic and production skip policy; extensions remain optional.
    pub fn plan(&self, context: &PlanContext<'_>) -> Result<IndexBuildPlan, BoxedError> {
        let mut planned = Vec::new();
        for handler in &self.handlers {
            match handler.plan(context) {
                Ok(Some(plan)) => planned.push(plan),
                Ok(None) => {}
                Err(err) => {
                    if handler.kinds().iter().all(|kind| kind.is_builtin())
                        && cfg!(any(test, feature = "test"))
                    {
                        panic!(
                            "Failed to prepare built-in index, region_id: {}, err: {err}",
                            context.metadata.region_id
                        );
                    }
                    warn!(err; "Failed to prepare index, region_id: {}, kinds: {:?}", context.metadata.region_id, handler.kinds());
                }
            }
        }
        Ok(IndexBuildPlan { planned })
    }
}

/// Typed plans retained across output files of one operation.
#[derive(Debug, Clone, Default)]
pub struct IndexBuildPlan {
    planned: Vec<Arc<dyn PlannedIndex>>,
}

impl IndexBuildPlan {
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
    pub fn without_extensions(&self) -> Self {
        Self {
            planned: self
                .planned
                .iter()
                .filter(|plan| plan.kinds().iter().all(|kind| kind.is_builtin()))
                .cloned()
                .collect(),
        }
    }

    /// Whether every handler considers the published file current.
    pub fn is_current(&self, file: &FileMeta, context: &CurrencyContext<'_>) -> bool {
        self.planned
            .iter()
            .all(|plan| plan.is_current(file, context))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::file::IndexType;
    use crate::sst::index::kind::{ExtensionIndexKind, IndexKind};

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
        for invalid in [
            vec![],
            vec![kinds[0]],
            vec![IndexKind::Builtin(IndexType::FulltextIndex)],
        ] {
            assert!(registry.register(Arc::new(TestHandler(invalid))).is_err());
            assert_eq!(registry.handlers.len(), 4);
        }
    }

    #[test]
    fn test_registration_rejects_sketch_builtin_ownership() {
        let mut registry =
            IndexRegistry::new(Default::default(), Default::default(), Default::default());
        for kind in [IndexType::Hll, IndexType::Kll, IndexType::Mg] {
            assert!(
                registry
                    .register(Arc::new(TestHandler(vec![IndexKind::Builtin(kind)])))
                    .is_err()
            );
        }
        assert_eq!(registry.handlers.len(), 3);
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
