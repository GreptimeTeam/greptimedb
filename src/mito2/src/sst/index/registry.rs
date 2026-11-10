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

//! Ordered planning of built-in index implementations.

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_telemetry::warn;

use crate::config::{BloomFilterConfig, FulltextIndexConfig, InvertedIndexConfig};
use crate::sst::index::handler::builtin::BuiltinIndexHandler;
use crate::sst::index::handler::{IndexKindHandler, PlanContext, PlannedIndex};

/// Shared registry installed by the engine.
pub(crate) type IndexRegistryRef = Arc<IndexRegistry>;

/// Built-in handlers in index creation order.
#[derive(Debug, Clone)]
pub(crate) struct IndexRegistry {
    handlers: Vec<Arc<dyn IndexKindHandler>>,
}

impl IndexRegistry {
    /// Registers the existing built-in creators in their established order.
    pub(crate) fn new(
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

    /// Prepares each handler once, retaining the existing test panic and
    /// production log-and-skip policy for construction failures.
    pub(crate) fn plan(&self, context: &PlanContext<'_>) -> IndexBuildPlan {
        let mut planned = Vec::new();
        for handler in &self.handlers {
            match handler.plan(context) {
                Ok(Some(plan)) => planned.push(plan),
                Ok(None) => {}
                Err(err) => {
                    Self::record_planning_error(handler.as_ref(), context, err);
                }
            }
        }
        IndexBuildPlan { planned }
    }

    fn record_planning_error(
        handler: &dyn IndexKindHandler,
        context: &PlanContext<'_>,
        err: BoxedError,
    ) {
        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to prepare built-in index, region_id: {}, err: {err}",
                context.metadata.region_id
            );
        }
        warn!(err; "Failed to prepare index, region_id: {}, kind: {:?}", context.metadata.region_id, handler.kind());
    }
}

/// Typed plans retained across output files of one operation.
#[derive(Debug, Clone, Default)]
pub(crate) struct IndexBuildPlan {
    planned: Vec<Arc<dyn PlannedIndex>>,
}

impl IndexBuildPlan {
    /// Plans in index creation order.
    pub(crate) fn planned(&self) -> &[Arc<dyn PlannedIndex>] {
        &self.planned
    }
}
