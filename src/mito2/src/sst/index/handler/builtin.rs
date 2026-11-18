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

//! Dispatches built-in planning and shares column metadata policy.

mod bloom;
mod fulltext;
mod inverted;

use std::sync::Arc;

use common_error::ext::BoxedError;
use index::target::IndexTarget;

use crate::config::{BloomFilterConfig, FulltextIndexConfig, InvertedIndexConfig};
use crate::sst::file::FileMeta;
use crate::sst::index::handler::builtin::bloom::BloomPlan;
use crate::sst::index::handler::builtin::fulltext::FulltextPlan;
use crate::sst::index::handler::builtin::inverted::InvertedPlan;
use crate::sst::index::handler::{IndexKindHandler, PlanContext, PlanEntry, PlannedIndex};
use crate::sst::index::kind::IndexKind;

/// Configuration for one of the existing file-level built-in creators.
#[derive(Debug, Clone)]
pub(crate) enum BuiltinIndexHandler {
    Inverted(InvertedIndexConfig),
    Fulltext(FulltextIndexConfig),
    Bloom(BloomFilterConfig),
}

impl IndexKindHandler for BuiltinIndexHandler {
    fn kinds(&self) -> &[IndexKind] {
        match self {
            Self::Inverted(_) => &[IndexKind::Inverted],
            Self::Fulltext(_) => &[IndexKind::Fulltext],
            Self::Bloom(_) => &[IndexKind::Bloom],
        }
    }

    fn validate(&self, _context: &PlanContext<'_>) -> Result<(), BoxedError> {
        // Built-in options retain their existing schema/build-time validation.
        // Planning here would make region creation reject inputs it previously accepted.
        Ok(())
    }

    fn plan(&self, context: &PlanContext<'_>) -> Result<Option<Arc<dyn PlannedIndex>>, BoxedError> {
        match self {
            Self::Inverted(config) => InvertedPlan::plan(config, context)
                .map(|plan| plan.map(|plan| plan as Arc<dyn PlannedIndex>)),
            Self::Fulltext(config) => FulltextPlan::plan(config, context),
            Self::Bloom(config) => BloomPlan::plan(config, context),
        }
    }
}

fn target_column_id(target: &IndexTarget) -> store_api::storage::ColumnId {
    match target {
        IndexTarget::ColumnId(id) => *id,
        IndexTarget::JsonPath(path) => path.column_id(),
    }
}

fn column_target_id(target: &IndexTarget) -> Option<store_api::storage::ColumnId> {
    match target {
        IndexTarget::ColumnId(id) => Some(*id),
        IndexTarget::JsonPath(_) => None,
    }
}

fn normalize_entries(entries: &mut Vec<PlanEntry>) {
    entries.sort_by_key(|entry| (entry.column_id, entry.format_version));
    entries.dedup_by(|a, b| {
        if a.column_id == b.column_id {
            b.format_version = b.format_version.max(a.format_version);
            true
        } else {
            false
        }
    });
}

fn builtin_is_current(file: &FileMeta, context: &PlanContext<'_>) -> bool {
    // Preserve the established schema/version policy, including its JSON rebuild scope.
    file.is_index_consistent_with_region(&context.metadata.column_metadatas, context.index_options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::region::options::IndexOptions;
    use crate::sst::index::IndexBuildType;
    use crate::test_util::sst_util;

    #[test]
    fn test_builtin_validation_preserves_build_time_option_errors() {
        for (handler, key) in [
            (
                BuiltinIndexHandler::Fulltext(FulltextIndexConfig::default()),
                datatypes::schema::FULLTEXT_KEY,
            ),
            (
                BuiltinIndexHandler::Bloom(BloomFilterConfig::default()),
                datatypes::schema::SKIPPING_INDEX_KEY,
            ),
        ] {
            let mut metadata = sst_util::sst_region_metadata();
            let column = metadata
                .column_metadatas
                .iter_mut()
                .find(|column| column.semantic_type == api::v1::SemanticType::Field)
                .unwrap();
            column.column_schema =
                column
                    .column_schema
                    .clone()
                    .with_metadata(std::collections::HashMap::from([(
                        key.to_string(),
                        "invalid json".to_string(),
                    )]));
            let metadata = Arc::new(metadata);
            let options = IndexOptions::default();
            let context = PlanContext {
                metadata: &metadata,
                index_options: &options,
                build_type: IndexBuildType::Flush,
                existing_indexes: &[],
            };
            assert!(handler.validate(&context).is_ok());
            assert!(handler.plan(&context).is_err());
        }
    }

    #[test]
    fn test_builtin_planning_reports_invalid_options() {
        for (handler, key) in [
            (
                BuiltinIndexHandler::Fulltext(FulltextIndexConfig::default()),
                datatypes::schema::FULLTEXT_KEY,
            ),
            (
                BuiltinIndexHandler::Bloom(BloomFilterConfig::default()),
                datatypes::schema::SKIPPING_INDEX_KEY,
            ),
        ] {
            let mut metadata = sst_util::sst_region_metadata();
            let column = metadata
                .column_metadatas
                .iter_mut()
                .find(|column| column.semantic_type == api::v1::SemanticType::Field)
                .unwrap();
            column.column_schema =
                column
                    .column_schema
                    .clone()
                    .with_metadata(std::collections::HashMap::from([(
                        key.to_string(),
                        "invalid json".to_string(),
                    )]));
            let metadata = Arc::new(metadata);
            let options = IndexOptions::default();
            let context = PlanContext {
                metadata: &metadata,
                index_options: &options,
                build_type: IndexBuildType::Flush,
                existing_indexes: &[],
            };
            assert!(handler.plan(&context).is_err());
        }
    }
}
