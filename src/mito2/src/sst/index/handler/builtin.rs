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

fn builtin_is_current<'a>(
    file: &FileMeta,
    context: &PlanContext<'_>,
    kind: IndexKind,
    targets: impl Iterator<Item = (&'a IndexTarget, u8)>,
) -> bool {
    if !file
        .is_index_consistent_with_region(&context.metadata.column_metadatas, context.index_options)
    {
        return false;
    }
    if context.build_type != crate::sst::index::IndexBuildType::Manual {
        return true;
    }
    let mut paths = targets
        .filter_map(|(target, version)| match target {
            IndexTarget::JsonPath(path) => Some((path, version)),
            IndexTarget::ColumnId(_) => None,
        })
        .peekable();
    if paths.peek().is_none() {
        return true;
    }
    let recorded: std::collections::HashMap<_, _> = file
        .indexes
        .iter()
        .filter_map(|index| Some(((index.column_id, index.json_path.as_deref()?), index)))
        .collect();
    paths.all(|(path, version)| {
        recorded
            .get(&(path.column_id(), path.path()))
            .is_some_and(|index| {
                index.created_indexes.contains(&kind.index_type())
                    && index.fulltext_bloom_version >= version
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::region::options::IndexOptions;
    use crate::sst::index::IndexBuildType;
    use crate::test_util::sst_util;

    #[test]
    fn test_invalid_json_hints_preserve_other_column_indexes() {
        use arrow_schema::extension::{
            EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
        };
        use datatypes::data_type::ConcreteDataType;
        use datatypes::extension::json::Json2ExtensionType;
        use datatypes::types::json_type::{JsonNativeType, JsonObjectType};

        let mut metadata = sst_util::sst_region_metadata();
        let field = &mut metadata.column_metadatas[2].column_schema;
        field.data_type = ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new()));
        field.mut_metadata().insert(
            EXTENSION_TYPE_NAME_KEY.into(),
            Json2ExtensionType::NAME.into(),
        );
        field
            .mut_metadata()
            .insert(EXTENSION_TYPE_METADATA_KEY.into(), "invalid json".into());
        let metadata = Arc::new(metadata);
        let options = IndexOptions::default();
        for build_type in [IndexBuildType::Flush, IndexBuildType::Compact] {
            let context = PlanContext {
                metadata: &metadata,
                index_options: &options,
                build_type,
                rebuild_source: None,
                existing_indexes: &[],
            };
            for (handler, column_id) in [
                (
                    BuiltinIndexHandler::Inverted(InvertedIndexConfig::default()),
                    0,
                ),
                (BuiltinIndexHandler::Bloom(BloomFilterConfig::default()), 1),
            ] {
                let plan = handler.plan(&context).unwrap().unwrap();
                assert_eq!(plan.entries().len(), 1);
                assert_eq!(plan.entries()[0].column_id, column_id);
            }
        }
    }

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
                rebuild_source: None,
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
                rebuild_source: None,
                existing_indexes: &[],
            };
            assert!(handler.plan(&context).is_err());
        }
    }
}
