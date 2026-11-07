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
    ) -> std::result::Result<Option<Arc<dyn PlannedIndex>>, common_error::ext::BoxedError> {
        let existing = context.existing_indexes.iter().any(|column| {
            column
                .created_indexes
                .contains(&crate::sst::file::IndexType::Hll)
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
    ) -> std::result::Result<Option<Box<dyn IndexCreator>>, common_error::ext::BoxedError> {
        Ok(None)
    }

    fn is_current(&self, _file: &FileMeta, _context: &CurrencyContext<'_>) -> bool {
        false
    }
}

#[test]
fn test_rebuild_selection_uses_registered_currency_and_empty_maintenance_plans() {
    let request = CreateRequestBuilder::new().build();
    let mut metadata = RegionMetadataBuilder::new(RegionId::new(1, 1));
    metadata.primary_key(request.primary_key);
    for mut column in request.column_metadatas {
        column.column_schema.set_inverted_index(false);
        column.column_schema.unset_skipping_options().unwrap();
        metadata.push_column_metadata(column);
    }
    let snapshot = IndexBuildSnapshot {
        metadata: Arc::new(metadata.build().unwrap()),
        index_options: IndexOptions::default(),
    };
    let builtin = IndexRegistry::new(Default::default(), Default::default(), Default::default());
    let mut registered = builtin.clone();
    registered.register(Arc::new(MaintenanceHandler)).unwrap();
    let empty = FileMeta::default();
    let mut existing = empty.clone();
    existing.indexes.push(ColumnIndexMetadata {
        column_id: 0,
        created_indexes: smallvec::smallvec![crate::sst::file::IndexType::Hll],
        ..Default::default()
    });
    for reason in [IndexBuildType::Manual, IndexBuildType::SchemaChange] {
        assert!(
            !index_needs_rebuild(&builtin, &snapshot, &existing, reason.clone()),
            "unknown extension must not cause an OSS rebuild loop"
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
