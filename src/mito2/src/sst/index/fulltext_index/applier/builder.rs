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

use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadata;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplier;
use crate::sst::index::puffin_manager::PuffinManagerFactory;

/// `FulltextIndexApplierBuilder` is a builder for `FulltextIndexApplier`.
pub struct FulltextIndexApplierBuilder<'a> {
    region_dir: String,
    store: ObjectStore,
    puffin_manager_factory: PuffinManagerFactory,
    metadata: &'a RegionMetadata,
    queries: Vec<(ColumnId, String)>,
}

impl<'a> FulltextIndexApplierBuilder<'a> {
    /// Creates a new `FulltextIndexApplierBuilder`.
    pub fn new(
        region_dir: String,
        store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        metadata: &'a RegionMetadata,
    ) -> Self {
        Self {
            region_dir,
            store,
            puffin_manager_factory,
            metadata,
            queries: Vec::new(),
        }
    }

    /// Builds `SstIndexApplier` from the given expressions.
    pub fn build(mut self, exprs: &[Expr]) -> Result<Option<FulltextIndexApplier>> {
        for expr in exprs {
            let Expr::ScalarFunction(f) = expr else {
                continue;
            };
            if f.name() != "matches" {
                continue;
            }
            if f.args.len() != 2 {
                continue;
            }

            let Expr::Column(c) = &f.args[0] else {
                continue;
            };
            let Some(column) = self.metadata.column_by_name(&c.name) else {
                continue;
            };

            let Expr::Literal(ScalarValue::Utf8(Some(query))) = &f.args[1] else {
                continue;
            };

            self.queries.push((column.column_id, query.to_string()));
        }

        Ok((!self.queries.is_empty()).then(|| {
            FulltextIndexApplier::new(
                self.region_dir,
                self.store,
                self.queries,
                self.puffin_manager_factory,
            )
        }))
    }
}
