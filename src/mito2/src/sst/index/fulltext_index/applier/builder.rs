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

use std::collections::HashMap;

use datafusion_common::ScalarValue;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datatypes::prelude::ConcreteDataType;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use smallvec::SmallVec;
use store_api::metadata::RegionMetadata;
use store_api::storage::RegionId;

use crate::cache::file_cache::FileCacheRef;
use crate::cache::index::bloom_filter_index::BloomFilterIndexCacheRef;
use crate::error::Result;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplier;
use crate::sst::index::fulltext_index::{
    FulltextPredicate, MatchesPredicate, MatchesTermPredicate,
};
use crate::sst::index::puffin_manager::PuffinManagerFactory;

/// `FulltextIndexApplierBuilder` is a builder for `FulltextIndexApplier`.
pub struct FulltextIndexApplierBuilder<'a> {
    region_dir: String,
    region_id: RegionId,
    store: ObjectStore,
    puffin_manager_factory: PuffinManagerFactory,
    metadata: &'a RegionMetadata,
    file_cache: Option<FileCacheRef>,
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    bloom_filter_cache: Option<BloomFilterIndexCacheRef>,
}

impl<'a> FulltextIndexApplierBuilder<'a> {
    /// Creates a new `FulltextIndexApplierBuilder`.
    pub fn new(
        region_dir: String,
        region_id: RegionId,
        store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        metadata: &'a RegionMetadata,
    ) -> Self {
        Self {
            region_dir,
            region_id,
            store,
            puffin_manager_factory,
            metadata,
            file_cache: None,
            puffin_metadata_cache: None,
            bloom_filter_cache: None,
        }
    }

    /// Sets the file cache to be used by the `FulltextIndexApplier`.
    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    /// Sets the puffin metadata cache to be used by the `FulltextIndexApplier`.
    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    pub fn with_bloom_filter_cache(
        mut self,
        bloom_filter_cache: Option<BloomFilterIndexCacheRef>,
    ) -> Self {
        self.bloom_filter_cache = bloom_filter_cache;
        self
    }

    /// Builds `SstIndexApplier` from the given expressions.
    pub fn build(self, exprs: &[Expr]) -> Result<Option<FulltextIndexApplier>> {
        let mut predicates: HashMap<u32, SmallVec<[FulltextPredicate; 1]>> = HashMap::new();
        for expr in exprs {
            if let Some(p) = FulltextPredicate::from_expr(self.metadata, expr) {
                let column_id = p.column_id();
                predicates.entry(column_id).or_default().push(p);
            }
        }

        Ok((!predicates.is_empty()).then(|| {
            FulltextIndexApplier::new(
                self.region_dir,
                self.region_id,
                self.store,
                predicates,
                self.puffin_manager_factory,
            )
            .with_file_cache(self.file_cache)
            .with_puffin_metadata_cache(self.puffin_metadata_cache)
            .with_bloom_filter_cache(self.bloom_filter_cache)
        }))
    }
}

impl FulltextPredicate {
    fn from_expr(metadata: &RegionMetadata, expr: &Expr) -> Option<Self> {
        let Expr::ScalarFunction(f) = expr else {
            return None;
        };

        match f.name() {
            "matches" => Self::from_matches_func(metadata, f),
            "matches_term" => Self::from_matches_term_func(metadata, f),
            _ => None,
        }
    }

    fn from_matches_func(metadata: &RegionMetadata, f: &ScalarFunction) -> Option<Self> {
        if f.args.len() != 2 {
            return None;
        }

        let Expr::Column(c) = &f.args[0] else {
            return None;
        };

        let column = metadata.column_by_name(&c.name)?;
        if column.column_schema.data_type != ConcreteDataType::string_datatype() {
            return None;
        }

        let Expr::Literal(ScalarValue::Utf8(Some(query))) = &f.args[1] else {
            return None;
        };

        Some(Self::Matches(MatchesPredicate {
            column_id: column.column_id,
            query: query.to_string(),
        }))
    }

    fn from_matches_term_func(metadata: &RegionMetadata, f: &ScalarFunction) -> Option<Self> {
        if f.args.len() != 2 {
            return None;
        }

        let mut lowered = false;
        let column;
        match &f.args[0] {
            Expr::Column(c) => {
                column = c;
            }
            Expr::ScalarFunction(f) => {
                let lower_arg = Self::extract_lower_arg(f)?;
                lowered = true;
                if let Expr::Column(c) = lower_arg {
                    column = c;
                } else {
                    return None;
                }
            }
            _ => return None,
        }

        let column = metadata.column_by_name(&column.name)?;
        if column.column_schema.data_type != ConcreteDataType::string_datatype() {
            return None;
        }

        if lowered {
            return None;
        }

        let Expr::Literal(ScalarValue::Utf8(Some(term))) = &f.args[1] else {
            return None;
        };

        Some(Self::MatchesTerm(MatchesTermPredicate {
            column_id: column.column_id,
            col_lowered: lowered,
            term: term.to_string(),
        }))
    }

    fn extract_lower_arg(lower_func: &ScalarFunction) -> Option<&Expr> {
        if lower_func.args.len() != 1 {
            return None;
        }

        if lower_func.name() != "lower" {
            return None;
        }

        if lower_func.args.len() != 1 {
            return None;
        }

        Some(&lower_func.args[0])
    }
}

#[cfg(test)]
mod tests {
    // TODO(zhongzc): add tests
}
