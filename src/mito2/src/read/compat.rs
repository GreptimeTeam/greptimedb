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

//! Utilities to adapt readers with different schema.

use store_api::metadata::{RegionMetadata, RegionMetadataRef};

use crate::error::Result;
use crate::read::projection::ProjectionMapper;
use crate::read::{Batch, BatchReader};

/// Reader to adapt schema of underlying reader to expected schema.
pub struct CompatReader<R> {
    /// Underlying reader.
    reader: R,
    /// Optional primary key adapter.
    compat_pk: Option<CompatPrimaryKey>,
    /// Optional fields adapter.
    compat_fields: Option<CompatFields>,
}

impl<R> CompatReader<R> {
    /// Creates a new compat reader.
    /// - `mapper` is built from the metadata users expect to see.
    /// - `reader_meta` is the metadata of the input reader.
    /// - `reader` is the input reader.
    pub fn new(
        mapper: &ProjectionMapper,
        reader_meta: RegionMetadataRef,
        reader: R,
    ) -> Result<CompatReader<R>> {
        let compat_pk = may_compat_primary_key(mapper.metadata(), &reader_meta)?;
        let compat_fields = may_compat_fields(mapper, &reader_meta)?;

        Ok(CompatReader {
            reader,
            compat_pk,
            compat_fields,
        })
    }
}

#[async_trait::async_trait]
impl<R: BatchReader> BatchReader for CompatReader<R> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let Some(batch) = self.reader.next_batch().await? else {
            return Ok(None);
        };

        // if let Some(compat_pk) = &self.compat_pk {
        //     compat_pk
        // }

        todo!()
    }
}

/// Returns true if `left` and `right` have same columns to read.
///
/// It only consider column ids.
pub(crate) fn has_same_columns(left: &RegionMetadata, right: &RegionMetadata) -> bool {
    if left.column_metadatas.len() != right.column_metadatas.len() {
        return false;
    }

    for (left_col, right_col) in left.column_metadatas.iter().zip(&right.column_metadatas) {
        if left_col.column_id != right_col.column_id {
            return false;
        }
        debug_assert_eq!(
            left_col.column_schema.data_type,
            right_col.column_schema.data_type
        );
        debug_assert_eq!(left_col.semantic_type, right_col.semantic_type);
    }

    true
}

fn may_compat_primary_key(
    expect: &RegionMetadata,
    actual: &RegionMetadata,
) -> Result<Option<CompatPrimaryKey>> {
    unimplemented!()
}

fn may_compat_fields(
    mapper: &ProjectionMapper,
    actual: &RegionMetadata,
) -> Result<Option<CompatFields>> {
    unimplemented!()
}

/// Helper to make primary key compatible.
struct CompatPrimaryKey {
    //
}

/// Helper to make fields compatible.
struct CompatFields {
    //
}
