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

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use crate::error;
use crate::memtable::bulk::row_group_reader::{
    MemtableRowGroupReader, MemtableRowGroupReaderBuilder,
};
use crate::read::Batch;
use crate::sst::parquet::file_range::RangeBase;
use crate::sst::parquet::format::ReadFormat;

pub(crate) type BulkIterContextRef = Arc<BulkIterContext>;

pub(crate) struct BulkIterContext {
    pub(crate) base: RangeBase,
}

impl BulkIterContext {
    pub(crate) fn read_format(&self) -> &ReadFormat {
        &self.base.read_format
    }
}

/// Iterator for reading data inside a bulk part.
pub struct BulkPartIter {
    context: BulkIterContextRef,
    row_groups_to_read: VecDeque<usize>,
    current_reader: Option<MemtableRowGroupReader>,
    builder: MemtableRowGroupReaderBuilder,
}

impl BulkPartIter {
    /// Creates a new [BulkPartIter].
    pub(crate) fn try_new(
        context: BulkIterContextRef,
        mut row_groups_to_read: VecDeque<usize>,
        parquet_meta: Arc<ParquetMetaData>,
        data: Bytes,
    ) -> error::Result<Self> {
        let projection_mask = ProjectionMask::roots(
            parquet_meta.file_metadata().schema_descr(),
            context.read_format().projection_indices().iter().copied(),
        );

        let builder = MemtableRowGroupReaderBuilder::try_new(
            context.clone(),
            projection_mask,
            parquet_meta,
            data,
        )?;

        let init_reader = row_groups_to_read
            .pop_front()
            .map(|first_row_group| builder.build_row_group_reader(first_row_group, None))
            .transpose()?;
        Ok(Self {
            context,
            row_groups_to_read,
            current_reader: init_reader,
            builder,
        })
    }

    pub(crate) fn next_batch(&mut self) -> error::Result<Option<Batch>> {
        //todo(hl): maybe we also need to support lastrow mode here.
        if let Some(current) = &mut self.current_reader {
            if let Some(batch) = current.next_inner()? {
                if let Some(after_prune) = self.prune(batch)? {
                    return Ok(Some(after_prune));
                }
            }
        }

        // Previous row group finished, read next row group
        while let Some(next_row_group) = self.row_groups_to_read.pop_front() {
            let mut next_reader = self.builder.build_row_group_reader(next_row_group, None)?;

            if let Some(next_batch) = next_reader.next_inner()?
                && let Some(after_prune) = self.prune(next_batch)?
            {
                self.current_reader = Some(next_reader);
                return Ok(Some(after_prune));
            }
        }
        Ok(None)
    }

    /// Prunes batch according to filters.
    fn prune(&self, batch: Batch) -> error::Result<Option<Batch>> {
        //todo(hl): add metrics.

        // fast path
        if self.context.base.filters.is_empty() {
            return Ok(Some(batch));
        }

        let Some(batch_filtered) = self.context.base.precise_filter(batch)? else {
            // the entire batch is filtered out
            return Ok(None);
        };
        if !batch_filtered.is_empty() {
            Ok(Some(batch_filtered))
        } else {
            Ok(None)
        }
    }
}

impl Iterator for BulkPartIter {
    type Item = error::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}
