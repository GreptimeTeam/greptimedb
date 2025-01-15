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
use store_api::storage::SequenceNumber;

use crate::error;
use crate::memtable::bulk::context::BulkIterContextRef;
use crate::memtable::bulk::row_group_reader::{
    MemtableRowGroupReader, MemtableRowGroupReaderBuilder,
};
use crate::read::Batch;

/// Iterator for reading data inside a bulk part.
pub struct BulkPartIter {
    row_groups_to_read: VecDeque<usize>,
    current_reader: Option<PruneReader>,
    builder: MemtableRowGroupReaderBuilder,
    sequence: Option<SequenceNumber>,
}

impl BulkPartIter {
    /// Creates a new [BulkPartIter].
    pub(crate) fn try_new(
        context: BulkIterContextRef,
        mut row_groups_to_read: VecDeque<usize>,
        parquet_meta: Arc<ParquetMetaData>,
        data: Bytes,
        sequence: Option<SequenceNumber>,
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
            .transpose()?
            .map(|r| PruneReader::new(context, r));
        Ok(Self {
            row_groups_to_read,
            current_reader: init_reader,
            builder,
            sequence,
        })
    }

    pub(crate) fn next_batch(&mut self) -> error::Result<Option<Batch>> {
        let Some(current) = &mut self.current_reader else {
            // All row group exhausted.
            return Ok(None);
        };

        if let Some(batch) = current.next_batch()? {
            return Ok(Some(batch));
        }

        // Previous row group exhausted, read next row group
        while let Some(next_row_group) = self.row_groups_to_read.pop_front() {
            current.reset(self.builder.build_row_group_reader(next_row_group, None)?);
            if let Some(next_batch) = current.next_batch()? {
                return Ok(Some(next_batch));
            }
        }
        Ok(None)
    }
}

impl Iterator for BulkPartIter {
    type Item = error::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

struct PruneReader {
    context: BulkIterContextRef,
    row_group_reader: MemtableRowGroupReader,
}

//todo(hl): maybe we also need to support lastrow mode here.
impl PruneReader {
    fn new(context: BulkIterContextRef, reader: MemtableRowGroupReader) -> Self {
        Self {
            context,
            row_group_reader: reader,
        }
    }

    /// Iterates current inner reader until exhausted.
    fn next_batch(&mut self) -> error::Result<Option<Batch>> {
        while let Some(b) = self.row_group_reader.next_inner()? {
            match self.prune(b)? {
                Some(b) => {
                    return Ok(Some(b));
                }
                None => {
                    continue;
                }
            }
        }
        Ok(None)
    }

    /// Prunes batch according to filters.
    fn prune(&mut self, batch: Batch) -> error::Result<Option<Batch>> {
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

    fn reset(&mut self, reader: MemtableRowGroupReader) {
        self.row_group_reader = reader;
    }
}
