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

use std::cmp;

use async_trait::async_trait;
use store_api::storage::{
    GetRequest, GetResponse, ReadContext, ScanRequest, ScanResponse, SchemaRef, SequenceNumber,
    Snapshot,
};

use crate::chunk::{ChunkReaderBuilder, ChunkReaderImpl};
use crate::error::{Error, Result};
use crate::sst::AccessLayerRef;
use crate::version::VersionRef;

/// [Snapshot] implementation.
pub struct SnapshotImpl {
    version: VersionRef,
    /// Max sequence number (inclusive) visible to user.
    visible_sequence: SequenceNumber,
    sst_layer: AccessLayerRef,
}

#[async_trait]
impl Snapshot for SnapshotImpl {
    type Error = Error;
    type Reader = ChunkReaderImpl;

    fn schema(&self) -> &SchemaRef {
        self.version.user_schema()
    }

    async fn scan(
        &self,
        ctx: &ReadContext,
        request: ScanRequest,
    ) -> Result<ScanResponse<ChunkReaderImpl>> {
        let visible_sequence = self.sequence_to_read(request.sequence);
        let memtable_version = self.version.memtables();

        let mutables = memtable_version.mutable_memtable();
        let immutables = memtable_version.immutable_memtables();

        let mut builder = ChunkReaderBuilder::new(
            self.version.metadata().id(),
            self.version.schema().clone(),
            self.sst_layer.clone(),
        )
        .reserve_num_memtables(memtable_version.num_memtables())
        .projection(request.projection)
        .filters(request.filters)
        .batch_size(ctx.batch_size)
        .output_ordering(request.output_ordering)
        .visible_sequence(visible_sequence)
        .pick_memtables(mutables.clone())
        .use_chain_reader(true);

        for memtable in immutables {
            builder = builder.pick_memtables(memtable.clone());
        }

        let reader = builder.pick_all_ssts(self.version.ssts())?.build().await?;

        Ok(ScanResponse { reader })
    }

    async fn get(&self, _ctx: &ReadContext, _request: GetRequest) -> Result<GetResponse> {
        unimplemented!()
    }
}

impl SnapshotImpl {
    pub fn new(
        version: VersionRef,
        visible_sequence: SequenceNumber,
        sst_layer: AccessLayerRef,
    ) -> SnapshotImpl {
        SnapshotImpl {
            version,
            visible_sequence,
            sst_layer,
        }
    }

    #[inline]
    fn sequence_to_read(&self, request_sequence: Option<SequenceNumber>) -> SequenceNumber {
        request_sequence
            .map(|s| cmp::min(s, self.visible_sequence))
            .unwrap_or(self.visible_sequence)
    }
}
