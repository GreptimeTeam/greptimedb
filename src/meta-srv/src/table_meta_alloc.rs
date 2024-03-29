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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_meta::ddl::table_meta::PeerAllocator;
use common_meta::ddl::TableMetadataAllocatorContext;
use common_meta::error::{ExternalSnafu, Result as MetaResult};
use common_meta::peer::Peer;
use snafu::{ensure, ResultExt};
use store_api::storage::MAX_REGION_SEQ;

use crate::error::{self, Result, TooManyPartitionsSnafu};
use crate::metasrv::{SelectorContext, SelectorRef};
use crate::selector::SelectorOptions;

pub struct MetasrvPeerAllocator {
    ctx: SelectorContext,
    selector: SelectorRef,
}

impl MetasrvPeerAllocator {
    pub fn new(ctx: SelectorContext, selector: SelectorRef) -> Self {
        Self { ctx, selector }
    }

    async fn alloc(
        &self,
        ctx: &TableMetadataAllocatorContext,
        regions: usize,
    ) -> Result<Vec<Peer>> {
        ensure!(regions <= MAX_REGION_SEQ as usize, TooManyPartitionsSnafu);

        let mut peers = self
            .selector
            .select(
                ctx.cluster_id,
                &self.ctx,
                SelectorOptions {
                    min_required_items: regions,
                    allow_duplication: true,
                },
            )
            .await?;

        ensure!(
            peers.len() >= regions,
            error::NoEnoughAvailableDatanodeSnafu {
                required: regions,
                available: peers.len(),
            }
        );

        peers.truncate(regions);

        Ok(peers)
    }
}

#[async_trait]
impl PeerAllocator for MetasrvPeerAllocator {
    async fn alloc(
        &self,
        ctx: &TableMetadataAllocatorContext,
        regions: usize,
    ) -> MetaResult<Vec<Peer>> {
        self.alloc(ctx, regions)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }
}
