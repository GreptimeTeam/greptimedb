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

use std::collections::HashSet;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_meta::ddl::table_meta::PeerAllocator;
use common_meta::error::{ExternalSnafu, Result as MetaResult};
use common_meta::peer::Peer;
use snafu::{ensure, ResultExt};
use store_api::storage::MAX_REGION_SEQ;

use crate::error::{self, Result, TooManyPartitionsSnafu};
use crate::metasrv::{SelectTarget, SelectorContext, SelectorRef};
use crate::selector::SelectorOptions;

pub struct MetasrvPeerAllocator {
    ctx: SelectorContext,
    selector: SelectorRef,
}

impl MetasrvPeerAllocator {
    /// Creates a new [`MetasrvPeerAllocator`] with the given [`SelectorContext`] and [`SelectorRef`].
    pub fn new(ctx: SelectorContext, selector: SelectorRef) -> Self {
        Self { ctx, selector }
    }

    /// Allocates a specified number (by `regions`) of [`Peer`] instances based on the number of
    /// regions. The returned peers will have the same length as the number of regions.
    ///
    /// This method is mainly a wrapper around the [`SelectorRef`]::`select` method. There is
    /// no guarantee that how the returned peers are used, like whether they are from the same
    /// table or not. So this method isn't idempotent.
    async fn alloc(&self, regions: usize) -> Result<Vec<Peer>> {
        ensure!(regions <= MAX_REGION_SEQ as usize, TooManyPartitionsSnafu);

        let mut peers = self
            .selector
            .select(
                &self.ctx,
                SelectorOptions {
                    min_required_items: regions,
                    allow_duplication: true,
                    exclude_peer_ids: HashSet::new(),
                },
            )
            .await?;

        ensure!(
            peers.len() >= regions,
            error::NoEnoughAvailableNodeSnafu {
                required: regions,
                available: peers.len(),
                select_target: SelectTarget::Datanode
            }
        );

        peers.truncate(regions);

        Ok(peers)
    }
}

#[async_trait]
impl PeerAllocator for MetasrvPeerAllocator {
    async fn alloc(&self, regions: usize) -> MetaResult<Vec<Peer>> {
        self.alloc(regions)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }
}
