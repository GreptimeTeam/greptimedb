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
use common_meta::error::{ExternalSnafu, Result as MetaResult};
use common_meta::peer::{Peer, PeerAllocator};
use snafu::{ResultExt, ensure};

use crate::discovery::utils::accept_ingest_workload;
use crate::error::{Result, TooManyPartitionsSnafu};
use crate::metasrv::{SelectorContext, SelectorRef};
use crate::selector::SelectorOptions;

pub struct MetasrvPeerAllocator {
    ctx: SelectorContext,
    selector: SelectorRef,
    max_items: Option<u32>,
}

impl MetasrvPeerAllocator {
    /// Creates a new [`MetasrvPeerAllocator`] with the given [`SelectorContext`] and [`SelectorRef`].
    pub fn new(ctx: SelectorContext, selector: SelectorRef) -> Self {
        Self {
            ctx,
            selector,
            max_items: None,
        }
    }

    pub fn with_max_items(self, max_items: u32) -> Self {
        Self {
            ctx: self.ctx,
            selector: self.selector,
            max_items: Some(max_items),
        }
    }

    /// Allocates a specified number (by `regions`) of [`Peer`] instances based on the number of
    /// regions. The returned peers will have the same length as the number of regions.
    ///
    /// This method is mainly a wrapper around the [`SelectorRef`]::`select` method. There is
    /// no guarantee that how the returned peers are used, like whether they are from the same
    /// table or not. So this method isn't idempotent.
    async fn alloc(&self, min_required_items: usize) -> Result<Vec<Peer>> {
        if let Some(max_items) = self.max_items {
            ensure!(
                min_required_items <= max_items as usize,
                TooManyPartitionsSnafu
            );
        }

        self.selector
            .select(
                &self.ctx,
                SelectorOptions {
                    min_required_items,
                    allow_duplication: true,
                    exclude_peer_ids: HashSet::new(),
                    workload_filter: Some(accept_ingest_workload),
                },
            )
            .await
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
