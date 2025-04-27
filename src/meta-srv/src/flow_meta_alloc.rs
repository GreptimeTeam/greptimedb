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

use common_error::ext::BoxedError;
use common_meta::ddl::flow_meta::PartitionPeerAllocator;
use common_meta::peer::Peer;
use snafu::ResultExt;

use crate::metasrv::{SelectorContext, SelectorRef};
use crate::selector::SelectorOptions;

pub struct FlowPeerAllocator {
    ctx: SelectorContext,
    selector: SelectorRef,
}

impl FlowPeerAllocator {
    pub fn new(ctx: SelectorContext, selector: SelectorRef) -> Self {
        Self { ctx, selector }
    }
}

#[async_trait::async_trait]
impl PartitionPeerAllocator for FlowPeerAllocator {
    async fn alloc(&self, partitions: usize) -> common_meta::error::Result<Vec<Peer>> {
        self.selector
            .select(
                &self.ctx,
                SelectorOptions {
                    min_required_items: partitions,
                    allow_duplication: true,
                    exclude_peer_ids: HashSet::new(),
                },
            )
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)
    }
}
