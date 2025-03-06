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

pub mod manager;

pub mod add_region_follower;
mod create;
mod test_util;

use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::key::TableMetadataManagerRef;

use crate::cluster::MetaPeerClientRef;
use crate::service::mailbox::MailboxRef;

#[derive(Clone)]
/// The context of add/remove region follower procedure.
pub struct Context {
    /// The table metadata manager.
    pub table_metadata_manager: TableMetadataManagerRef,
    /// The mailbox.
    pub mailbox: MailboxRef,
    /// The metasrv's address.
    pub server_addr: String,
    /// The cache invalidator.
    pub cache_invalidator: CacheInvalidatorRef,
    /// The meta peer client.
    pub meta_peer_client: MetaPeerClientRef,
}
