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
use std::sync::Arc;

use store_api::storage::RegionNumber;

use crate::error::Result;

pub type WalOptionsAllocatorRef = Arc<dyn WalOptionsAllocator>;

#[async_trait::async_trait]
pub trait WalOptionsAllocator: Send + Sync {
    async fn allocate(
        &self,
        region_numbers: &[RegionNumber],
        skip_wal: bool,
    ) -> Result<HashMap<RegionNumber, String>>;
}
