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

use std::sync::Arc;

use crate::error::Result;
use crate::metadata::{TableId, TableType};
use crate::thin_table::ThinTable;

pub mod adapter;
mod metrics;
pub mod numbers;
pub mod scan;

pub type TableRef = Arc<ThinTable>;

#[async_trait::async_trait]
pub trait TableIdProvider {
    async fn next_table_id(&self) -> Result<TableId>;
}

pub type TableIdProviderRef = Arc<dyn TableIdProvider + Send + Sync>;
