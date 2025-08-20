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
use std::time::Duration;

use api::v1::RowInsertRequests;
use humantime::format_duration;
use store_api::mito_engine_options::{APPEND_MODE_KEY, TTL_KEY};

use crate::error::Result;

/// Context holds the catalog and schema information.
pub struct Context<'a> {
    /// The catalog name.
    pub catalog: &'a str,
    /// The schema name.
    pub schema: &'a str,
}

/// Options for insert operations.
#[derive(Debug, Clone, Copy)]
pub struct InsertOptions {
    /// Time-to-live for the inserted data.
    pub ttl: Duration,
    /// Whether to use append mode for the insert.
    pub append_mode: bool,
}

impl InsertOptions {
    /// Converts the insert options to a list of key-value string hints.
    pub fn to_hints(&self) -> Vec<(&str, String)> {
        vec![
            (TTL_KEY, format_duration(self.ttl).to_string()),
            (APPEND_MODE_KEY, self.append_mode.to_string()),
        ]
    }
}

pub type InserterRef = Arc<dyn Inserter>;

/// [`Inserter`] allows different components to share a unified mechanism for inserting data.
///
/// An implementation may perform the insert locally (e.g., via a direct procedure call) or
/// delegate/forward it to another node for processing (e.g., MetaSrv forwarding to an
/// available Frontend).
#[async_trait::async_trait]
pub trait Inserter: Send + Sync {
    async fn insert_rows(
        &self,
        context: &Context<'_>,
        requests: RowInsertRequests,
        options: Option<&InsertOptions>,
    ) -> Result<()>;
}
