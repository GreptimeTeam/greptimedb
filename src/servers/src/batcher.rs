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

//! Logical-table and ordinary-table batching implementations.

mod flow_notifier;
mod flow_sender;
pub mod logical_table;
pub mod table;

#[cfg(test)]
mod test_util;

use serde::{Deserialize, Serialize};

/// Controls whether batching waits for storage before replying to the client.
const PENDING_ROWS_BATCH_SYNC_ENV: &str = "PENDING_ROWS_BATCH_SYNC";

/// Returns whether pending-row batch submissions wait for the flush result
/// before replying to the client (synchronous mode), controlled by the
/// `PENDING_ROWS_BATCH_SYNC` environment variable and defaulting to `true`.
///
/// Callers that reason about how long a remote write request may block (e.g.
/// the frontend HTTP timeout fallback) must consult this instead of
/// duplicating the env lookup.
pub fn pending_rows_batch_sync_enabled() -> bool {
    std::env::var(PENDING_ROWS_BATCH_SYNC_ENV)
        .ok()
        .as_deref()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(true)
}

/// Ingestion protocols that can opt into pending-row batching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchingProtocol {
    Prom,
    Influxdb,
    Opentsdb,
    Otlp,
    Logs,
    Loki,
    Splunk,
    Elasticsearch,
    HttpSql,
    Mysql,
    Postgres,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_names_reject_unknown_values() {
        assert_eq!(
            serde_json::from_str::<BatchingProtocol>("\"prom\"").unwrap(),
            BatchingProtocol::Prom
        );
        for name in ["sql", "unknown"] {
            assert!(serde_json::from_str::<BatchingProtocol>(&format!("\"{name}\"")).is_err());
        }
        assert_eq!(
            serde_json::from_str::<BatchingProtocol>("\"http_sql\"").unwrap(),
            BatchingProtocol::HttpSql
        );
    }
}
