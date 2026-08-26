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

use std::time::Duration;

use etcd_client::{Client, EventType, WatchOptions, WatchStream, Watcher};
use serde::Deserialize;
use tokio::time::timeout;
use uuid::Uuid;

const ETCD_ENDPOINT: &str = "127.0.0.1:2379";
const PROCEDURE_STORE_PREFIX: &str = "/__procedure__/procedure/";
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
struct StoredProcedureMessage {
    type_name: String,
    #[serde(default)]
    parent_id: Option<serde_json::Value>,
}

/// Watches the old stage for a persisted procedure and clones its exact stored
/// message under a fresh ID so the current stage must recover it.
pub(crate) struct OldProcedureSnapshot {
    client: Client,
    watcher: Watcher,
    stream: WatchStream,
    type_name: String,
}

impl OldProcedureSnapshot {
    /// Starts watching before the case setup submits the target procedure.
    pub(crate) async fn start(type_name: &str) -> Result<Self, String> {
        let mut client = Client::connect([ETCD_ENDPOINT], None)
            .await
            .map_err(|error| format!("failed to connect to compatibility etcd: {error}"))?;
        let (watcher, stream) = client
            .watch(
                PROCEDURE_STORE_PREFIX,
                Some(WatchOptions::new().with_prefix()),
            )
            .await
            .map_err(|error| format!("failed to watch persisted procedures: {error}"))?;

        Ok(Self {
            client,
            watcher,
            stream,
            type_name: type_name.to_string(),
        })
    }

    /// Clones the first matching old-binary procedure state observed during setup.
    pub(crate) async fn clone_captured(mut self) -> Result<Uuid, String> {
        let type_name = self.type_name.clone();
        let (key, value) = timeout(CAPTURE_TIMEOUT, async {
            loop {
                let response = self
                    .stream
                    .message()
                    .await
                    .map_err(|error| format!("failed to read procedure watch: {error}"))?
                    .ok_or_else(|| "persisted-procedure watch ended unexpectedly".to_string())?;

                for event in response.events() {
                    if event.event_type() != EventType::Put {
                        continue;
                    }
                    let Some(kv) = event.kv() else {
                        continue;
                    };
                    if !kv.key().ends_with(b".step")
                        || !message_is_root_type(kv.value(), &type_name)
                    {
                        continue;
                    }
                    return Ok::<_, String>((kv.key().to_vec(), kv.value().to_vec()));
                }
            }
        })
        .await
        .map_err(|_| {
            format!(
                "timed out waiting for old procedure type '{}' to persist",
                self.type_name
            )
        })??;

        self.watcher
            .cancel()
            .await
            .map_err(|error| format!("failed to cancel procedure watch: {error}"))?;

        let procedure_id = Uuid::new_v4();
        let cloned_key = clone_step_key(&key, procedure_id)?;
        self.client
            .put(cloned_key, value, None)
            .await
            .map_err(|error| format!("failed to clone old procedure snapshot: {error}"))?;

        Ok(procedure_id)
    }
}

fn message_is_root_type(value: &[u8], expected: &str) -> bool {
    serde_json::from_slice::<StoredProcedureMessage>(value)
        .is_ok_and(|message| message.type_name == expected && message.parent_id.is_none())
}

fn clone_step_key(key: &[u8], procedure_id: Uuid) -> Result<String, String> {
    let key = std::str::from_utf8(key)
        .map_err(|error| format!("persisted-procedure key is not UTF-8: {error}"))?;
    let remainder = key
        .strip_prefix(PROCEDURE_STORE_PREFIX)
        .ok_or_else(|| format!("persisted-procedure key has an unexpected prefix: {key}"))?;
    let (original_id, step) = remainder
        .split_once('/')
        .ok_or_else(|| format!("persisted-procedure key has an unexpected shape: {key}"))?;
    Uuid::parse_str(original_id)
        .map_err(|error| format!("persisted-procedure key has an invalid ID: {error}"))?;
    if step.contains('/') || !step.ends_with(".step") {
        return Err(format!(
            "persisted-procedure key is not a step record: {key}"
        ));
    }

    Ok(format!("{PROCEDURE_STORE_PREFIX}{procedure_id}/{step}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_exact_procedure_type() {
        let message = br#"{"type_name":"metasrv-procedure::ReconcileTable","data":"{}"}"#;
        assert!(message_is_root_type(
            message,
            "metasrv-procedure::ReconcileTable"
        ));
        assert!(!message_is_root_type(
            message,
            "metasrv-procedure::ReconcileDatabase"
        ));
        assert!(!message_is_root_type(
            br#"{"type_name":"metasrv-procedure::ReconcileTable","parent_id":"9e1cf409-e582-4b69-8b64-91e0e61d32e4"}"#,
            "metasrv-procedure::ReconcileTable"
        ));
        assert!(!message_is_root_type(
            b"",
            "metasrv-procedure::ReconcileTable"
        ));
    }

    #[test]
    fn clones_only_procedure_step_keys() {
        let id = Uuid::parse_str("9e1cf409-e582-4b69-8b64-91e0e61d32e4").unwrap();
        assert_eq!(
            clone_step_key(
                b"/__procedure__/procedure/2b38cc53-4130-4ca1-8a90-c50f2a04cb98/0000000000.step",
                id,
            )
            .unwrap(),
            "/__procedure__/procedure/9e1cf409-e582-4b69-8b64-91e0e61d32e4/0000000000.step"
        );
        assert!(
            clone_step_key(
                b"/__procedure__/procedure/2b38cc53-4130-4ca1-8a90-c50f2a04cb98/0000000001.commit",
                id,
            )
            .is_err()
        );
    }
}
