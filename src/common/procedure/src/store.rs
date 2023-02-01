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
use std::fmt;

use common_telemetry::logging;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{Result, ToJsonSnafu};
use crate::store::state_store::StateStoreRef;
use crate::{BoxedProcedure, ProcedureId};

mod state_store;

/// Serialized data of a procedure.
#[derive(Debug, Serialize, Deserialize)]
struct ProcedureMessage {
    /// Type name of the procedure. The procedure framework also use the type name to
    /// find a loader to load the procedure.
    type_name: String,
    /// The data of the procedure.
    data: String,
    /// Parent procedure id.
    parent_id: Option<ProcedureId>,
}

/// Procedure storage layer.
#[derive(Clone)]
struct ProcedureStore(StateStoreRef);

impl ProcedureStore {
    /// Dump the `procedure` to the storage.
    async fn store_procedure(
        &self,
        procedure_id: ProcedureId,
        step: u32,
        procedure: &BoxedProcedure,
        parent_id: Option<ProcedureId>,
    ) -> Result<()> {
        let type_name = procedure.type_name();
        let data = procedure.dump()?;

        let message = ProcedureMessage {
            type_name: type_name.to_string(),
            data,
            parent_id,
        };
        let key = ParsedKey {
            procedure_id,
            step,
            is_committed: false,
        }
        .to_string();
        let value = serde_json::to_string(&message).context(ToJsonSnafu)?;

        self.0.put(&key, value.into_bytes()).await?;

        Ok(())
    }

    /// Write commit flag to the storage.
    async fn commit_procedure(&self, procedure_id: ProcedureId, step: u32) -> Result<()> {
        let key = ParsedKey {
            procedure_id,
            step,
            is_committed: true,
        }
        .to_string();
        self.0.put(&key, Vec::new()).await?;

        Ok(())
    }

    /// Load uncommitted procedures from the storage.
    async fn load_messages(&self) -> Result<HashMap<ProcedureId, ProcedureMessage>> {
        let mut messages = HashMap::new();
        // Tracks the last key-value pair of an uncommitted procedure.
        let mut procedure_key_value = None;

        // Scan all procedures.
        let mut key_values = self.0.list("/").await?;
        while let Some((key, value)) = key_values.try_next().await? {
            let Some(curr_key) = ParsedKey::parse_str(&key) else {
                logging::info!("Unknown key while loading procedures, key: {}", key);
                continue;
            };
            let Some((prev_key, prev_value)) = &procedure_key_value else {
                // This is new procedure. We stores the key-value pair and check next
                // key-value pair.
                procedure_key_value = Some((curr_key, value));
                continue;
            };
            if prev_key.procedure_id == curr_key.procedure_id && !curr_key.is_committed {
                // The same procedure, update its value.
                procedure_key_value = Some((curr_key, value));
            } else if prev_key.procedure_id == curr_key.procedure_id {
                // Skips the procedure if it is committed.
                procedure_key_value = None;
            } else {
                // A new procedure, now we can load previous procedure.
                let Some(message) = self.load_one_message(prev_key, prev_value) else {
                    // We don't abort the loading process and just ignore errors to ensure all remaining
                    // procedures are loaded.
                    continue;
                };
                messages.insert(prev_key.procedure_id, message);

                procedure_key_value = Some((curr_key, value));
            }
        }

        // Load last procedure.
        if let Some((last_key, last_value)) = &procedure_key_value {
            if let Some(message) = self.load_one_message(last_key, last_value) {
                messages.insert(last_key.procedure_id, message);
            }
        }

        Ok(messages)
    }

    fn load_one_message(&self, key: &ParsedKey, value: &[u8]) -> Option<ProcedureMessage> {
        serde_json::from_slice(value)
            .map_err(|e| {
                // `e` doesn't impl ErrorExt so we print it as normal error.
                logging::error!("Failed to parse value, key: {:?}, source: {}", key, e);
                e
            })
            .ok()
    }
}

/// Key to refer the procedure in the [ProcedureStore].
#[derive(Debug)]
struct ParsedKey {
    procedure_id: ProcedureId,
    step: u32,
    is_committed: bool,
}

impl fmt::Display for ParsedKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}.{}",
            self.procedure_id,
            self.step,
            if self.is_committed { "commit" } else { "step" }
        )
    }
}

impl ParsedKey {
    /// Try to parse the key from specific `input`.
    fn parse_str(input: &str) -> Option<ParsedKey> {
        let mut iter = input.rsplit('/');
        let name = iter.next()?;
        let id_str = iter.next()?;

        let procedure_id = ProcedureId::parse_str(id_str).ok()?;

        let mut parts = name.split('.');
        let step_str = parts.next()?;
        let suffix = parts.next()?;
        let is_committed = match suffix {
            "commit" => true,
            "step" => false,
            _ => return None,
        };
        let step = step_str.parse().ok()?;

        Some(ParsedKey {
            procedure_id,
            step,
            is_committed,
        })
    }
}
