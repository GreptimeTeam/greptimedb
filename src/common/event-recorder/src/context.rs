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

use api::v1::meta::ProcedureEventContext as PbProcedureEventContext;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumString};

/// Event metadata supplied when a procedure is submitted.
///
/// Protocol is deliberately absent. The trusted submission adapter derives it
/// from the typed query channel before serializing the protobuf request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcedureEventInput {
    pub reason: TriggerReason,
    pub extensions: HashMap<String, String>,
}

impl ProcedureEventInput {
    pub fn new(reason: TriggerReason) -> Self {
        Self {
            reason,
            extensions: Default::default(),
        }
    }
}

impl From<&ProcedureEventInput> for PbProcedureEventContext {
    fn from(input: &ProcedureEventInput) -> Self {
        Self {
            reason: input.reason.as_ref().to_string(),
            protocol: String::new(),
            extensions: input.extensions.clone(),
        }
    }
}

impl From<PbProcedureEventContext> for ProcedureEventInput {
    fn from(context: PbProcedureEventContext) -> Self {
        Self {
            reason: TriggerReason::from_extension(&context.reason),
            extensions: context.extensions,
        }
    }
}

/// Stable context recorded for a procedure event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentEventContext {
    pub reason: TriggerReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extensions: serde_json::Map<String, serde_json::Value>,
}

impl PersistentEventContext {
    /// Creates an event context with no additional extensions.
    pub fn new(reason: TriggerReason) -> Self {
        Self {
            reason,
            protocol: None,
            extensions: Default::default(),
        }
    }

    /// Adds the protocol that originated the operation.
    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = Some(protocol.into());
        self
    }
}

impl From<(ProcedureEventInput, Option<String>)> for PersistentEventContext {
    fn from((input, protocol): (ProcedureEventInput, Option<String>)) -> Self {
        Self {
            reason: input.reason,
            protocol,
            extensions: input
                .extensions
                .into_iter()
                .map(|(key, value)| (key, serde_json::Value::String(value)))
                .collect(),
        }
    }
}

impl Default for PersistentEventContext {
    fn default() -> Self {
        Self::new(TriggerReason::default())
    }
}

impl From<PbProcedureEventContext> for PersistentEventContext {
    fn from(context: PbProcedureEventContext) -> Self {
        Self {
            reason: TriggerReason::from_extension(&context.reason),
            protocol: (!context.protocol.is_empty()).then_some(context.protocol),
            extensions: context
                .extensions
                .into_iter()
                .map(|(key, value)| (key, serde_json::Value::String(value)))
                .collect(),
        }
    }
}

/// Stable classification of a procedure trigger.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, AsRefStr, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TriggerReason {
    Manual,
    AutoCreate,
    AutoAlter,
    AutoRepartition,
    AutoRebalance,
    RegionFailover,
    ScheduledGc,
    #[default]
    #[serde(other)]
    Unknown,
}

impl TriggerReason {
    pub fn from_extension(value: &str) -> Self {
        value.parse().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_event_context_serialization() {
        let context = PersistentEventContext::new(TriggerReason::Manual).with_protocol("mysql");

        assert_eq!(
            json!({
                "reason": "manual",
                "protocol": "mysql",
            }),
            serde_json::to_value(context).unwrap()
        );
        assert_eq!(
            json!({ "reason": "manual" }),
            serde_json::to_value(PersistentEventContext::new(TriggerReason::Manual)).unwrap()
        );
    }

    #[test]
    fn test_event_context_from_protobuf() {
        let protobuf = PbProcedureEventContext {
            reason: "auto_create".to_string(),
            protocol: "postgres".to_string(),
            extensions: HashMap::from([
                ("source".to_string(), "sql".to_string()),
                ("tenant".to_string(), "a".to_string()),
            ]),
        };
        assert_eq!(
            PersistentEventContext::from(protobuf),
            PersistentEventContext {
                reason: TriggerReason::AutoCreate,
                protocol: Some("postgres".to_string()),
                extensions: serde_json::Map::from_iter([
                    ("source".to_string(), json!("sql")),
                    ("tenant".to_string(), json!("a")),
                ]),
            }
        );
    }

    #[test]
    fn test_event_input_protobuf_has_no_protocol() {
        let input = ProcedureEventInput {
            reason: TriggerReason::AutoCreate,
            extensions: HashMap::from([("source".to_string(), "sql".to_string())]),
        };

        assert_eq!(
            PbProcedureEventContext::from(&input),
            PbProcedureEventContext {
                reason: "auto_create".to_string(),
                protocol: String::new(),
                extensions: input.extensions.clone(),
            }
        );

        assert_eq!(
            ProcedureEventInput::from(PbProcedureEventContext {
                reason: "auto_create".to_string(),
                protocol: "untrusted".to_string(),
                extensions: input.extensions.clone(),
            }),
            input
        );
    }

    #[test]
    fn test_trigger_reason_deserializes_unknown_value() {
        let reason: TriggerReason = serde_json::from_str("\"future_reason\"").unwrap();
        assert_eq!(TriggerReason::Unknown, reason);
    }
}
