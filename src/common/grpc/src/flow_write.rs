// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use common_error::ext::{ErrorExt, StackError};
use common_error::status_code::StatusCode;

use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::arrow::array::{Array, StringArray};
use datatypes::prelude::{ConcreteDataType, VectorRef};
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::StringVector;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use uuid::Uuid;

pub const COMMAND_KEY: &str = "flow_write_command";
pub const CHILD_KEY: &str = "flow_write_child";

/// The control command carried by a Flow write request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Open { nonce: String },
    Write { capability: String },
    Close { capability: String },
}

impl Command {
    /// Serializes this command as JSON.
    pub fn encode(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    /// Parses a serialized command.
    pub fn decode(value: &str) -> serde_json::Result<Self> {
        serde_json::from_str(value)
    }
}

/// A safe terminal application error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalError {
    pub code: u32,
    pub message: String,
}

impl std::fmt::Display for TerminalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TerminalError {}

impl StackError for TerminalError {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        buf.push(format!("{layer}: {self:?}"));
    }

    fn next(&self) -> Option<&dyn StackError> {
        None
    }
}

impl ErrorExt for TerminalError {
    fn status_code(&self) -> StatusCode {
        StatusCode::from_u32(self.code).unwrap_or(StatusCode::Internal)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A finite response for a tracked Flow write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reply {
    Opened {
        capability: String,
    },
    Closed,
    Complete {
        rows: usize,
        cost: usize,
        metrics: Option<common_recordbatch::adapter::RecordBatchMetrics>,
        error: Option<TerminalError>,
    },
}

fn envelope_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![ColumnSchema::new(
        "envelope",
        ConcreteDataType::string_datatype(),
        false,
    )]))
}

fn response_error(reason: impl Into<String>) -> common_recordbatch::error::Error {
    common_recordbatch::error::Error::CreateRecordBatches {
        reason: reason.into(),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

/// Encodes a reply in the finite one-string-column record-batch envelope.
pub fn reply_batches(reply: &Reply) -> common_recordbatch::error::Result<RecordBatches> {
    let value = serde_json::to_string(reply).map_err(|e| response_error(e.to_string()))?;
    let batch = RecordBatch::new(
        envelope_schema(),
        vec![Arc::new(StringVector::from(vec![value])) as VectorRef],
    )?;
    RecordBatches::try_new(batch.schema.clone(), vec![batch])
}

/// Decodes a reply from its finite one-string-column record-batch envelope.
pub fn decode_reply(batches: &RecordBatches) -> common_recordbatch::error::Result<Reply> {
    let batch = batches
        .iter()
        .next()
        .ok_or_else(|| response_error("missing reply batch"))?;
    if batches.iter().count() != 1 || batch.num_columns() != 1 || batch.num_rows() != 1 {
        return Err(response_error("reply must be exactly one string value"));
    }
    let values = batch.columns()[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| response_error("reply value is not a string"))?;
    if values.is_null(0) {
        return Err(response_error("reply value is null"));
    }
    let value = values.value(0);
    serde_json::from_str(value).map_err(|e| response_error(e.to_string()))
}

/// Errors that reject admission or indicate that an admitted writer was lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    UnknownCapability,
    Closing,
    WriterLost,
    PayloadMismatch,
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for RegistryError {}

impl StackError for RegistryError {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        buf.push(format!("{layer}: {self:?}"));
    }

    fn next(&self) -> Option<&dyn StackError> {
        None
    }
}

impl ErrorExt for RegistryError {
    fn status_code(&self) -> StatusCode {
        StatusCode::InvalidArguments
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Result of atomically admitting or joining a payload.
#[derive(Debug)]
pub enum Admission<R> {
    Start(watch::Sender<Option<Arc<R>>>),
    Join(watch::Receiver<Option<Arc<R>>>),
}

#[derive(Debug)]
struct Entry<P, R> {
    payload: Option<P>,
    sender: Option<watch::Sender<Option<Arc<R>>>>,
    receiver: watch::Receiver<Option<Arc<R>>>,
    closing: bool,
}

#[derive(Debug)]
struct State<P, R> {
    openers: HashMap<String, String>,
    entries: HashMap<String, Entry<P, R>>,
}

/// Receiver-issued capabilities for single-write admission and completion sharing.
#[derive(Debug)]
pub struct FlowWriteRegistry<P, R> {
    state: Mutex<State<P, R>>,
}

impl<P, R> Default for FlowWriteRegistry<P, R> {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                openers: HashMap::new(),
                entries: HashMap::new(),
            }),
        }
    }
}

impl<P: PartialEq, R: Send + Sync + 'static> FlowWriteRegistry<P, R> {
    /// Returns the active capability for this opener nonce, creating one if needed.
    pub fn open(&self, nonce: &str) -> Result<String, RegistryError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(capability) = state.openers.get(nonce) {
            if state
                .entries
                .get(capability)
                .is_some_and(|entry| entry.closing)
            {
                return Err(RegistryError::Closing);
            }
            return Ok(capability.clone());
        }
        let capability = Uuid::new_v4().to_string();
        let (sender, receiver) = watch::channel(None);
        state.openers.insert(nonce.to_string(), capability.clone());
        state.entries.insert(
            capability.clone(),
            Entry {
                payload: None,
                sender: Some(sender),
                receiver,
                closing: false,
            },
        );
        Ok(capability)
    }

    /// Atomically starts one payload or joins its identical in-flight completion.
    pub fn admit(&self, capability: &str, payload: P) -> Result<Admission<R>, RegistryError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(capability)
            .ok_or(RegistryError::UnknownCapability)?;
        if entry.closing {
            return Err(RegistryError::Closing);
        }
        match &entry.payload {
            Some(existing) if existing != &payload => Err(RegistryError::PayloadMismatch),
            Some(_) => Ok(Admission::Join(entry.receiver.clone())),
            None => {
                entry.payload = Some(payload);
                let sender = entry.sender.take().ok_or(RegistryError::Closing)?;
                Ok(Admission::Start(sender))
            }
        }
    }

    /// Closes admission, drains an admitted writer, then deletes the capability.
    pub async fn close(&self, capability: &str) -> Result<(), RegistryError> {
        let receiver = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(entry) = state.entries.get_mut(capability) else {
                return Ok(());
            };
            entry.closing = true;
            if entry.payload.is_none() {
                state.entries.remove(capability);
                state.openers.retain(|_, value| value != capability);
                return Ok(());
            }
            entry.receiver.clone()
        };
        wait_completion(receiver).await?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.entries.remove(capability);
        state.openers.retain(|_, value| value != capability);
        Ok(())
    }
}

/// Waits for an admitted writer's positive terminal completion.
pub async fn wait_completion<R>(
    mut receiver: watch::Receiver<Option<Arc<R>>>,
) -> Result<Arc<R>, RegistryError> {
    loop {
        if let Some(value) = receiver.borrow().clone() {
            return Ok(value);
        }
        if receiver.changed().await.is_err() {
            return Err(RegistryError::WriterLost);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    #[tokio::test]
    async fn concurrent_admission_has_one_owner_and_same_payload_joins() {
        let registry = Arc::new(FlowWriteRegistry::<String, usize>::default());
        let cap = registry.open("nonce").unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let registry = registry.clone();
            let cap = cap.clone();
            let barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                barrier.wait();
                registry.admit(&cap, "payload".into()).unwrap()
            }));
        }
        let admissions: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert_eq!(
            admissions
                .iter()
                .filter(|a| matches!(a, Admission::Start(_)))
                .count(),
            1
        );
        let sender = admissions
            .into_iter()
            .find_map(|a| match a {
                Admission::Start(s) => Some(s),
                _ => None,
            })
            .unwrap();
        sender.send(Some(Arc::new(7))).unwrap();
        registry.close(&cap).await.unwrap();
    }

    #[tokio::test]
    async fn close_drains_active_writer_and_deletes_old_capability() {
        let registry = Arc::new(FlowWriteRegistry::<String, usize>::default());
        let cap = registry.open("first").unwrap();
        let Admission::Start(sender) = registry.admit(&cap, "payload".into()).unwrap() else {
            panic!()
        };
        let close_registry = registry.clone();
        let close_cap = cap.clone();
        let close = tokio::spawn(async move { close_registry.close(&close_cap).await });
        tokio::task::yield_now().await;
        assert!(matches!(
            registry.admit(&cap, "payload".into()),
            Err(RegistryError::Closing)
        ));
        assert!(!close.is_finished());
        sender.send(Some(Arc::new(9))).unwrap();
        close.await.unwrap().unwrap();
        assert!(matches!(
            registry.admit(&cap, "payload".into()),
            Err(RegistryError::UnknownCapability)
        ));
        registry.close(&cap).await.unwrap();
        let new_cap = registry.open("first").unwrap();
        assert_ne!(cap, new_cap);
        assert!(matches!(
            registry.admit(&cap, "late".into()),
            Err(RegistryError::UnknownCapability)
        ));
        assert!(matches!(
            registry.admit(&new_cap, "new".into()).unwrap(),
            Admission::Start(_)
        ));
    }

    #[tokio::test]
    async fn writer_loss_is_not_completion() {
        let registry = FlowWriteRegistry::<String, usize>::default();
        let cap = registry.open("nonce").unwrap();
        let Admission::Start(sender) = registry.admit(&cap, "payload".into()).unwrap() else {
            panic!()
        };
        let Admission::Join(receiver) = registry.admit(&cap, "payload".into()).unwrap() else {
            panic!()
        };
        drop(sender);
        assert_eq!(
            wait_completion(receiver).await.unwrap_err(),
            RegistryError::WriterLost
        );
        assert_eq!(
            registry.close(&cap).await.unwrap_err(),
            RegistryError::WriterLost
        );
    }

    #[test]
    fn command_and_reply_envelope_round_trip_and_malformed_envelopes() {
        let command = Command::Open { nonce: "n".into() };
        assert_eq!(
            Command::decode(&command.encode().unwrap()).unwrap(),
            command
        );
        let reply = Reply::Complete {
            rows: 2,
            cost: 3,
            metrics: Some(common_recordbatch::adapter::RecordBatchMetrics::default()),
            error: Some(TerminalError {
                code: 4,
                message: "terminal".into(),
            }),
        };
        let batches = reply_batches(&reply).unwrap();
        let decoded = decode_reply(&batches).unwrap();
        assert!(matches!(
            decoded,
            Reply::Complete {
                rows: 2,
                cost: 3,
                error: Some(_),
                ..
            }
        ));

        let schema = envelope_schema();
        let two_rows = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(StringVector::from(vec!["a", "b"])) as VectorRef],
        )
        .unwrap();
        assert!(
            decode_reply(&RecordBatches::try_new(schema.clone(), vec![two_rows]).unwrap()).is_err()
        );
        let int_schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "envelope",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let non_string = RecordBatch::new(
            int_schema.clone(),
            vec![Arc::new(datatypes::vectors::Int32Vector::from(vec![Some(1)])) as VectorRef],
        )
        .unwrap();
        assert!(
            decode_reply(&RecordBatches::try_new(int_schema, vec![non_string]).unwrap()).is_err()
        );
        let invalid_json = RecordBatch::new(
            envelope_schema(),
            vec![Arc::new(StringVector::from(vec!["not json"])) as VectorRef],
        )
        .unwrap();
        assert!(
            decode_reply(
                &RecordBatches::try_new(invalid_json.schema.clone(), vec![invalid_json]).unwrap()
            )
            .is_err()
        );
    }
}
