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

use common_telemetry::{debug, error, info, warn};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{Result, ToJsonSnafu};
pub(crate) use crate::store::state_store::StateStoreRef;
use crate::ProcedureId;

pub mod state_store;
pub mod util;

/// Key prefix of procedure store.
const PROC_PATH: &str = "procedure/";

/// Constructs a path for procedure store.
macro_rules! proc_path {
    ($store: expr, $fmt:expr) => { format!("{}{}", $store.proc_path(), format_args!($fmt)) };
    ($store: expr, $fmt:expr, $($args:tt)*) => { format!("{}{}", $store.proc_path(), format_args!($fmt, $($args)*)) };
}

pub(crate) use proc_path;

/// Serialized data of a procedure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcedureMessage {
    /// Type name of the procedure. The procedure framework also use the type name to
    /// find a loader to load the procedure.
    pub type_name: String,
    /// The data of the procedure.
    pub data: String,
    /// Parent procedure id.
    pub parent_id: Option<ProcedureId>,
    /// Current step.
    pub step: u32,
    /// Errors raised during the procedure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A collection of all procedures' messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcedureMessages {
    /// A map of uncommitted procedures
    pub messages: HashMap<ProcedureId, ProcedureMessage>,
    /// A map of rolling back procedures
    pub rollback_messages: HashMap<ProcedureId, ProcedureMessage>,
    /// A list of finished procedures' ids
    pub finished_ids: Vec<ProcedureId>,
}

/// Procedure storage layer.
pub(crate) struct ProcedureStore {
    proc_path: String,
    store: StateStoreRef,
}

impl ProcedureStore {
    /// Creates a new [ProcedureStore] from specific [StateStoreRef].
    pub(crate) fn new(parent_path: &str, store: StateStoreRef) -> ProcedureStore {
        let proc_path = format!("{}{PROC_PATH}", parent_path);
        info!("The procedure state store path is: {}", &proc_path);
        ProcedureStore { proc_path, store }
    }

    #[inline]
    pub(crate) fn proc_path(&self) -> &str {
        &self.proc_path
    }

    /// Dump the `procedure` to the storage.
    pub(crate) async fn store_procedure(
        &self,
        procedure_id: ProcedureId,
        step: u32,
        type_name: String,
        data: String,
        parent_id: Option<ProcedureId>,
    ) -> Result<()> {
        let message = ProcedureMessage {
            type_name,
            data,
            parent_id,
            step,
            error: None,
        };
        let key = ParsedKey {
            prefix: &self.proc_path,
            procedure_id,
            step,
            key_type: KeyType::Step,
        }
        .to_string();
        let value = serde_json::to_string(&message).context(ToJsonSnafu)?;

        self.store.put(&key, value.into_bytes()).await?;

        Ok(())
    }

    /// Write commit flag to the storage.
    pub(crate) async fn commit_procedure(
        &self,
        procedure_id: ProcedureId,
        step: u32,
    ) -> Result<()> {
        let key = ParsedKey {
            prefix: &self.proc_path,
            procedure_id,
            step,
            key_type: KeyType::Commit,
        }
        .to_string();
        self.store.put(&key, Vec::new()).await?;

        Ok(())
    }

    /// Write rollback flag to the storage.
    pub(crate) async fn rollback_procedure(
        &self,
        procedure_id: ProcedureId,
        message: ProcedureMessage,
    ) -> Result<()> {
        let key = ParsedKey {
            prefix: &self.proc_path,
            procedure_id,
            step: message.step,
            key_type: KeyType::Rollback,
        }
        .to_string();

        self.store
            .put(&key, serde_json::to_vec(&message).context(ToJsonSnafu)?)
            .await?;

        Ok(())
    }

    /// Delete states of procedure from the storage.
    pub(crate) async fn delete_procedure(&self, procedure_id: ProcedureId) -> Result<()> {
        let path = proc_path!(self, "{procedure_id}/");
        // TODO(yingwen): We can optimize this to avoid reading the value.
        let mut key_values = self.store.walk_top_down(&path).await?;
        // 8 should be enough for most procedures.
        let mut step_keys = Vec::with_capacity(8);
        let mut finish_keys = Vec::new();
        while let Some((key_set, _)) = key_values.try_next().await? {
            let key = key_set.key();
            let Some(curr_key) = ParsedKey::parse_str(&self.proc_path, key) else {
                warn!("Unknown key while deleting procedures, key: {}", key);
                continue;
            };
            if curr_key.key_type == KeyType::Step {
                step_keys.extend(key_set.keys());
            } else {
                // .commit or .rollback
                finish_keys.extend(key_set.keys());
            }
        }

        debug!(
            "Delete keys for procedure {}, step_keys: {:?}, finish_keys: {:?}",
            procedure_id, step_keys, finish_keys
        );
        // We delete all step keys first.
        self.store.batch_delete(step_keys.as_slice()).await?;
        // Then we delete the finish keys, to ensure
        self.store.batch_delete(finish_keys.as_slice()).await?;
        // Finally we remove the directory itself.
        self.store.delete(&path).await?;
        // Maybe we could use procedure_id.commit/rollback as the file name so we could
        // use remove_all to remove the directory and then remove the commit/rollback file.

        Ok(())
    }

    /// Load procedures from the storage.
    pub(crate) async fn load_messages(&self) -> Result<ProcedureMessages> {
        // Track the key-value pair by procedure id.
        let mut procedure_key_values: HashMap<_, (ParsedKey, Vec<u8>)> = HashMap::new();

        // Scan all procedures.
        let mut key_values = self.store.walk_top_down(&self.proc_path).await?;
        while let Some((key_set, value)) = key_values.try_next().await? {
            let key = key_set.key();
            let Some(curr_key) = ParsedKey::parse_str(&self.proc_path, key) else {
                warn!("Unknown key while loading procedures, key: {}", key);
                continue;
            };

            if let Some(entry) = procedure_key_values.get_mut(&curr_key.procedure_id) {
                if entry.0.step < curr_key.step {
                    entry.0 = curr_key;
                    entry.1 = value;
                }
            } else {
                let _ = procedure_key_values.insert(curr_key.procedure_id, (curr_key, value));
            }
        }

        let mut messages = HashMap::with_capacity(procedure_key_values.len());
        let mut rollback_messages = HashMap::new();
        let mut finished_ids = Vec::new();
        for (procedure_id, (parsed_key, value)) in procedure_key_values {
            match parsed_key.key_type {
                KeyType::Step => {
                    let Some(message) = self.load_one_message(&parsed_key, &value) else {
                        // We don't abort the loading process and just ignore errors to ensure all remaining
                        // procedures are loaded.
                        continue;
                    };
                    let _ = messages.insert(procedure_id, message);
                }
                KeyType::Commit => {
                    finished_ids.push(procedure_id);
                }
                KeyType::Rollback => {
                    let Some(message) = self.load_one_message(&parsed_key, &value) else {
                        // We don't abort the loading process and just ignore errors to ensure all remaining
                        // procedures are loaded.
                        continue;
                    };
                    let _ = rollback_messages.insert(procedure_id, message);
                }
            }
        }

        Ok(ProcedureMessages {
            messages,
            rollback_messages,
            finished_ids,
        })
    }

    fn load_one_message(&self, key: &ParsedKey, value: &[u8]) -> Option<ProcedureMessage> {
        serde_json::from_slice(value)
            .map_err(|e| {
                // `e` doesn't impl ErrorExt so we print it as normal error.
                error!("Failed to parse value, key: {:?}, source: {:?}", key, e);
                e
            })
            .ok()
    }
}

/// Suffix type of the key.
#[derive(Debug, PartialEq, Eq)]
enum KeyType {
    Step,
    Commit,
    Rollback,
}

impl KeyType {
    fn as_str(&self) -> &'static str {
        match self {
            KeyType::Step => "step",
            KeyType::Commit => "commit",
            KeyType::Rollback => "rollback",
        }
    }

    fn from_str(s: &str) -> Option<KeyType> {
        match s {
            "step" => Some(KeyType::Step),
            "commit" => Some(KeyType::Commit),
            "rollback" => Some(KeyType::Rollback),
            _ => None,
        }
    }
}

/// Key to refer the procedure in the [ProcedureStore].
#[derive(Debug, PartialEq, Eq)]
struct ParsedKey<'a> {
    prefix: &'a str,
    procedure_id: ProcedureId,
    step: u32,
    key_type: KeyType,
}

impl<'a> fmt::Display for ParsedKey<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}/{:010}.{}",
            self.prefix,
            self.procedure_id,
            self.step,
            self.key_type.as_str(),
        )
    }
}

impl<'a> ParsedKey<'a> {
    /// Try to parse the key from specific `input`.
    fn parse_str(prefix: &'a str, input: &str) -> Option<ParsedKey<'a>> {
        let input = input.strip_prefix(prefix)?;
        let mut iter = input.rsplit('/');
        let name = iter.next()?;
        let id_str = iter.next()?;

        let procedure_id = ProcedureId::parse_str(id_str).ok()?;

        let mut parts = name.split('.');
        let step_str = parts.next()?;
        let suffix = parts.next()?;
        let key_type = KeyType::from_str(suffix)?;
        let step = step_str.parse().ok()?;

        Some(ParsedKey {
            prefix,
            procedure_id,
            step,
            key_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::ObjectStore;

    use crate::store::state_store::ObjectStateStore;
    use crate::BoxedProcedure;

    impl ProcedureStore {
        pub(crate) fn from_object_store(store: ObjectStore) -> ProcedureStore {
            let state_store = ObjectStateStore::new(store);

            ProcedureStore::new("data/", Arc::new(state_store))
        }
    }

    use async_trait::async_trait;
    use common_test_util::temp_dir::{create_temp_dir, TempDir};
    use object_store::services::Fs as Builder;

    use super::*;
    use crate::{Context, LockKey, Procedure, Status};

    fn procedure_store_for_test(dir: &TempDir) -> ProcedureStore {
        let store_dir = dir.path().to_str().unwrap();
        let builder = Builder::default().root(store_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        ProcedureStore::from_object_store(object_store)
    }

    #[test]
    fn test_parsed_key() {
        let dir = create_temp_dir("store_procedure");
        let store = procedure_store_for_test(&dir);

        let procedure_id = ProcedureId::random();
        let key = ParsedKey {
            prefix: &store.proc_path,
            procedure_id,
            step: 2,
            key_type: KeyType::Step,
        };
        assert_eq!(
            proc_path!(store, "{procedure_id}/0000000002.step"),
            key.to_string()
        );
        assert_eq!(
            key,
            ParsedKey::parse_str(&store.proc_path, &key.to_string()).unwrap()
        );

        let key = ParsedKey {
            prefix: &store.proc_path,
            procedure_id,
            step: 2,
            key_type: KeyType::Commit,
        };
        assert_eq!(
            proc_path!(store, "{procedure_id}/0000000002.commit"),
            key.to_string()
        );
        assert_eq!(
            key,
            ParsedKey::parse_str(&store.proc_path, &key.to_string()).unwrap()
        );

        let key = ParsedKey {
            prefix: &store.proc_path,
            procedure_id,
            step: 2,
            key_type: KeyType::Rollback,
        };
        assert_eq!(
            proc_path!(store, "{procedure_id}/0000000002.rollback"),
            key.to_string()
        );
        assert_eq!(
            key,
            ParsedKey::parse_str(&store.proc_path, &key.to_string()).unwrap()
        );
    }

    #[test]
    fn test_parse_invalid_key() {
        let dir = create_temp_dir("store_procedure");
        let store = procedure_store_for_test(&dir);

        assert!(ParsedKey::parse_str(&store.proc_path, "").is_none());
        assert!(ParsedKey::parse_str(&store.proc_path, "invalidprefix").is_none());
        assert!(ParsedKey::parse_str(&store.proc_path, "procedu/0000000003.step").is_none());
        assert!(ParsedKey::parse_str(&store.proc_path, "procedure-0000000003.step").is_none());

        let procedure_id = ProcedureId::random();
        let input = proc_path!(store, "{procedure_id}");
        assert!(ParsedKey::parse_str(&store.proc_path, &input).is_none());

        let input = proc_path!(store, "{procedure_id}");
        assert!(ParsedKey::parse_str(&store.proc_path, &input).is_none());

        let input = proc_path!(store, "{procedure_id}/0000000003");
        assert!(ParsedKey::parse_str(&store.proc_path, &input).is_none());

        let input = proc_path!(store, "{procedure_id}/0000000003.");
        assert!(ParsedKey::parse_str(&store.proc_path, &input).is_none());

        let input = proc_path!(store, "{procedure_id}/0000000003.other");
        assert!(ParsedKey::parse_str(&store.proc_path, &input).is_none());

        assert!(ParsedKey::parse_str(&store.proc_path, "12345/0000000003.step").is_none());

        let input = proc_path!(store, "{procedure_id}-0000000003.commit");
        assert!(ParsedKey::parse_str(&store.proc_path, &input).is_none());
    }

    #[test]
    fn test_procedure_message() {
        let mut message = ProcedureMessage {
            type_name: "TestMessage".to_string(),
            data: "no parent id".to_string(),
            parent_id: None,
            step: 4,
            error: None,
        };

        let json = serde_json::to_string(&message).unwrap();
        assert_eq!(
            json,
            r#"{"type_name":"TestMessage","data":"no parent id","parent_id":null,"step":4}"#
        );

        let procedure_id = ProcedureId::parse_str("9f805a1f-05f7-490c-9f91-bd56e3cc54c1").unwrap();
        message.parent_id = Some(procedure_id);
        let json = serde_json::to_string(&message).unwrap();
        assert_eq!(
            json,
            r#"{"type_name":"TestMessage","data":"no parent id","parent_id":"9f805a1f-05f7-490c-9f91-bd56e3cc54c1","step":4}"#
        );
    }

    struct MockProcedure {
        data: String,
    }

    impl MockProcedure {
        fn new(data: impl Into<String>) -> MockProcedure {
            MockProcedure { data: data.into() }
        }
    }

    #[async_trait]
    impl Procedure for MockProcedure {
        fn type_name(&self) -> &str {
            "MockProcedure"
        }

        async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
            unimplemented!()
        }

        fn dump(&self) -> Result<String> {
            Ok(self.data.clone())
        }

        fn lock_key(&self) -> LockKey {
            LockKey::default()
        }
    }

    #[tokio::test]
    async fn test_store_procedure() {
        let dir = create_temp_dir("store_procedure");
        let store = procedure_store_for_test(&dir);

        let procedure_id = ProcedureId::random();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("test store procedure"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(procedure_id, 0, type_name, data, None)
            .await
            .unwrap();

        let ProcedureMessages {
            messages,
            rollback_messages,
            finished_ids,
        } = store.load_messages().await.unwrap();
        assert_eq!(1, messages.len());
        assert!(rollback_messages.is_empty());
        assert!(finished_ids.is_empty());
        let msg = messages.get(&procedure_id).unwrap();
        let expect = ProcedureMessage {
            type_name: "MockProcedure".to_string(),
            data: "test store procedure".to_string(),
            parent_id: None,
            step: 0,
            error: None,
        };
        assert_eq!(expect, *msg);
    }

    #[tokio::test]
    async fn test_commit_procedure() {
        let dir = create_temp_dir("commit_procedure");
        let store = procedure_store_for_test(&dir);

        let procedure_id = ProcedureId::random();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("test store procedure"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(procedure_id, 0, type_name, data, None)
            .await
            .unwrap();
        store.commit_procedure(procedure_id, 1).await.unwrap();

        let ProcedureMessages {
            messages,
            rollback_messages,
            finished_ids,
        } = store.load_messages().await.unwrap();
        assert!(messages.is_empty());
        assert!(rollback_messages.is_empty());
        assert_eq!(&[procedure_id], &finished_ids[..]);
    }

    #[tokio::test]
    async fn test_rollback_procedure() {
        let dir = create_temp_dir("rollback_procedure");
        let store = procedure_store_for_test(&dir);

        let procedure_id = ProcedureId::random();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("test store procedure"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(
                procedure_id,
                0,
                type_name.to_string(),
                data.to_string(),
                None,
            )
            .await
            .unwrap();
        let message = ProcedureMessage {
            type_name,
            data,
            parent_id: None,
            step: 1,
            error: None,
        };
        store
            .rollback_procedure(procedure_id, message)
            .await
            .unwrap();

        let ProcedureMessages {
            messages,
            rollback_messages,
            finished_ids,
        } = store.load_messages().await.unwrap();
        assert!(messages.is_empty());
        assert_eq!(1, rollback_messages.len());
        assert!(finished_ids.is_empty());
        assert!(rollback_messages.contains_key(&procedure_id));
    }

    #[tokio::test]
    async fn test_delete_procedure() {
        let dir = create_temp_dir("delete_procedure");
        let store = procedure_store_for_test(&dir);

        let procedure_id = ProcedureId::random();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("test store procedure"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(procedure_id, 0, type_name, data, None)
            .await
            .unwrap();
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(procedure_id, 1, type_name, data, None)
            .await
            .unwrap();

        store.delete_procedure(procedure_id).await.unwrap();

        let ProcedureMessages {
            messages,
            rollback_messages,
            finished_ids,
        } = store.load_messages().await.unwrap();
        assert!(messages.is_empty());
        assert!(rollback_messages.is_empty());
        assert!(finished_ids.is_empty());
    }

    #[tokio::test]
    async fn test_delete_committed_procedure() {
        let dir = create_temp_dir("delete_committed");
        let store = procedure_store_for_test(&dir);

        let procedure_id = ProcedureId::random();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("test store procedure"));

        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(procedure_id, 0, type_name, data, None)
            .await
            .unwrap();

        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(procedure_id, 1, type_name, data, None)
            .await
            .unwrap();
        store.commit_procedure(procedure_id, 2).await.unwrap();

        store.delete_procedure(procedure_id).await.unwrap();

        let ProcedureMessages {
            messages,
            rollback_messages,
            finished_ids,
        } = store.load_messages().await.unwrap();
        assert!(messages.is_empty());
        assert!(rollback_messages.is_empty());
        assert!(finished_ids.is_empty());
    }

    #[tokio::test]
    async fn test_load_messages() {
        let dir = create_temp_dir("load_messages");
        let store = procedure_store_for_test(&dir);

        // store 3 steps
        let id0 = ProcedureId::random();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("id0-0"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(id0, 0, type_name, data, None)
            .await
            .unwrap();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("id0-1"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(id0, 1, type_name, data, None)
            .await
            .unwrap();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("id0-2"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(id0, 2, type_name, data, None)
            .await
            .unwrap();

        // store 2 steps and then commit
        let id1 = ProcedureId::random();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("id1-0"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(id1, 0, type_name, data, None)
            .await
            .unwrap();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("id1-1"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(id1, 1, type_name, data, None)
            .await
            .unwrap();
        store.commit_procedure(id1, 2).await.unwrap();

        // store 1 step
        let id2 = ProcedureId::random();
        let procedure: BoxedProcedure = Box::new(MockProcedure::new("id2-0"));
        let type_name = procedure.type_name().to_string();
        let data = procedure.dump().unwrap();
        store
            .store_procedure(id2, 0, type_name, data, None)
            .await
            .unwrap();

        let ProcedureMessages {
            messages,
            rollback_messages,
            finished_ids,
        } = store.load_messages().await.unwrap();
        assert_eq!(2, messages.len());
        assert!(rollback_messages.is_empty());
        assert_eq!(1, finished_ids.len());

        let msg = messages.get(&id0).unwrap();
        assert_eq!("id0-2", msg.data);
        let msg = messages.get(&id2).unwrap();
        assert_eq!("id2-0", msg.data);
    }
}
