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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_procedure::error::{
    DeletePoisonSnafu, GetPoisonSnafu, PutPoisonSnafu, Result as ProcedureResult,
};
use common_procedure::poison::PoisonManager;
use common_procedure::{PoisonKey, ProcedureId};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{ProcedurePoisonConflictSnafu, Result};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{DeserializedValueWithBytes, MetadataValue};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::kv_backend::KvBackendRef;

const PROCEDURE_POISON_KEY_PREFIX: &str = "__procedure_poison";

fn to_meta_key(token: &PoisonKey) -> String {
    format!("{}/{}", PROCEDURE_POISON_KEY_PREFIX, token)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoisonValue {
    procedure_id: ProcedureId,
}

type PoisonDecodeResult = Result<Option<DeserializedValueWithBytes<PoisonValue>>>;

/// The manager for poison.
pub struct KvBackendPoisonManager {
    kv_backend: KvBackendRef,
}

impl KvBackendPoisonManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Builds a create consistency poison transaction,
    /// it expected the `__procedure_poison/{resource_type}/{resource_id}` wasn't occupied.
    fn build_create_txn(
        &self,
        key: &str,
        value: &PoisonValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> PoisonDecodeResult,
    )> {
        let key = key.as_bytes().to_vec();
        let value = value.try_as_raw_value()?;
        let txn = Txn::put_if_not_exists(key.clone(), value);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(key)),
        ))
    }

    /// Builds a delete poison transaction,
    /// it expected the `__procedure_poison/{resource_type}/{resource_id}` was occupied.
    fn build_delete_txn(
        &self,
        key: &str,
        value: PoisonValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> PoisonDecodeResult,
    )> {
        let key = key.as_bytes().to_vec();
        let value = value.try_as_raw_value()?;

        let txn = Txn::new()
            .when(vec![Compare::with_value(
                key.clone(),
                CompareOp::Equal,
                value,
            )])
            .and_then(vec![TxnOp::Delete(key.clone())])
            .or_else(vec![TxnOp::Get(key.clone())]);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(key)),
        ))
    }

    async fn get(&self, token: &PoisonKey) -> Result<Option<PoisonValue>> {
        let key = to_meta_key(token);
        let value = self.kv_backend.get(key.as_bytes()).await?;
        value
            .map(|v| PoisonValue::try_from_raw_value(&v.value))
            .transpose()
    }

    /// Put the poison.
    ///
    /// If the poison is already put by other procedure, it will return an error.
    async fn put(&self, key: &PoisonKey, procedure_id: ProcedureId) -> Result<()> {
        let key = to_meta_key(key);
        let (txn, on_failure) = self.build_create_txn(&key, &PoisonValue { procedure_id })?;

        let mut resp = self.kv_backend.txn(txn).await?;

        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_value = on_failure(&mut set)?
                .context(ProcedurePoisonConflictSnafu {
                    msg: "Reads the empty poison value in comparing operation of the put consistency poison",
                })?
                .into_inner();

            ensure!(
                remote_value.procedure_id == procedure_id,
                ProcedurePoisonConflictSnafu {
                    msg: format!(
                        "The poison is already put by other procedure {}",
                        remote_value.procedure_id
                    ),
                }
            );
        }

        Ok(())
    }

    /// Deletes the poison.
    ///
    /// If the poison is not put by the procedure, it will return an error.
    async fn delete(&self, key: &PoisonKey, procedure_id: ProcedureId) -> Result<()> {
        let key = to_meta_key(key);
        let (txn, on_failure) = self.build_delete_txn(&key, PoisonValue { procedure_id })?;

        let mut resp = self.kv_backend.txn(txn).await?;

        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_value = on_failure(&mut set)?;

            ensure!(
                remote_value.is_none(),
                ProcedurePoisonConflictSnafu {
                    msg: format!(
                        "The poison is already put by other procedure {}",
                        remote_value.unwrap().into_inner().procedure_id
                    ),
                }
            );
        }

        Ok(())
    }
}

#[async_trait]
impl PoisonManager for KvBackendPoisonManager {
    async fn set_poison(&self, key: &PoisonKey, procedure_id: ProcedureId) -> ProcedureResult<()> {
        self.put(key, procedure_id)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| PutPoisonSnafu {
                key: key.clone(),
                procedure_id,
            })
    }

    async fn delete_poison(
        &self,
        key: &PoisonKey,
        procedure_id: ProcedureId,
    ) -> ProcedureResult<()> {
        self.delete(key, procedure_id)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| DeletePoisonSnafu {
                key: key.clone(),
                procedure_id,
            })
    }

    async fn get_poison(&self, key: &PoisonKey) -> ProcedureResult<Option<ProcedureId>> {
        self.get(key)
            .await
            .map(|v| v.map(|v| v.procedure_id))
            .map_err(BoxedError::new)
            .with_context(|_| GetPoisonSnafu { key: key.clone() })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use common_procedure::poison::ResourceType;

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_poison() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let poison_manager = KvBackendPoisonManager::new(mem_kv.clone());

        let key = PoisonKey::new(ResourceType::Table, "1");

        let procedure_id = ProcedureId::random();

        poison_manager.put(&key, procedure_id).await.unwrap();

        // Put again, should be ok.
        poison_manager.put(&key, procedure_id).await.unwrap();

        // Delete, should be ok.
        poison_manager.delete(&key, procedure_id).await.unwrap();

        // Delete again, should be ok.
        poison_manager.delete(&key, procedure_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_consistency_poison_failed() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let poison_manager = KvBackendPoisonManager::new(mem_kv.clone());

        let key = PoisonKey::new(ResourceType::Table, "1");

        let procedure_id = ProcedureId::random();
        let procedure_id2 = ProcedureId::random();

        poison_manager.put(&key, procedure_id).await.unwrap();

        let err = poison_manager.put(&key, procedure_id2).await.unwrap_err();
        assert_matches!(err, Error::ProcedurePoisonConflict { .. });

        let err = poison_manager
            .delete(&key, procedure_id2)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ProcedurePoisonConflict { .. });
    }

    #[test]
    fn test_serialize_deserialize() {
        let key = PoisonKey::new(ResourceType::Table, "1");
        let value = PoisonValue {
            procedure_id: ProcedureId::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        };

        let serialized_key = key.to_string().as_bytes().to_vec();
        let serialized_value = value.try_as_raw_value().unwrap();

        let expected_key = "__procedure_poison/table/1";
        let expected_value = r#"{"procedure_id":"550e8400-e29b-41d4-a716-446655440000"}"#;

        assert_eq!(expected_key.as_bytes(), serialized_key);
        assert_eq!(expected_value.as_bytes(), serialized_value);
    }
}
