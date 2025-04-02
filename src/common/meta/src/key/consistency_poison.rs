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

//! ## Description:
//! This key is used to mark an ongoing operation on a specific resource. It ensures metadata consistency
//! between datanodes and the metadata.
//!
//! ## Behavior:
//! - **Insertion**: When an operation begins on a resource, this key is inserted into the metadata store.
//! - **Deletion**: If the operation completes successfully, the key is removed from the metadata store.
//! - **Failure Handling**:
//!   - If the operation fails, the key remains in the metadata store.
//!   - The presence of this key indicates possible metadata inconsistencies between datanodes and the metadata service.
//!   - New operations on the same resource are rejected until the metadata is repaired.
//!
//! ## Example Keys:
//! - `__consistency_poison/table/1234` → Ensures consistency for table ID `1234`.
//! - `__consistency_poison/index/5678` → Ensures consistency for index ID `5678`.
//!
//! ## Usage Considerations:
//! - Before performing a new operation on a resource, check for the existence of this key.
//! - If the key exists, apply appropriate recovery mechanisms before proceeding with further operations.
//! - Implement logging and alerting to detect and resolve inconsistencies efficiently.

use std::fmt::Display;
use std::str::FromStr;

use common_procedure::ProcedureId;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};
use strum::{AsRefStr, EnumString};

use crate::error::{self, InvalidMetadataSnafu, Result};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    DeserializedValueWithBytes, MetadataKey, MetadataValue, CONSISTENCY_POISON_KEY_PATTERN,
    CONSISTENCY_POISON_PREFIX,
};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::kv_backend::KvBackendRef;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum ResourceType {
    Table,
}

/// The key of consistency poison.
///
/// The format is `__consistency_poison/{resource_type}/{resource_id}`.
pub struct ConsistencyPoisonKey {
    resource_type: ResourceType,
    resource_id: u64,
}

impl ConsistencyPoisonKey {
    pub fn new(resource_type: ResourceType, resource_id: u64) -> Self {
        Self {
            resource_type,
            resource_id,
        }
    }
}

impl Display for ConsistencyPoisonKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            CONSISTENCY_POISON_PREFIX,
            self.resource_type.as_ref(),
            self.resource_id
        )
    }
}

impl MetadataKey<'_, ConsistencyPoisonKey> for ConsistencyPoisonKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<ConsistencyPoisonKey> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "ConsistencyPoisonKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures = CONSISTENCY_POISON_KEY_PATTERN
            .captures(key)
            .with_context(|| InvalidMetadataSnafu {
                err_msg: format!("Invalid ConsistencyPoisonKey '{key}'"),
            })?;
        // Safety: pass the regex check above
        let resource_type =
            ResourceType::from_str(&captures[1])
                .ok()
                .with_context(|| InvalidMetadataSnafu {
                    err_msg: format!("Invalid ResourceType '{}'", &captures[1]),
                })?;
        let resource_id = captures[2].parse::<u64>().unwrap();
        Ok(ConsistencyPoisonKey {
            resource_type,
            resource_id,
        })
    }
}

/// The value of consistency poison.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsistencyPoisonValue {
    procedure_id: String,
}

#[derive(Clone)]

pub struct ConsistencyPoisonManager {
    kv_backend: KvBackendRef,
}

pub type ConsistencyPoisonDecodeResult =
    Result<Option<DeserializedValueWithBytes<ConsistencyPoisonValue>>>;

impl ConsistencyPoisonManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Builds a create consistency poison transaction, it expected the `__consistency_poison/{resource_type}/{resource_id}` wasn't occupied.
    fn build_create_txn(
        &self,
        key: &ConsistencyPoisonKey,
        value: &ConsistencyPoisonValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> ConsistencyPoisonDecodeResult,
    )> {
        let key = key.to_bytes();
        let value = value.try_as_raw_value()?;

        let txn = Txn::put_if_not_exists(key.clone(), value);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(key)),
        ))
    }

    fn build_delete_txn(
        &self,
        key: &ConsistencyPoisonKey,
        value: ConsistencyPoisonValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> ConsistencyPoisonDecodeResult,
    )> {
        let key = key.to_bytes();
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

    #[cfg(test)]
    pub async fn get(&self, key: &ConsistencyPoisonKey) -> Result<Option<ConsistencyPoisonValue>> {
        let key = key.to_bytes();
        let value = self.kv_backend.get(&key).await?;
        Ok(value.map(|v| ConsistencyPoisonValue::try_from_raw_value(&v.value).unwrap()))
    }

    /// Put the consistency poison.
    ///
    /// If the consistency poison is already put by other procedure, it will return an error.
    pub async fn put(&self, key: &ConsistencyPoisonKey, procedure_id: ProcedureId) -> Result<()> {
        let (txn, on_failure) = self.build_create_txn(
            key,
            &ConsistencyPoisonValue {
                procedure_id: procedure_id.to_string(),
            },
        )?;

        let mut resp = self.kv_backend.txn(txn).await?;

        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_value = on_failure(&mut set)?
                .context(error::ConsistencyPoisonSnafu {
                    msg: "Reads the empty consistency poison value in comparing operation of the put consistency poison",
                })?
                .into_inner();

            ensure!(
                remote_value.procedure_id == procedure_id.to_string(),
                error::ConsistencyPoisonSnafu {
                    msg: format!(
                        "The consistency poison value is already put by other procedure {}",
                        remote_value.procedure_id
                    ),
                }
            );
        }

        Ok(())
    }

    /// Deletes the consistency poison.
    pub async fn delete(
        &self,
        key: &ConsistencyPoisonKey,
        procedure_id: ProcedureId,
    ) -> Result<()> {
        let (txn, on_failure) = self.build_delete_txn(
            key,
            ConsistencyPoisonValue {
                procedure_id: procedure_id.to_string(),
            },
        )?;

        let mut resp = self.kv_backend.txn(txn).await?;

        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_value = on_failure(&mut set)?;

            ensure!(
                remote_value.is_none(),
                error::ConsistencyPoisonSnafu {
                    msg: format!(
                        "The consistency poison value is not put by the procedure {}",
                        remote_value.unwrap().into_inner().procedure_id
                    ),
                }
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_consistency_poison() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let consistency_guard_manager = ConsistencyPoisonManager::new(mem_kv.clone());

        let key = ConsistencyPoisonKey {
            resource_type: ResourceType::Table,
            resource_id: 1,
        };

        let procedure_id = ProcedureId::random();

        consistency_guard_manager
            .put(&key, procedure_id)
            .await
            .unwrap();

        // Put again, should be ok.
        consistency_guard_manager
            .put(&key, procedure_id)
            .await
            .unwrap();

        // Delete, should be ok.
        consistency_guard_manager
            .delete(&key, procedure_id)
            .await
            .unwrap();

        // Delete again, should be ok.
        consistency_guard_manager
            .delete(&key, procedure_id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_consistency_poison_failed() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let consistency_guard_manager = ConsistencyPoisonManager::new(mem_kv.clone());

        let key = ConsistencyPoisonKey {
            resource_type: ResourceType::Table,
            resource_id: 1,
        };

        let procedure_id = ProcedureId::random();
        let procedure_id2 = ProcedureId::random();

        consistency_guard_manager
            .put(&key, procedure_id)
            .await
            .unwrap();

        let err = consistency_guard_manager
            .put(&key, procedure_id2)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ConsistencyPoison { .. });

        let err = consistency_guard_manager
            .delete(&key, procedure_id2)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ConsistencyPoison { .. });
    }

    #[test]
    fn test_serialize_deserialize() {
        let key = ConsistencyPoisonKey {
            resource_type: ResourceType::Table,
            resource_id: 1,
        };
        let value = ConsistencyPoisonValue {
            procedure_id: "1".to_string(),
        };

        let serialized_key = key.to_bytes();
        let serialized_value = value.try_as_raw_value().unwrap();

        let expected_key = "__consistency_poison/table/1";
        let expected_value = r#"{"procedure_id":"1"}"#;

        assert_eq!(expected_key.as_bytes(), serialized_key);
        assert_eq!(expected_value.as_bytes(), serialized_value);
    }
}
