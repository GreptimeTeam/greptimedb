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

use std::fmt::Display;

use snafu::{ensure, OptionExt};

use super::TableMetaKeyGetTxnOp;
use crate::error::{self, Result};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::TableMetaKey;
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::kv_backend::KvBackendRef;

/// [TombstoneManager] provides the ability to:
/// - logically delete values
/// - restore the deleted values
pub(crate) struct TombstoneManager {
    kv_backend: KvBackendRef,
}

const TOMBSTONE_PREFIX: &str = "__tombstone/";

pub(crate) struct TombstoneKey<T>(T);

fn to_tombstone(key: &[u8]) -> Vec<u8> {
    [TOMBSTONE_PREFIX.as_bytes(), key].concat()
}

impl<T: TableMetaKey> TombstoneKey<&T> {
    /// Returns the origin key and tombstone key.
    fn to_keys(&self) -> (Vec<u8>, Vec<u8>) {
        let key = self.0.as_raw_key();
        let tombstone_key = to_tombstone(&key);
        (key, tombstone_key)
    }

    /// Returns the origin key and tombstone key.
    fn into_keys(self) -> (Vec<u8>, Vec<u8>) {
        self.to_keys()
    }

    /// Returns the tombstone key.
    fn to_tombstone_key(&self) -> Vec<u8> {
        let key = self.0.as_raw_key();
        to_tombstone(&key)
    }

    /// Returns the tombstone key.
    fn into_tombstone_key(self) -> Vec<u8> {
        self.to_tombstone_key()
    }
}

impl<T: TableMetaKey> TableMetaKeyGetTxnOp for TombstoneKey<&T> {
    fn build_get_op(
        &self,
    ) -> (
        TxnOp,
        impl FnMut(&'_ mut TxnOpGetResponseSet) -> Option<Vec<u8>>,
    ) {
        let key = to_tombstone(&self.0.as_raw_key());
        (TxnOp::Get(key.clone()), TxnOpGetResponseSet::filter(key))
    }
}

/// Atomic Key:
/// The value corresponding to the key remains consistent between two transactions.
pub(crate) enum Key<T> {
    Atomic(T),
    Other(T),
}

impl<T> Key<T> {
    fn get_inner(&self) -> &T {
        match self {
            Key::Atomic(key) => key,
            Key::Other(key) => key,
        }
    }

    fn is_atomic(&self) -> bool {
        matches!(self, Key::Atomic(_))
    }
}

impl TombstoneManager {
    /// Returns [TombstoneManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }
    /// Creates tombstones for keys.
    ///
    /// Preforms to:
    /// - retrieve all values corresponding `keys`.
    /// - stores tombstone values.
    pub(crate) async fn create<T: TableMetaKey + TableMetaKeyGetTxnOp + Display>(
        &self,
        keys: Vec<Key<T>>,
    ) -> Result<bool> {
        // Builds transaction to retrieve all values
        let (operations, mut filters): (Vec<_>, Vec<_>) = keys
            .iter()
            .map(|key| key.get_inner().build_get_op())
            .unzip();

        let txn = Txn::new().and_then(operations);
        let mut resp = self.kv_backend.txn(txn).await?;
        ensure!(
            resp.succeeded,
            error::UnexpectedSnafu {
                err_msg: "Failed to retrieves the metadata"
            }
        );

        let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
        // Builds the create tombstone transaction.
        let mut tombstone_operations = Vec::with_capacity(keys.len() * 2);
        let mut tombstone_comparison = vec![];
        for (idx, key) in keys.iter().enumerate() {
            let filter = &mut filters[idx];
            let value = filter(&mut set).with_context(|| error::UnexpectedSnafu {
                err_msg: format!("Missing value, key: {}", key.get_inner()),
            })?;
            let (origin_key, tombstone_key) = TombstoneKey(key.get_inner()).into_keys();
            // Compares the atomic key.
            if key.is_atomic() {
                tombstone_comparison.push(Compare::with_value(
                    origin_key.clone(),
                    CompareOp::Equal,
                    value.clone(),
                ));
            }
            tombstone_operations.push(TxnOp::Delete(origin_key));
            tombstone_operations.push(TxnOp::Put(tombstone_key, value));
        }

        let txn = if !tombstone_comparison.is_empty() {
            Txn::new().when(tombstone_comparison)
        } else {
            Txn::new()
        }
        .and_then(tombstone_operations);

        let resp = self.kv_backend.txn(txn).await?;
        Ok(resp.succeeded)
    }

    /// Restores tombstones for keys.
    ///
    /// Preforms to:
    /// - retrieve all tombstone values corresponding `keys`.
    /// - stores tombstone values.
    pub(crate) async fn restore<T: TableMetaKey + TableMetaKeyGetTxnOp + Display>(
        &self,
        keys: Vec<Key<T>>,
    ) -> Result<bool> {
        // Builds transaction to retrieve all tombstone values
        let tombstone_keys = keys
            .iter()
            .map(|key| TombstoneKey(key.get_inner()))
            .collect::<Vec<_>>();
        let (operations, mut filters): (Vec<_>, Vec<_>) =
            tombstone_keys.iter().map(|key| key.build_get_op()).unzip();

        let txn = Txn::new().and_then(operations);
        let mut resp = self.kv_backend.txn(txn).await?;
        ensure!(
            resp.succeeded,
            error::UnexpectedSnafu {
                err_msg: "Failed to retrieves the metadata"
            }
        );

        let mut set = TxnOpGetResponseSet::from(&mut resp.responses);

        // Builds the restore tombstone transaction.
        let mut tombstone_operations = Vec::with_capacity(keys.len() * 2);
        let mut tombstone_comparison = vec![];
        for (idx, key) in keys.iter().enumerate() {
            let filter = &mut filters[idx];
            let value = filter(&mut set).with_context(|| error::UnexpectedSnafu {
                err_msg: format!("Missing value, key: {}", key.get_inner()),
            })?;
            let (origin_key, tombstone_key) = tombstone_keys[idx].to_keys();
            // Compares the atomic key.
            if key.is_atomic() {
                tombstone_comparison.push(Compare::with_value(
                    tombstone_key.clone(),
                    CompareOp::Equal,
                    value.clone(),
                ));
            }
            tombstone_operations.push(TxnOp::Delete(tombstone_key));
            tombstone_operations.push(TxnOp::Put(origin_key, value));
        }

        let txn = if !tombstone_comparison.is_empty() {
            Txn::new().when(tombstone_comparison)
        } else {
            Txn::new()
        }
        .and_then(tombstone_operations);

        let resp = self.kv_backend.txn(txn).await?;
        Ok(resp.succeeded)
    }

    /// Deletes tombstones for keys.
    pub(crate) async fn delete<T: TableMetaKey>(&self, keys: Vec<T>) -> Result<()> {
        let operations = keys
            .iter()
            .map(|key| TxnOp::Delete(TombstoneKey(key).to_tombstone_key()))
            .collect::<Vec<_>>();

        let txn = Txn::new().and_then(operations);
        // Always success.
        let _ = self.kv_backend.txn(txn).await?;
        Ok(())
    }
}
