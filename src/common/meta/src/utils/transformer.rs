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

mod reallocate_region_wal_options;

pub use reallocate_region_wal_options::RellocateRegionWalOptions;
use snafu::ensure;

use crate::error::{self, Result};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;

/// Represents possible actions for metadata.
#[derive(Debug, Clone)]
pub enum MetadataAction {
    /// No operation, used as a placeholder.
    Noop,
    /// Represents a transaction-based modification to the metadata.
    Transform(Txn),
}

#[cfg(test)]
impl MetadataAction {
    /// Converts a [`MetadataAction`] into a [`Txn`]
    ///
    /// # Panics
    /// This method panics if the [`MetadataAction`] is not [`MetadataAction::Transform`].
    pub fn into_transform(self) -> Txn {
        let Self::Transform(txn) = self else {
            unreachable!()
        };

        txn
    }
}

/// A trait for processing key-value pairs to generate [`MetadataAction`].
#[async_trait::async_trait]
pub trait MetadataTransformer: Sync + Send {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    fn accept(&self, key: &[u8]) -> bool;

    async fn handle(&self, key: Vec<u8>, value: Vec<u8>) -> Result<MetadataAction>;
}

/// A trait for applying [`MetadataAction`] to a backend.
#[async_trait::async_trait]
pub trait MetadataApplier: Sync + Send {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    fn accept(&self, action: &MetadataAction) -> bool;

    async fn handle(&self, action: MetadataAction) -> Result<()>;
}

#[derive(Debug, Default)]
/// The noop executor
pub struct NoopMetadataApplier;

#[async_trait::async_trait]
impl MetadataApplier for NoopMetadataApplier {
    fn accept(&self, _action: &MetadataAction) -> bool {
        true
    }

    async fn handle(&self, _action: MetadataAction) -> Result<()> {
        Ok(())
    }
}

/// Used to apply [`MetadataAction`] to the [`KvBackendRef`].
pub struct KvBackendMetadataApplier {
    kv_backend: KvBackendRef,
}

impl KvBackendMetadataApplier {
    /// Creates the [`KvBackendMetadataApplier`].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }
}

#[async_trait::async_trait]
impl MetadataApplier for KvBackendMetadataApplier {
    fn accept(&self, action: &MetadataAction) -> bool {
        matches!(action, MetadataAction::Transform(..))
    }

    async fn handle(&self, action: MetadataAction) -> Result<()> {
        // Safety: checked before
        let MetadataAction::Transform(txn) = action else {
            return error::UnexpectedSnafu {
                err_msg: "expected 'MetadataAction::Modify'",
            }
            .fail();
        };

        let resp = self.kv_backend.txn(txn).await?;
        ensure!(
            resp.succeeded,
            error::UnexpectedSnafu {
                err_msg: "Failed to exeuctor transaction"
            }
        );

        Ok(())
    }
}

/// A processor that handles metadata transformation and application.
pub struct MetadataProcessor {
    transformers: Vec<Box<dyn MetadataTransformer>>,
    applier: Box<dyn MetadataApplier>,
}

impl MetadataProcessor {
    /// Creates a new `MetadataProcessor`.
    pub fn new(
        transformers: Vec<Box<dyn MetadataTransformer>>,
        applier: Box<dyn MetadataApplier>,
    ) -> Self {
        Self {
            transformers,
            applier,
        }
    }

    /// Finds the first acceptable transformer for the given key.
    fn find_acceptable_transformer(&self, key: &[u8]) -> Option<&dyn MetadataTransformer> {
        self.transformers
            .iter()
            .find(|t| t.accept(key))
            .map(|v| v.as_ref())
    }

    /// Processes a key-value pair using the appropriate transformer and applies the action.
    pub async fn handle(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if let Some(transformer) = self.find_acceptable_transformer(&key) {
            let action = transformer.handle(key, value).await?;
            if self.applier.accept(&action) {
                self.applier.handle(action).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use super::KvBackendMetadataApplier;
    use crate::error;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::txn::Txn;
    use crate::kv_backend::KvBackend;
    use crate::utils::transformer::{MetadataAction, MetadataApplier};

    #[test]
    fn test_accept() {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let modifier = KvBackendMetadataApplier::new(kv_backend);
        assert!(!modifier.accept(&MetadataAction::Noop));
        assert!(modifier.accept(&MetadataAction::Transform(Txn::new())));
    }

    #[tokio::test]
    async fn test_handle() {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let modifier = KvBackendMetadataApplier::new(kv_backend.clone());
        let txn = Txn::put_if_not_exists("hi".as_bytes().to_vec(), "foobar".as_bytes().to_vec());
        let action = MetadataAction::Transform(txn);
        modifier.handle(action.clone()).await.unwrap();
        let kv = kv_backend.get("hi".as_bytes()).await.unwrap().unwrap();
        assert_eq!(&kv.value, "foobar".as_bytes());

        let err = modifier.handle(action.clone()).await.unwrap_err();
        assert_matches!(err, error::Error::Unexpected { .. });
    }
}
