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

use common_config::wal::WalOptions;
use store_api::logstore::entry::{Entry, Id as EntryId};
use store_api::logstore::namespace::{Id as NamespaceId, Namespace};
use store_api::logstore::{AppendBatchResponse, AppendResponse, LogStore};

use crate::error::{Error, Result};

/// A noop log store which only for test
#[derive(Debug, Default)]
pub struct NoopLogStore;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct EntryImpl;

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct NamespaceImpl;

impl Namespace for NamespaceImpl {
    fn id(&self) -> NamespaceId {
        0
    }
}

impl Entry for EntryImpl {
    type Error = Error;
    type Namespace = NamespaceImpl;

    fn data(&self) -> &[u8] {
        &[]
    }

    fn id(&self) -> EntryId {
        0
    }

    fn namespace(&self) -> Self::Namespace {
        Default::default()
    }
}

#[async_trait::async_trait]
impl LogStore for NoopLogStore {
    type Error = Error;
    type Namespace = NamespaceImpl;
    type Entry = EntryImpl;

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    async fn append(&self, mut _e: Self::Entry) -> Result<AppendResponse> {
        Ok(AppendResponse::default())
    }

    async fn append_batch(&self, _e: Vec<Self::Entry>) -> Result<AppendBatchResponse> {
        Ok(AppendBatchResponse::default())
    }

    async fn read(
        &self,
        _ns: &Self::Namespace,
        _entry_id: EntryId,
    ) -> Result<store_api::logstore::entry_stream::SendableEntryStream<'_, Self::Entry, Self::Error>>
    {
        Ok(Box::pin(futures::stream::once(futures::future::ready(Ok(
            vec![],
        )))))
    }

    async fn create_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    async fn delete_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
        Ok(vec![])
    }

    fn entry<D: AsRef<[u8]>>(
        &self,
        data: D,
        entry_id: EntryId,
        ns: Self::Namespace,
    ) -> Self::Entry {
        let _ = data;
        let _ = entry_id;
        let _ = ns;
        EntryImpl
    }

    fn namespace(&self, ns_id: NamespaceId, wal_options: &WalOptions) -> Self::Namespace {
        let _ = ns_id;
        let _ = wal_options;
        NamespaceImpl
    }

    async fn obsolete(
        &self,
        ns: Self::Namespace,
        entry_id: EntryId,
    ) -> std::result::Result<(), Self::Error> {
        let _ = ns;
        let _ = entry_id;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_entry() {
        let e = EntryImpl;
        assert_eq!(0, e.data().len());
        assert_eq!(0, e.id());
    }

    #[tokio::test]
    async fn test_noop_logstore() {
        let store = NoopLogStore;
        let e = store.entry("".as_bytes(), 1, NamespaceImpl);
        let _ = store.append(e.clone()).await.unwrap();
        assert!(store.append_batch(vec![e]).await.is_ok());
        store.create_namespace(&NamespaceImpl).await.unwrap();
        assert_eq!(0, store.list_namespaces().await.unwrap().len());
        store.delete_namespace(&NamespaceImpl).await.unwrap();
        assert_eq!(NamespaceImpl, store.namespace(0, &WalOptions::default()));
        store.obsolete(NamespaceImpl, 1).await.unwrap();
    }
}
