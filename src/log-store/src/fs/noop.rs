use store_api::logstore::{entry::Id, LogStore};

use crate::error::{Error, Result};
use crate::fs::{entry::EntryImpl, namespace::LocalNamespace, AppendResponseImpl};

/// Only for test
#[derive(Default)]
pub struct NoopLogStore {}

#[async_trait::async_trait]
impl LogStore for NoopLogStore {
    type Error = Error;
    type Namespace = LocalNamespace;
    type Entry = EntryImpl;
    type AppendResponse = AppendResponseImpl;

    async fn append(
        &self,
        _ns: Self::Namespace,
        mut _e: Self::Entry,
    ) -> Result<Self::AppendResponse> {
        Ok(AppendResponseImpl {
            entry_id: 0,
            offset: 0,
        })
    }

    async fn append_batch(&self, _ns: Self::Namespace, _e: Vec<Self::Entry>) -> Result<Id> {
        todo!()
    }

    async fn read(
        &self,
        _ns: Self::Namespace,
        _id: Id,
    ) -> Result<store_api::logstore::entry_stream::SendableEntryStream<'_, Self::Entry, Self::Error>>
    {
        todo!()
    }

    async fn create_namespace(&mut self, _ns: Self::Namespace) -> Result<()> {
        todo!()
    }

    async fn delete_namespace(&mut self, _ns: Self::Namespace) -> Result<()> {
        todo!()
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
        todo!()
    }
}
