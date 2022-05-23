use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;

use crate::wal::entry::Entry;
use crate::wal::namespace::NamespaceRef;
use crate::wal::Offset;

pub trait EntryStream: Stream<Item = Vec<Entry>> {
    fn namespace(&self) -> NamespaceRef;
    fn start_offset(&self) -> Offset;
}

pub type PinnedEntryStream<'a> = Pin<Box<dyn Stream<Item = Vec<Entry>> + 'a>>;

pub type EntryStreamRef<'a> = Arc<dyn EntryStream + 'a>;

pub struct EntryStreamImpl<'a> {
    inner: PinnedEntryStream<'a>,
    start_offset: Offset,
    namespace: NamespaceRef,
}

impl<'a> EntryStream for EntryStreamImpl<'a> {
    fn namespace(&self) -> NamespaceRef {
        self.namespace.clone()
    }

    fn start_offset(&self) -> Offset {
        self.start_offset
    }
}

impl Stream for EntryStreamImpl<'_> {
    type Item = Vec<Entry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(v)) => Poll::Ready(Some(v)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
