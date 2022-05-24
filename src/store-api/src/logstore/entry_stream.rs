use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

use crate::logstore::entry::Entry;
use crate::logstore::Offset;

pub trait EntryStream: Stream<Item = Vec<Entry>> {
    fn start_offset(&self) -> Offset;
}

pub type SendableEntryStream<'a> = Pin<Box<dyn Stream<Item = Vec<Entry>> + Send + 'a>>;

pub struct EntryStreamImpl<'a> {
    inner: SendableEntryStream<'a>,
    start_offset: Offset,
}

impl<'a> EntryStream for EntryStreamImpl<'a> {
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

#[cfg(test)]
mod tests {

    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    pub async fn test_entry_stream() {
        let stream =
            async_stream::stream!({ yield vec![Entry::new("test_entry".as_bytes(), 0, 128)] });

        let mut stream_impl = EntryStreamImpl {
            inner: Box::pin(stream),
            start_offset: 1234,
        };

        if let Some(v) = stream_impl.next().await {
            assert_eq!(1, v.len());
            assert_eq!(b"test_entry", v[0].data());
        }
    }
}
