use std::pin::Pin;

use futures::Stream;

use crate::logstore::entry::Entry;
use crate::logstore::Offset;

pub trait EntryStream: Stream<Item = Vec<Self::Entry>> {
    type Entry: Entry;
    fn start_offset(&self) -> Offset;
}

pub type SendableEntryStream<'a, E> = Pin<Box<dyn Stream<Item = Vec<E>> + Send + 'a>>;

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use futures::StreamExt;

    use super::*;
    use crate::logstore::entry::Epoch;

    pub struct SimpleEntry {
        /// Offset of current entry
        offset: Offset,
        /// Epoch of current entry
        epoch: Epoch,
        /// Binary data of current entry
        data: Vec<u8>,
    }

    impl Entry for SimpleEntry {
        fn data(&self) -> &[u8] {
            self.data.as_slice()
        }

        fn offset(&self) -> Offset {
            self.offset
        }

        fn epoch(&self) -> Epoch {
            self.epoch
        }
    }

    impl SimpleEntry {
        pub fn new(data: impl AsRef<[u8]>, offset: Offset, epoch: u64) -> Self {
            let data = data.as_ref().to_vec();
            Self {
                data,
                offset,
                epoch,
            }
        }
    }

    pub struct EntryStreamImpl<'a> {
        inner: SendableEntryStream<'a, SimpleEntry>,
        start_offset: Offset,
    }

    impl<'a> EntryStream for EntryStreamImpl<'a> {
        type Entry = SimpleEntry;

        fn start_offset(&self) -> Offset {
            self.start_offset
        }
    }

    impl Stream for EntryStreamImpl<'_> {
        type Item = Vec<SimpleEntry>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(v)) => Poll::Ready(Some(v)),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    #[tokio::test]
    pub async fn test_entry_stream() {
        let stream = async_stream::stream!({
            yield vec![SimpleEntry::new("test_entry".as_bytes(), 0, 128)]
        });

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
