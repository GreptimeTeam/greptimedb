use std::pin::Pin;

use common_error::prelude::ErrorExt;
use futures::Stream;

use crate::logstore::entry::Entry;

pub trait EntryStream: Stream<Item = Result<Vec<Self::Entry>, Self::Error>> {
    type Error: ErrorExt;
    type Entry: Entry;
    fn start_id(&self) -> u64;
}

pub type SendableEntryStream<'a, I, E> = Pin<Box<dyn Stream<Item = Result<Vec<I>, E>> + Send + 'a>>;

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::task::{Context, Poll};

    use futures::StreamExt;

    use super::*;
    use crate::logstore::entry::{Epoch, Id, Offset};

    pub struct SimpleEntry {
        /// Offset of current entry
        offset: Offset,
        /// Epoch of current entry
        epoch: Epoch,
        /// Binary data of current entry
        data: Vec<u8>,
    }

    use common_error::prelude::{ErrorExt, Snafu};
    use snafu::{Backtrace, ErrorCompat};

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display("Failed to deserialize entry"))]
        Deserialization,
        #[snafu(display("Entry corrupted"))]
        Corrupted,
    }

    impl ErrorExt for Error {
        fn backtrace_opt(&self) -> Option<&Backtrace> {
            ErrorCompat::backtrace(self)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl Entry for SimpleEntry {
        type Error = Error;

        fn data(&self) -> &[u8] {
            self.data.as_slice()
        }

        fn id(&self) -> Id {
            0u64
        }

        fn offset(&self) -> Offset {
            self.offset
        }

        fn set_offset(&mut self, _offset: Offset) {}

        fn set_id(&mut self, _id: Id) {}

        fn epoch(&self) -> Epoch {
            self.epoch
        }

        fn len(&self) -> usize {
            self.data.len()
        }

        fn is_empty(&self) -> bool {
            self.data.is_empty()
        }

        fn serialize(&self) -> Vec<u8> {
            self.data.clone()
        }

        fn deserialize(_b: impl AsRef<[u8]>) -> Result<Self, Self::Error> {
            unimplemented!()
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
        inner: SendableEntryStream<'a, SimpleEntry, Error>,
        start_id: u64,
    }

    impl<'a> EntryStream for EntryStreamImpl<'a> {
        type Error = Error;
        type Entry = SimpleEntry;

        fn start_id(&self) -> u64 {
            self.start_id
        }
    }

    impl Stream for EntryStreamImpl<'_> {
        type Item = Result<Vec<SimpleEntry>, Error>;

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
            yield Ok(vec![SimpleEntry::new("test_entry".as_bytes(), 0, 128)])
        });

        let mut stream_impl = EntryStreamImpl {
            inner: Box::pin(stream),
            start_id: 1234,
        };

        if let Some(v) = stream_impl.next().await {
            let vec = v.unwrap();
            assert_eq!(1, vec.len());
            assert_eq!(b"test_entry", vec[0].data());
        }
    }
}
