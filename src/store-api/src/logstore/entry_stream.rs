// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    use crate::logstore::entry::{Encode, Epoch, Id, Offset};

    pub struct SimpleEntry {
        /// Offset of current entry
        offset: Offset,
        /// Epoch of current entry
        epoch: Epoch,
        /// Binary data of current entry
        data: Vec<u8>,
    }

    use common_base::buffer::{Buffer, BufferMut};
    use common_error::prelude::{ErrorExt, Snafu};
    use snafu::{Backtrace, ErrorCompat};

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub struct Error {}

    #[derive(Debug, Clone)]
    pub struct Namespace {}

    impl crate::logstore::Namespace for Namespace {
        fn id(&self) -> crate::logstore::namespace::Id {
            0
        }
    }

    impl ErrorExt for Error {
        fn backtrace_opt(&self) -> Option<&Backtrace> {
            ErrorCompat::backtrace(self)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl Encode for SimpleEntry {
        type Error = Error;

        fn encode_to<T: BufferMut>(&self, buf: &mut T) -> Result<usize, Self::Error> {
            buf.write_from_slice(self.data.as_slice()).unwrap();
            Ok(self.data.as_slice().len())
        }

        fn decode<T: Buffer>(_buf: &mut T) -> Result<Self, Self::Error> {
            unimplemented!()
        }

        fn encoded_size(&self) -> usize {
            self.data.as_slice().len()
        }
    }

    impl Entry for SimpleEntry {
        type Error = Error;
        type Namespace = Namespace;

        fn data(&self) -> &[u8] {
            &self.data
        }

        fn id(&self) -> Id {
            0u64
        }

        fn offset(&self) -> Offset {
            self.offset
        }

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

        fn namespace(&self) -> Self::Namespace {
            Namespace {}
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
