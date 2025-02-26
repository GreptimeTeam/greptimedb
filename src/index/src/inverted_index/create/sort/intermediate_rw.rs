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

//! Intermediate codec for external sorting.
//!
//! This module provides serialization and deserialization logic for
//! handling intermediate data during the sorting process.
//! The serialization format is as follows:
//!
//! ```text
//! [magic][item][item]...[item]
//!    [4]       [?]
//!
//! Each [item] is structured as:
//! [value len][value][bitmap len][bitmap]
//!     [8]       [?]       [8]        [?]
//! ```
//!
//! Each item represents a value and its associated bitmap, serialized with their lengths for
//! easier deserialization.

mod codec_v1;

use asynchronous_codec::{FramedRead, FramedWrite};
use futures::{stream, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, StreamExt};
use snafu::ResultExt;

use crate::inverted_index::create::sort::SortedStream;
use crate::inverted_index::error::{
    CloseSnafu, FlushSnafu, ReadSnafu, Result, UnknownIntermediateCodecMagicSnafu, WriteSnafu,
};
use crate::Bytes;

/// `IntermediateWriter` serializes and writes intermediate data to the wrapped `writer`
pub struct IntermediateWriter<W> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> IntermediateWriter<W> {
    /// Creates a new `IntermediateWriter` wrapping an `AsyncWrite`
    pub fn new(writer: W) -> IntermediateWriter<W> {
        IntermediateWriter { writer }
    }

    /// Serializes and writes all provided values to the wrapped writer
    pub async fn write_all(
        mut self,
        values: impl IntoIterator<Item = (Bytes, roaring::RoaringBitmap)>,
    ) -> Result<()> {
        let (codec_magic, encoder) = (
            codec_v1::CODEC_V1_MAGIC,
            codec_v1::IntermediateItemEncoderV1,
        );

        self.writer
            .write_all(codec_magic)
            .await
            .context(WriteSnafu)?;

        let value_stream = stream::iter(values.into_iter().map(Ok));
        let frame_write = FramedWrite::new(&mut self.writer, encoder);
        // `forward()` will flush and close the writer when the stream ends
        if let Err(e) = value_stream.forward(frame_write).await {
            self.writer.flush().await.context(FlushSnafu)?;
            self.writer.close().await.context(CloseSnafu)?;
            return Err(e);
        }

        Ok(())
    }
}

/// Reads intermediate serialized data from an `AsyncRead` source and converts it to a [`SortedStream`]
pub struct IntermediateReader<R> {
    reader: R,
}

impl<R: AsyncRead + Unpin + Send + 'static> IntermediateReader<R> {
    pub fn new(reader: R) -> IntermediateReader<R> {
        IntermediateReader { reader }
    }

    /// Reads the magic header, determines the codec, and returns a stream of deserialized values.
    pub async fn into_stream(mut self) -> Result<SortedStream> {
        let mut magic = [0u8; 4];
        self.reader
            .read_exact(&mut magic)
            .await
            .context(ReadSnafu)?;

        let decoder = match &magic {
            codec_v1::CODEC_V1_MAGIC => codec_v1::IntermediateItemDecoderV1,
            _ => return UnknownIntermediateCodecMagicSnafu { magic }.fail(),
        };

        Ok(Box::new(FramedRead::new(self.reader, decoder)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Seek, SeekFrom};

    use futures::io::{AllowStdIo, Cursor};
    use tempfile::tempfile;

    use super::*;
    use crate::inverted_index::error::Error;

    fn bitmap(bytes: &[u8]) -> roaring::RoaringBitmap {
        roaring::RoaringBitmap::from_lsb0_bytes(0, bytes)
    }

    #[tokio::test]
    async fn test_intermediate_read_write_basic() {
        let file_r = tempfile().unwrap();
        let file_w = file_r.try_clone().unwrap();
        let mut buf_r = AllowStdIo::new(file_r);
        let buf_w = AllowStdIo::new(file_w);

        let values = BTreeMap::from_iter([
            (Bytes::from("a"), bitmap(&[0b10101010])),
            (Bytes::from("b"), bitmap(&[0b01010101])),
        ]);

        let writer = IntermediateWriter::new(buf_w);
        writer.write_all(values.clone()).await.unwrap();
        // reset the handle
        buf_r.seek(SeekFrom::Start(0)).unwrap();

        let reader = IntermediateReader::new(buf_r);
        let mut stream = reader.into_stream().await.unwrap();

        let a = stream.next().await.unwrap().unwrap();
        assert_eq!(a, (Bytes::from("a"), bitmap(&[0b10101010])));
        let b = stream.next().await.unwrap().unwrap();
        assert_eq!(b, (Bytes::from("b"), bitmap(&[0b01010101])));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_intermediate_read_write_empty() {
        let mut buf = vec![];

        let values = BTreeMap::new();

        let writer = IntermediateWriter::new(&mut buf);
        writer.write_all(values.clone()).await.unwrap();

        let reader = IntermediateReader::new(Cursor::new(buf));
        let mut stream = reader.into_stream().await.unwrap();

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_intermediate_read_with_invalid_magic() {
        let buf = b"invalid".to_vec();

        let reader = IntermediateReader::new(Cursor::new(buf));
        let result = reader.into_stream().await;
        assert!(matches!(
            result,
            Err(Error::UnknownIntermediateCodecMagic { .. })
        ))
    }
}
