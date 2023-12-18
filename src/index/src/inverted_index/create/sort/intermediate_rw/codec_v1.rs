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

use std::io;

use asynchronous_codec::{BytesMut, Decoder, Encoder};
use bytes::{Buf, BufMut};
use common_base::BitVec;
use snafu::{location, Location};

use crate::inverted_index::error::{Error, Result};
use crate::inverted_index::Bytes;

const U64_LENGTH: usize = std::mem::size_of::<u64>();

/// Magic bytes for this intermediate codec version
pub const CODEC_V1_MAGIC: &[u8; 4] = b"im01";

/// Codec for serializing and deserializing intermediate data for external sorting.
///
/// Binary format serialization. The item is laid out as follows:
/// ```text
/// [value len][value][bitmap len][bitmap]
///     [8]     [?]       [8]       [?]
/// ```
pub struct IntermediateCodecV1;

/// [`FramedWrite`] requires the [`Encoder`] trait to be implemented.
impl Encoder for IntermediateCodecV1 {
    type Item<'a> = (Bytes, BitVec);
    type Error = Error;

    fn encode(&mut self, item: (Bytes, BitVec), dst: &mut BytesMut) -> Result<()> {
        let value_bytes = item.0;
        let bitmap_bytes = item.1.into_vec();

        dst.reserve(U64_LENGTH * 2 + value_bytes.len() + bitmap_bytes.len());
        dst.put_u64_le(value_bytes.len() as u64);
        dst.extend_from_slice(&value_bytes);
        dst.put_u64_le(bitmap_bytes.len() as u64);
        dst.extend_from_slice(&bitmap_bytes);
        Ok(())
    }
}

/// [`FramedRead`] requires the [`Decoder`] trait to be implemented.
impl Decoder for IntermediateCodecV1 {
    type Item = (Bytes, BitVec);
    type Error = Error;

    /// Decodes the `src` into `(Bytes, BitVec)`. Returns `None` if
    /// the `src` does not contain enough data for a complete item.
    ///
    /// Only after successful decoding, the `src` is advanced. Otherwise,
    /// it is left untouched to wait for filling more data and retrying.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        // [value len][value][bitmap len][bitmap]
        //     [8]     [?]       [8]       [?]

        // decode value len
        if src.len() < U64_LENGTH {
            return Ok(None);
        }
        let (value_len, buf) = src.split_at(U64_LENGTH);
        let value_len = u64::from_le_bytes(value_len.try_into().unwrap()) as usize;

        // decode value
        if buf.len() < value_len {
            return Ok(None);
        }
        let (value_bytes, buf) = buf.split_at(value_len);

        // decode bitmap len
        if buf.len() < U64_LENGTH {
            return Ok(None);
        }
        let (bitmap_len, buf) = buf.split_at(U64_LENGTH);
        let bitmap_len = u64::from_le_bytes(bitmap_len.try_into().unwrap()) as usize;

        // decode bitmap
        if buf.len() < bitmap_len {
            return Ok(None);
        }
        let bitmap_bytes = &buf[..bitmap_len];

        let item = (value_bytes.to_vec(), BitVec::from_slice(bitmap_bytes));

        src.advance(U64_LENGTH * 2 + value_len + bitmap_len);
        Ok(Some(item))
    }
}

/// Required for [`Encoder`] and [`Decoder`] implementations.
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::CommonIoError {
            error,
            location: location!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intermediate_codec_basic() {
        let mut codec = IntermediateCodecV1;
        let mut buf = BytesMut::new();

        let item = (b"hello".to_vec(), BitVec::from_slice(&[0b10101010]));
        codec.encode(item.clone(), &mut buf).unwrap();
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), item);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);

        let item1 = (b"world".to_vec(), BitVec::from_slice(&[0b01010101]));
        codec.encode(item.clone(), &mut buf).unwrap();
        codec.encode(item1.clone(), &mut buf).unwrap();
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), item);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), item1);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_intermediate_codec_empty_item() {
        let mut codec = IntermediateCodecV1;
        let mut buf = BytesMut::new();

        let item = (b"".to_vec(), BitVec::from_slice(&[]));
        codec.encode(item.clone(), &mut buf).unwrap();
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap(), item);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_intermediate_codec_partial() {
        let mut codec = IntermediateCodecV1;
        let mut buf = BytesMut::new();

        let item = (b"hello".to_vec(), BitVec::from_slice(&[0b10101010]));
        codec.encode(item.clone(), &mut buf).unwrap();

        let partial_length = U64_LENGTH + 3;
        let mut partial_bytes = buf.split_to(partial_length);

        assert_eq!(codec.decode(&mut partial_bytes).unwrap(), None); // not enough data
        partial_bytes.extend_from_slice(&buf[..]);
        assert_eq!(codec.decode(&mut partial_bytes).unwrap().unwrap(), item);
        assert_eq!(codec.decode(&mut partial_bytes).unwrap(), None);
        assert!(partial_bytes.is_empty());
    }
}
