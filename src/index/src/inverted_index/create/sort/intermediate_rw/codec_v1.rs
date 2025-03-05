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
use greptime_proto::v1::index::BitmapType;
use snafu::ResultExt;

use crate::bitmap::Bitmap;
use crate::inverted_index::error::{CommonIoSnafu, Error, Result};
use crate::Bytes;

const U64_LENGTH: usize = std::mem::size_of::<u64>();

/// Magic bytes for this intermediate codec version
pub const CODEC_V1_MAGIC: &[u8; 4] = b"im01";

/// Serializes items of external sorting intermediate files.
pub struct IntermediateItemEncoderV1 {
    pub bitmap_type: BitmapType,
}

/// [`FramedWrite`] requires the [`Encoder`] trait to be implemented.
impl Encoder for IntermediateItemEncoderV1 {
    type Item<'a> = (Bytes, Bitmap);
    type Error = Error;

    fn encode(&mut self, item: (Bytes, Bitmap), dst: &mut BytesMut) -> Result<()> {
        let value_bytes = item.0;
        let bitmap_size = item.1.serialized_size(self.bitmap_type);

        dst.reserve(U64_LENGTH * 2 + value_bytes.len() + bitmap_size);
        dst.put_u64_le(value_bytes.len() as u64);
        dst.extend_from_slice(&value_bytes);
        dst.put_u64_le(bitmap_size as u64);
        item.1
            .serialize_into(self.bitmap_type, &mut dst.writer())
            .context(CommonIoSnafu)?;

        Ok(())
    }
}

/// Deserializes items of external sorting intermediate files.
pub struct IntermediateItemDecoderV1 {
    pub bitmap_type: BitmapType,
}

/// [`FramedRead`] requires the [`Decoder`] trait to be implemented.
impl Decoder for IntermediateItemDecoderV1 {
    type Item = (Bytes, Bitmap);
    type Error = Error;

    /// Decodes the `src` into `(Bytes, RoaringBitmap)`. Returns `None` if
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

        let bitmap = Bitmap::deserialize_from(&buf[..bitmap_len], self.bitmap_type)
            .context(CommonIoSnafu)?;

        let item = (value_bytes.to_vec(), bitmap);

        src.advance(U64_LENGTH * 2 + value_len + bitmap_len);
        Ok(Some(item))
    }
}

/// Required for [`Encoder`] and [`Decoder`] implementations.
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Err::<(), io::Error>(error)
            .context(CommonIoSnafu)
            .unwrap_err()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap(bytes: &[u8]) -> Bitmap {
        Bitmap::from_lsb0_bytes(bytes, BitmapType::Roaring)
    }

    #[test]
    fn test_intermediate_codec_basic() {
        let mut encoder = IntermediateItemEncoderV1 {
            bitmap_type: BitmapType::Roaring,
        };
        let mut buf = BytesMut::new();

        let item = (b"hello".to_vec(), bitmap(&[0b10101010]));
        encoder.encode(item.clone(), &mut buf).unwrap();

        let mut decoder = IntermediateItemDecoderV1 {
            bitmap_type: BitmapType::Roaring,
        };
        assert_eq!(decoder.decode(&mut buf).unwrap().unwrap(), item);
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);

        let item1 = (b"world".to_vec(), bitmap(&[0b01010101]));
        encoder.encode(item.clone(), &mut buf).unwrap();
        encoder.encode(item1.clone(), &mut buf).unwrap();
        assert_eq!(decoder.decode(&mut buf).unwrap().unwrap(), item);
        assert_eq!(decoder.decode(&mut buf).unwrap().unwrap(), item1);
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_intermediate_codec_empty_item() {
        let mut encoder = IntermediateItemEncoderV1 {
            bitmap_type: BitmapType::Roaring,
        };
        let mut buf = BytesMut::new();

        let item = (b"".to_vec(), bitmap(&[]));
        encoder.encode(item.clone(), &mut buf).unwrap();

        let mut decoder = IntermediateItemDecoderV1 {
            bitmap_type: BitmapType::Roaring,
        };
        assert_eq!(decoder.decode(&mut buf).unwrap().unwrap(), item);
        assert_eq!(decoder.decode(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_intermediate_codec_partial() {
        let mut encoder = IntermediateItemEncoderV1 {
            bitmap_type: BitmapType::Roaring,
        };
        let mut buf = BytesMut::new();

        let item = (b"hello".to_vec(), bitmap(&[0b10101010]));
        encoder.encode(item.clone(), &mut buf).unwrap();

        let partial_length = U64_LENGTH + 3;
        let mut partial_bytes = buf.split_to(partial_length);

        let mut decoder = IntermediateItemDecoderV1 {
            bitmap_type: BitmapType::Roaring,
        };
        assert_eq!(decoder.decode(&mut partial_bytes).unwrap(), None); // not enough data
        partial_bytes.extend_from_slice(&buf[..]);
        assert_eq!(decoder.decode(&mut partial_bytes).unwrap().unwrap(), item);
        assert_eq!(decoder.decode(&mut partial_bytes).unwrap(), None);
        assert!(partial_bytes.is_empty());
    }
}
