use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use common_base::buffer::{Buffer, BufferMut};
use futures::Stream;
use snafu::{ensure, ResultExt};
use store_api::logstore::entry::{Encode, Entry, Epoch, Id, Offset};
use store_api::logstore::entry_stream::{EntryStream, SendableEntryStream};

use crate::error::{CorruptedSnafu, DecodeSnafu, EncodeSnafu, Error};
use crate::fs::crc;

// length+offset+epoch+crc
const ENTRY_MIN_LEN: usize = 4 + 8 + 8 + 4;

#[derive(Debug, PartialEq, Clone)]
pub struct EntryImpl {
    pub data: Vec<u8>,
    pub offset: Offset,
    pub id: Id,
    pub epoch: Epoch,
}

impl Encode for EntryImpl {
    type Error = Error;

    /// Entry binary format (Little endian):
    ///
    /// +--------+--------+--------+--------+--------+
    //  |entry id|  epoch | length |  data  |  CRC   |
    //  +--------+--------+--------+--------+--------+
    //  | 8 bytes| 8 bytes| 4 bytes|<length>| 4 bytes|
    //  +--------+--------+--------+--------+--------+
    ///
    fn encode_to<T: BufferMut>(&self, buf: &mut T) -> Result<usize, Self::Error> {
        let data_length = self.data.len();
        buf.write_u64_le(self.id).context(EncodeSnafu)?;
        buf.write_u64_le(self.epoch).context(EncodeSnafu)?;
        buf.write_u32_le(data_length as u32).context(EncodeSnafu)?;
        buf.write_from_slice(self.data.as_slice())
            .context(EncodeSnafu)?;
        let checksum = crc::CRC_ALGO.checksum(buf.as_slice());
        buf.write_u32_le(checksum).context(EncodeSnafu)?;
        Ok(data_length + ENTRY_MIN_LEN)
    }

    fn decode<T: Buffer>(buf: &mut T) -> Result<Self, Self::Error> {
        ensure!(
            buf.remaining_size() >= ENTRY_MIN_LEN,
            DecodeSnafu {
                size: buf.remaining_size(),
            }
        );

        macro_rules! map_err {
            ($stmt: expr, $var: ident) => {
                $stmt.map_err(|_| {
                    DecodeSnafu {
                        size: $var.remaining_size(),
                    }
                    .build()
                })
            };
        }

        let mut digest = crc::CRC_ALGO.digest();
        let id = map_err!(buf.read_u64_le(), buf)?;
        digest.update(&id.to_le_bytes());
        let epoch = map_err!(buf.read_u64_le(), buf)?;
        digest.update(&epoch.to_le_bytes());
        let data_len = map_err!(buf.read_u32_le(), buf)?;
        digest.update(&data_len.to_le_bytes());
        ensure!(
            buf.remaining_size() >= data_len as usize,
            DecodeSnafu {
                size: buf.remaining_size()
            }
        );
        let mut data = vec![0u8; data_len as usize];
        map_err!(buf.read_to_slice(&mut data), buf)?;
        digest.update(&data);
        let crc_read = map_err!(buf.read_u32_le(), buf)?;
        let crc_calc = digest.finalize();
        ensure!(
            crc_read == crc_calc,
            CorruptedSnafu {
                msg: format!(
                    "CRC mismatch while decoding entry, read: {}, calc: {}",
                    hex::encode_upper(crc_read.to_le_bytes()),
                    hex::encode_upper(crc_calc.to_le_bytes())
                )
            }
        );

        Ok(Self {
            id,
            data,
            epoch,
            offset: 0,
        })
    }

    fn encoded_size(&self) -> usize {
        self.data.len() + ENTRY_MIN_LEN
    }
}

impl Entry for EntryImpl {
    type Error = Error;

    fn new(data: impl AsRef<[u8]>) -> Self {
        Self {
            id: 0,
            data: data.as_ref().to_vec(),
            offset: 0,
            epoch: 0,
        }
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn id(&self) -> Id {
        self.id
    }

    fn offset(&self) -> Offset {
        self.offset
    }

    fn set_offset(&mut self, offset: Offset) {
        self.offset = offset;
    }

    fn set_id(&mut self, id: Id) {
        self.id = id;
    }

    fn epoch(&self) -> Epoch {
        self.epoch
    }

    fn len(&self) -> usize {
        ENTRY_MIN_LEN + self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl TryFrom<Bytes> for EntryImpl {
    type Error = Error;

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        EntryImpl::decode(&mut value)
    }
}

impl From<&EntryImpl> for BytesMut {
    fn from(e: &EntryImpl) -> Self {
        let size = e.encoded_size();
        let mut res = BytesMut::with_capacity(size);
        e.encode_to(&mut res).unwrap(); // buffer is pre-allocated, so won't fail
        res
    }
}

pub struct StreamImpl<'a> {
    pub inner: SendableEntryStream<'a, EntryImpl, Error>,
    pub start_entry_id: Id,
}

impl<'a> Stream for StreamImpl<'a> {
    type Item = Result<Vec<EntryImpl>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<'a> EntryStream for StreamImpl<'a> {
    type Error = Error;
    type Entry = EntryImpl;

    fn start_id(&self) -> u64 {
        self.start_entry_id
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{ByteOrder, LittleEndian};

    use super::*;
    use crate::fs::crc::CRC_ALGO;

    #[test]
    pub fn test_entry_deser() {
        let data = "hello, world";
        let mut entry = EntryImpl::new(data.as_bytes());
        entry.set_id(8);
        entry.epoch = 9;
        let mut buf = BytesMut::with_capacity(entry.encoded_size());
        entry.encode_to(&mut buf).unwrap();
        assert_eq!(ENTRY_MIN_LEN + data.as_bytes().len(), buf.len());
        let decoded: EntryImpl = EntryImpl::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    pub fn test_rewrite_entry_id() {
        let data = "hello, world";
        let mut entry = EntryImpl::new(data.as_bytes());
        let mut buffer = BytesMut::with_capacity(entry.encoded_size());
        entry.encode_to(&mut buffer).unwrap();
        entry.set_id(123);
        assert_eq!(123, entry.id());

        // rewrite entry id.
        LittleEndian::write_u64(&mut buffer[0..8], 333);
        let len = buffer.len();
        let checksum = CRC_ALGO.checksum(&buffer[0..len - 4]);
        LittleEndian::write_u32(&mut buffer[len - 4..], checksum);

        let entry_impl = EntryImpl::decode(&mut buffer.freeze()).expect("Failed to deserialize");
        assert_eq!(333, entry_impl.id());
    }
}
