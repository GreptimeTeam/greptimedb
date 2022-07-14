use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use common_base::buffer::{Buffer, BufferMut};
use futures::Stream;
use snafu::{ensure, ResultExt};
use store_api::logstore::entry::{Encode, Entry, Epoch, Id, Offset};
use store_api::logstore::entry_stream::{EntryStream, SendableEntryStream};
use store_api::logstore::namespace::{Id as NamespaceId, Namespace};

use crate::error::{CorruptedSnafu, DecodeAgainSnafu, DecodeSnafu, EncodeSnafu, Error};
use crate::fs::crc;
use crate::fs::namespace::LocalNamespace;

// length + offset + namespace id + epoch + crc
const ENTRY_MIN_LEN: usize = HEADER_LENGTH + 4;
// length + offset + namespace id + epoch
const HEADER_LENGTH: usize = 4 + 8 + 8 + 8;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EntryImpl {
    pub data: Vec<u8>,
    pub offset: Offset,
    pub id: Id,
    pub namespace_id: NamespaceId,
    pub epoch: Epoch,
}

impl EntryImpl {
    #[cfg(test)]
    fn set_offset(&mut self, offset: Offset) {
        self.offset = offset;
    }
}

impl Encode for EntryImpl {
    type Error = Error;

    /// Entry binary format (Little endian):
    ///
    //  ```text
    //  +--------+--------------+-------+--------+--------+--------+
    //  |entry id| namespace id | epoch | length |  data  |  CRC   |
    //  +--------+--------------+-------+--------+--------+--------+
    //  | 8 bytes|    8 bytes   |8 bytes| 4 bytes|<length>| 4 bytes|
    //  +--------+--------------+-------+--------+--------+--------+
    // ```
    ///
    fn encode_to<T: BufferMut>(&self, buf: &mut T) -> Result<usize, Self::Error> {
        let data_length = self.data.len();
        buf.write_u64_le(self.id).context(EncodeSnafu)?;
        buf.write_u64_le(self.namespace_id).context(EncodeSnafu)?;
        buf.write_u64_le(self.epoch).context(EncodeSnafu)?;
        buf.write_u32_le(data_length as u32).context(EncodeSnafu)?;
        buf.write_from_slice(self.data.as_slice())
            .context(EncodeSnafu)?;
        let checksum = crc::CRC_ALGO.checksum(buf.as_slice());
        buf.write_u32_le(checksum).context(EncodeSnafu)?;
        Ok(data_length + ENTRY_MIN_LEN)
    }

    fn decode<T: Buffer>(buf: &mut T) -> Result<Self, Self::Error> {
        ensure!(buf.remaining_size() >= HEADER_LENGTH, DecodeAgainSnafu);

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
        let mut header = [0u8; HEADER_LENGTH];
        buf.peek_to_slice(&mut header).unwrap();

        let mut header = &header[..];
        let id = header.read_u64_le().unwrap(); // unwrap here is safe because header bytes must be present
        digest.update(&id.to_le_bytes());

        let namespace_id = header.read_u64_le().unwrap();
        digest.update(&namespace_id.to_le_bytes());

        let epoch = header.read_u64_le().unwrap();
        digest.update(&epoch.to_le_bytes());

        let data_len = header.read_u32_le().unwrap();
        digest.update(&data_len.to_le_bytes());

        ensure!(
            buf.remaining_size() >= ENTRY_MIN_LEN + data_len as usize,
            DecodeAgainSnafu
        );

        buf.advance_by(HEADER_LENGTH);

        let mut data = vec![0u8; data_len as usize];
        map_err!(buf.peek_to_slice(&mut data), buf)?;
        digest.update(&data);
        buf.advance_by(data_len as usize);

        let crc_read = map_err!(buf.peek_u32_le(), buf)?;
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

        buf.advance_by(4);

        Ok(Self {
            id,
            data,
            epoch,
            offset: 0,
            namespace_id,
        })
    }

    fn encoded_size(&self) -> usize {
        self.data.len() + ENTRY_MIN_LEN
    }
}

impl EntryImpl {
    pub(crate) fn new(data: impl AsRef<[u8]>, id: Id, namespace: LocalNamespace) -> EntryImpl {
        EntryImpl {
            id,
            data: data.as_ref().to_vec(),
            offset: 0,
            epoch: 0,
            namespace_id: namespace.id(),
        }
    }
}

impl Entry for EntryImpl {
    type Error = Error;
    type Namespace = LocalNamespace;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn id(&self) -> Id {
        self.id
    }

    fn offset(&self) -> Offset {
        self.offset
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

    fn namespace(&self) -> Self::Namespace {
        LocalNamespace::new(self.namespace_id)
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
    use async_stream::stream;
    use byteorder::{ByteOrder, LittleEndian};
    use futures::pin_mut;
    use futures_util::StreamExt;
    use tokio::time::Duration;

    use super::*;
    use crate::fs::chunk::{Chunk, ChunkList};
    use crate::fs::crc::CRC_ALGO;

    #[test]
    pub fn test_entry_deser() {
        let data = "hello, world";
        let mut entry = EntryImpl::new(data.as_bytes(), 8, LocalNamespace::new(42));
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
        let entry = EntryImpl::new(data.as_bytes(), 123, LocalNamespace::new(42));
        let mut buffer = BytesMut::with_capacity(entry.encoded_size());
        entry.encode_to(&mut buffer).unwrap();
        assert_eq!(123, entry.id());

        // rewrite entry id.
        LittleEndian::write_u64(&mut buffer[0..8], 333);
        let len = buffer.len();
        let checksum = CRC_ALGO.checksum(&buffer[0..len - 4]);
        LittleEndian::write_u32(&mut buffer[len - 4..], checksum);

        let entry_impl = EntryImpl::decode(&mut buffer.freeze()).expect("Failed to deserialize");
        assert_eq!(333, entry_impl.id());
    }

    fn prepare_entry_bytes(data: &str, id: Id) -> Bytes {
        let mut entry = EntryImpl::new(data.as_bytes(), id, LocalNamespace::new(42));
        entry.set_id(123);
        entry.set_offset(456);
        let mut buffer = BytesMut::with_capacity(entry.encoded_size());
        entry.encode_to(&mut buffer).unwrap();
        let len = buffer.len();
        let checksum = CRC_ALGO.checksum(&buffer[0..len - 4]);
        LittleEndian::write_u32(&mut buffer[len - 4..], checksum);
        buffer.freeze()
    }

    /// Test decode entry from a composite buffer.
    #[test]
    pub fn test_composite_buffer() {
        let data_1 = "hello, world";
        let bytes = prepare_entry_bytes(data_1, 0);
        EntryImpl::decode(&mut bytes.clone()).unwrap();
        let c1 = Chunk::copy_from_slice(&bytes);

        let data_2 = "LoremIpsumDolor";
        let bytes = prepare_entry_bytes(data_2, 1);
        EntryImpl::decode(&mut bytes.clone()).unwrap();
        let c2 = Chunk::copy_from_slice(&bytes);

        let mut chunks = ChunkList::new();
        chunks.push(c1);
        chunks.push(c2);

        assert_eq!(
            ENTRY_MIN_LEN * 2 + data_2.len() + data_1.len(),
            chunks.remaining_size()
        );

        let mut decoded = vec![];
        while chunks.remaining_size() > 0 {
            let entry_impl = EntryImpl::decode(&mut chunks).unwrap();
            decoded.push(entry_impl.data);
        }

        assert_eq!(
            vec![data_1.as_bytes().to_vec(), data_2.as_bytes().to_vec()],
            decoded
        );
    }

    // split an encoded entry to two different chunk and try decode from this composite chunk
    #[test]
    pub fn test_decode_split_data_from_composite_chunk() {
        let data = "hello, world";
        let bytes = prepare_entry_bytes(data, 42);
        assert_eq!(
            hex::decode("7B000000000000002A0000000000000000000000000000000C00000068656C6C6F2C20776F726C64E8EE2E57")
                .unwrap()
                .as_slice(),
            &bytes[..]
        );
        let original = EntryImpl::decode(&mut bytes.clone()).unwrap();
        let split_point = bytes.len() / 2;
        let (left, right) = bytes.split_at(split_point);

        let mut chunks = ChunkList::new();
        chunks.push(Chunk::copy_from_slice(left));
        chunks.push(Chunk::copy_from_slice(right));

        assert_eq!(bytes.len(), chunks.remaining_size());
        let decoded = EntryImpl::decode(&mut chunks).unwrap();
        assert_eq!(original, decoded);
    }

    // Tests decode entry from encoded entry data as two chunks
    #[tokio::test]
    pub async fn test_decode_from_chunk_stream() {
        // prepare entry
        let data = "hello, world";
        let bytes = prepare_entry_bytes(data, 42);
        assert_eq!(
            hex::decode("7b000000000000002a0000000000000000000000000000000c00000068656c6c6f2c20776f726c64e8ee2e57")
                .unwrap()
                .as_slice(),
            &bytes[..]
        );
        let original = EntryImpl::decode(&mut bytes.clone()).unwrap();
        let split_point = bytes.len() / 2;
        let (left, right) = bytes.split_at(split_point);

        // prepare chunk stream
        let chunk_stream = stream!({
            yield Chunk::copy_from_slice(left);
            tokio::time::sleep(Duration::from_millis(10)).await;
            yield Chunk::copy_from_slice(right);
        });

        pin_mut!(chunk_stream);

        let mut chunks = ChunkList::new();
        let mut decoded = vec![];
        while let Some(c) = chunk_stream.next().await {
            chunks.push(c);
            match EntryImpl::decode(&mut chunks) {
                Ok(e) => {
                    decoded.push(e);
                }
                Err(Error::DecodeAgain { .. }) => {
                    continue;
                }
                _ => {
                    panic!()
                }
            }
        }
        assert_eq!(1, decoded.len());
        assert_eq!(original, decoded.into_iter().next().unwrap());
    }
}
