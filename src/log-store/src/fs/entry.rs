use std::pin::Pin;
use std::task::{Context, Poll};

use byteorder::{ByteOrder, LittleEndian};
use futures::Stream;
use snafu::{ensure, Backtrace, GenerateImplicitData};
use store_api::logstore::entry::{Entry, Epoch, Id, Offset};
use store_api::logstore::entry_stream::{EntryStream, SendableEntryStream};

use crate::error::{DeserializationSnafu, Error};
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

impl Entry for EntryImpl {
    type Error = Error;

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

    fn serialize(&self) -> Vec<u8> {
        let res: Vec<u8> = self.into();
        res
    }

    fn deserialize(b: impl AsRef<[u8]>) -> Result<Self, Self::Error> {
        EntryImpl::try_from(b.as_ref())
    }
}

impl EntryImpl {
    pub fn new(data: impl AsRef<[u8]>) -> Self {
        let data = Vec::from(data.as_ref());
        Self {
            id: 0,
            data,
            offset: 0,
            epoch: 0,
        }
    }
}

/// Entry binary format (Little endian):
///
/// +--------+--------+--------+--------+--------+
//  |entry id|  epoch | length |  data  |  CRC   |
//  +--------+--------+--------+--------+--------+
//  | 8 bytes| 8 bytes| 4 bytes|<length>| 4 bytes|
//  +--------+--------+--------+--------+--------+
///
impl TryFrom<&[u8]> for EntryImpl {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        ensure!(value.len() >= ENTRY_MIN_LEN, DeserializationSnafu);

        // TODO(hl): will use byteorder to simplify encoding/decoding.
        let id_end_ofs = 8;
        let epoch_end_ofs = id_end_ofs + 8;
        let length_end_offset = epoch_end_ofs + 4;
        let length = LittleEndian::read_u32(&value[epoch_end_ofs..length_end_offset]);
        let data_end_ofs = length_end_offset + length as usize;
        let crc_end_ofs = data_end_ofs + 4;
        let data = Vec::from(&value[length_end_offset..data_end_ofs]);
        let id = LittleEndian::read_u64(&value[0..id_end_ofs]);
        let epoch = LittleEndian::read_u64(&value[id_end_ofs..epoch_end_ofs]);
        let crc_read = LittleEndian::read_u32(&value[data_end_ofs..crc_end_ofs]);

        // TODO(hl): add a config option to turn off CRC checksum.
        let crc_calc = crc::CRC_ALGO.checksum(&value[0..data_end_ofs]);
        if crc_calc != crc_read {
            return Err(Error::Corrupted {
                msg: format!("CRC mismatch, read: {}, calc: {}", crc_read, crc_calc),
                backtrace: Backtrace::generate(),
            });
        }

        Ok(Self {
            data,
            offset: 0usize,
            id,
            epoch,
        })
    }
}

impl From<&EntryImpl> for Vec<u8> {
    fn from(e: &EntryImpl) -> Self {
        let data_length = e.data.len();
        let total_size = data_length + ENTRY_MIN_LEN;
        let mut vec = vec![0u8; total_size];

        let buf = vec.as_mut_slice();

        let id_end_ofs = 8;
        let epoch_end_ofs = id_end_ofs + 8;
        let length_end_offset = epoch_end_ofs + 4;
        let data_end_ofs = length_end_offset + data_length as usize;
        let crc_end_ofs = data_end_ofs + 4;

        LittleEndian::write_u64(buf, e.id);
        LittleEndian::write_u64(&mut buf[id_end_ofs..epoch_end_ofs], e.epoch);
        LittleEndian::write_u32(
            &mut buf[epoch_end_ofs..length_end_offset],
            data_length as u32,
        ); // todo check this cast

        buf[length_end_offset..data_end_ofs].copy_from_slice(e.data.as_slice());
        let checksum = crc::CRC_ALGO.checksum(&buf[0..data_end_ofs]);
        LittleEndian::write_u32(&mut buf[data_end_ofs..crc_end_ofs], checksum);
        vec
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
    use super::*;
    use crate::fs::crc::CRC_ALGO;

    #[test]
    pub fn test_entry_deser() {
        let data = "hello, world";
        let entry = EntryImpl::new(data.as_bytes());
        let vec: Vec<u8> = (&entry).into();
        assert_eq!(ENTRY_MIN_LEN + data.as_bytes().len(), vec.len());
        let deserialized = EntryImpl::try_from(vec.as_slice()).unwrap();
        assert_eq!(entry, deserialized);
    }

    #[test]
    pub fn test_rewrite_entry_id() {
        let data = "hello, world";
        let mut entry = EntryImpl::new(data.as_bytes());
        let mut vec: Vec<u8> = (&entry).into();
        entry.set_id(123);
        assert_eq!(123, entry.id());

        // rewrite entry id.
        LittleEndian::write_u64(&mut vec[0..8], 333);
        let len = vec.len();
        let checksum = CRC_ALGO.checksum(&vec[0..len - 4]);
        LittleEndian::write_u32(&mut vec[len - 4..], checksum);

        let entry_impl = EntryImpl::deserialize(&vec).expect("Failed to deserialize");
        assert_eq!(333, entry_impl.id());
    }
}
