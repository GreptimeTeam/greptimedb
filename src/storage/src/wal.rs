use std::sync::Arc;

use common_error::prelude::BoxedError;
use prost::Message;
use snafu::ResultExt;
use store_api::logstore::{entry::Entry, namespace::Namespace, AppendResponse, LogStore};

use crate::{
    codec::{Decoder, Encoder},
    error::{self, Error, Result},
    proto::{self, PayloadType, WalHeader},
    write_batch::{codec::WriteBatchArrowEncoder, WriteBatch},
};

pub struct Wal<S> {
    region_id: u32,
    region_name: String,
    store: Arc<S>,
}

impl<S> Clone for Wal<S> {
    fn clone(&self) -> Self {
        Self {
            region_id: self.region_id,
            region_name: self.region_name.clone(),
            store: self.store.clone(),
        }
    }
}

impl<S> Wal<S> {
    pub fn new(region_id: u32, region_name: impl Into<String>, store: Arc<S>) -> Self {
        Self {
            region_id,
            region_name: region_name.into(),
            store,
        }
    }

    #[inline]
    pub fn region_id(&self) -> u32 {
        self.region_id
    }

    #[inline]
    pub fn region_name(&self) -> &str {
        &self.region_name
    }
}

impl<S: LogStore> Wal<S> {
    /// Data format:
    ///
    /// ```text
    /// |                                                                          |
    /// |-------------------------->    Header Len   <-----------------------------|          Arrow/Protobuf/... encoded
    /// |                                                                          |
    /// v                                                                          v
    /// +---------------------+----------------------------------------------------+--------------+-------------+--------------+
    /// |                     |                       Header                       |              |             |              |
    /// | Header Len(varint)  |  (last_manifest_version + mutation_extras + ...)   | Data Chunk0  | Data Chunk1 |     ...      |
    /// |                     |                                                    |              |             |              |
    /// +---------------------+----------------------------------------------------+--------------+-------------+--------------+
    /// ```
    ///
    pub async fn write_to_wal<'a>(
        &self,
        mut header: WalHeader,
        payload: Payload<'a>,
    ) -> Result<(u64, usize)> {
        match payload {
            Payload::None => header.payload_type = PayloadType::None.into(),
            Payload::WriteBatchArrow(batch) => {
                header.payload_type = PayloadType::WriteBatchArrow.into();
                header.mutation_extras = proto::gen_mutation_extras(batch);
            }
            Payload::WriteBatchProto(_) => {
                header.payload_type = PayloadType::WriteBatchProto.into()
            }
        }

        let mut buf = vec![];

        // header
        let wal_header_encoder = WalHeaderEncoder {};
        wal_header_encoder.encode(&header, &mut buf)?;

        if let Payload::WriteBatchArrow(batch) = payload {
            // entry
            let encoder = WriteBatchArrowEncoder::new(header.mutation_extras);
            // TODO(jiachun): provide some way to compute data size before encode, so we can preallocate an exactly sized buf.
            encoder
                .encode(batch, &mut buf)
                .map_err(BoxedError::new)
                .context(error::WriteWalSnafu {
                    region_id: self.region_id(),
                    region_name: self.region_name(),
                })?;
        }

        // TODO(jiachun): encode protobuf payload

        // write bytes to wal
        self.write(&buf).await
    }

    async fn write(&self, bytes: &[u8]) -> Result<(u64, usize)> {
        let ns = S::Namespace::new(&self.region_name, self.region_id as u64);
        let e = S::Entry::new(bytes);

        let res = self
            .store
            .append(ns, e)
            .await
            .map_err(BoxedError::new)
            .context(error::WriteWalSnafu {
                region_id: self.region_id(),
                region_name: self.region_name(),
            })?;

        Ok((res.entry_id(), res.offset()))
    }
}

pub enum Payload<'a> {
    None, // only header
    WriteBatchArrow(&'a WriteBatch),
    WriteBatchProto(&'a WriteBatch),
}

pub struct WalHeaderEncoder {}

impl Encoder for WalHeaderEncoder {
    type Item = WalHeader;
    type Error = Error;

    fn encode(&self, item: &WalHeader, dst: &mut Vec<u8>) -> Result<()> {
        item.encode_length_delimited(dst)
            .map_err(|err| err.into())
            .context(error::EncodeWalHeaderSnafu)
    }
}

pub struct WalHeaderDecoder {}

impl Decoder for WalHeaderDecoder {
    type Item = (usize, WalHeader);
    type Error = Error;

    fn decode(&self, src: &[u8]) -> Result<Option<(usize, WalHeader)>> {
        let mut data_pos = prost::decode_length_delimiter(src)
            .map_err(|err| err.into())
            .context(error::DecodeWalHeaderSnafu)?;
        data_pos += prost::length_delimiter_len(data_pos);

        let wal_header = WalHeader::decode_length_delimited(src)
            .map_err(|err| err.into())
            .context(error::DecodeWalHeaderSnafu)?;

        Ok(Some((data_pos, wal_header)))
    }
}

#[cfg(test)]
mod tests {
    use log_store::test_util;

    use super::*;

    #[tokio::test]
    pub async fn test_write_wal() {
        let (log_store, _tmp) =
            test_util::log_store_util::create_tmp_local_file_log_store("wal_test").await;
        let wal = Wal::new(0, "test_region", Arc::new(log_store));

        let res = wal.write(b"test1").await.unwrap();

        assert_eq!(0, res.0);
        assert_eq!(0, res.1);

        let res = wal.write(b"test2").await.unwrap();

        assert_eq!(1, res.0);
        assert_eq!(29, res.1);
    }

    #[test]
    pub fn test_wal_header_codec() {
        let wal_header = WalHeader {
            last_manifest_version: 99999999,
            mutation_extras: vec![],
        };

        let mut buf: Vec<u8> = vec![];
        let wal_encoder = WalHeaderEncoder {};
        wal_encoder.encode(&wal_header, &mut buf).unwrap();

        buf.push(1u8); // data
        buf.push(2u8); // data
        buf.push(3u8); // data

        let decoder = WalHeaderDecoder {};
        let res = decoder.decode(&buf).unwrap();

        assert!(res.is_some());

        let data_pos = res.unwrap().0;
        assert_eq!(buf.len() - 3, data_pos);
    }
}
