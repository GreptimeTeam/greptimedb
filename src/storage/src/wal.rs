use std::sync::Arc;

use common_error::prelude::BoxedError;
use prost::Message;
use snafu::ResultExt;
use store_api::logstore::{entry::Entry, namespace::Namespace, AppendResponse, LogStore};

use crate::{
    codec::{Decoder, Encoder},
    error::{self, Error, Result},
    proto::{self, *},
    write_batch::{WriteBatch, WriteBatchArrowEncoder},
};

#[derive(Clone)]
pub struct Wal<S> {
    region: String,
    store: Arc<S>,
}

impl<S> Wal<S> {
    pub fn new(region: impl Into<String>, store: Arc<S>) -> Self {
        Self {
            region: region.into(),
            store,
        }
    }

    pub fn region(&self) -> &str {
        &self.region
    }
}

impl<S: LogStore> Wal<S> {
    /// Data format:
    ///
    /// |                                                                                  |
    /// |--------------------------------->   Header Len    <------------------------------|                    Arrow encoded
    /// |                                                                                  |
    /// v                                                                                  v
    /// +---------------------+------------------------------------------------------------+--------------+-------------+--------------+
    /// |                     |                                                            |              |             |              |
    /// | Header Len(varint)  | Header(last_manifest_version + mutation_type + null_mask)  | Data Chunk0  | Data Chunk1 |     ...      |
    /// |                     |                                                            |              |             |              |
    /// +---------------------+------------------------------------------------------------+--------------+-------------+--------------+
    ///
    pub async fn write_to_wal(
        &self,
        mut header: WalHeader,
        batch: &WriteBatch,
    ) -> Result<(u64, usize)> {
        header.mutation_extras = proto::gen_mutation_extras(batch);

        let mut buf = vec![];

        // header
        let wal_header_encoder = WalHeaderEncoder {};
        wal_header_encoder.encode(&header, &mut buf)?;

        // entry
        let encoder = WriteBatchArrowEncoder::new(header.mutation_extras.clone());
        encoder
            .encode(batch, &mut buf)
            .map_err(BoxedError::new)
            .context(error::WriteWalSnafu {
                region: self.region(),
            })?;

        // write to wal
        self.write(&buf).await
    }

    async fn write(&self, bytes: &[u8]) -> Result<(u64, usize)> {
        // TODO(jiachun): region id
        let ns = S::Namespace::new(&self.region, 0);
        let e = S::Entry::new(bytes);

        let res = self
            .store
            .append(ns, e)
            .await
            .map_err(BoxedError::new)
            .context(error::WriteWalSnafu {
                region: &self.region,
            })?;

        Ok((res.entry_id(), res.offset()))
    }
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
    type Item = WalHeader;
    type Error = Error;

    fn decode(&self, src: &[u8]) -> Result<Option<WalHeader>> {
        let mut data_pos = prost::decode_length_delimiter(src)
            .map_err(|err| err.into())
            .context(error::DecodeWalHeaderSnafu)?;
        data_pos += prost::length_delimiter_len(data_pos);

        let mut wal_header = WalHeader::decode_length_delimited(src)
            .map_err(|err| err.into())
            .context(error::DecodeWalHeaderSnafu)?;
        wal_header.data_pos = data_pos as u64;

        Ok(Some(wal_header))
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
        let wal = Wal::new("test_region", Arc::new(log_store));

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
            data_pos: 0,
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

        let wal_header_encoded = res.unwrap();
        assert_eq!(buf.len() - 3, wal_header_encoded.data_pos as usize);
    }
}
