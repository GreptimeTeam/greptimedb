use std::pin::Pin;
use std::sync::Arc;

use common_error::prelude::BoxedError;
use futures::{stream, Stream, TryStreamExt};
use prost::Message;
use snafu::{ensure, ResultExt};
use store_api::{
    logstore::{entry::Entry, namespace::Namespace, AppendResponse, LogStore},
    storage::SequenceNumber,
};

use crate::{
    codec::{Decoder, Encoder},
    error::{self, Error, Result},
    proto::{self, PayloadType, WalHeader},
    write_batch::{
        codec::{WriteBatchArrowDecoder, WriteBatchArrowEncoder},
        WriteBatch,
    },
};

#[derive(Debug)]
pub struct Wal<S: LogStore> {
    namespace: S::Namespace,
    store: Arc<S>,
}

pub type WriteBatchStream<'a> = Pin<
    Box<dyn Stream<Item = Result<(SequenceNumber, WalHeader, Option<WriteBatch>)>> + Send + 'a>,
>;

// wal should be cheap to clone
impl<S: LogStore> Clone for Wal<S> {
    fn clone(&self) -> Self {
        Self {
            namespace: self.namespace.clone(),
            store: self.store.clone(),
        }
    }
}

impl<S: LogStore> Wal<S> {
    pub fn new(region_name: impl Into<String>, store: Arc<S>) -> Self {
        let region_name = region_name.into();
        let namespace = store.namespace(&region_name);

        Self { namespace, store }
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.namespace.name()
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
        seq: SequenceNumber,
        mut header: WalHeader,
        payload: Payload<'a>,
    ) -> Result<(u64, usize)> {
        header.payload_type = payload.payload_type();

        if let Payload::WriteBatchArrow(batch) = payload {
            header.mutation_extras = proto::gen_mutation_extras(batch);
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
                .context(error::WriteWalSnafu { name: self.name() })?;
        }

        // TODO(jiachun): encode protobuf payload

        // write bytes to wal
        self.write(seq, &buf).await
    }

    pub async fn read_from_wal(&self, start_seq: SequenceNumber) -> Result<WriteBatchStream<'_>> {
        let stream = self
            .store
            .read(&self.namespace, start_seq)
            .await
            .map_err(BoxedError::new)
            .context(error::ReadWalSnafu { name: self.name() })?
            .map_err(|e| Error::ReadWal {
                name: self.name().to_string(),
                source: BoxedError::new(e),
            })
            .and_then(|entries| async {
                let iter = entries.into_iter().map(|x| self.decode_entry(x));

                Ok(stream::iter(iter))
            })
            .try_flatten();

        Ok(Box::pin(stream))
    }

    async fn write(&self, seq: SequenceNumber, bytes: &[u8]) -> Result<(u64, usize)> {
        let mut e = self.store.entry(bytes);
        e.set_id(seq);

        let res = self
            .store
            .append(&self.namespace, e)
            .await
            .map_err(BoxedError::new)
            .context(error::WriteWalSnafu { name: self.name() })?;

        Ok((res.entry_id(), res.offset()))
    }

    fn decode_entry<E: Entry>(
        &self,
        entry: E,
    ) -> Result<(SequenceNumber, WalHeader, Option<WriteBatch>)> {
        let seq_num = entry.id();
        let input = entry.data();

        let wal_header_decoder = WalHeaderDecoder {};
        let (data_pos, mut header) = wal_header_decoder.decode(input)?;

        ensure!(
            data_pos <= input.len(),
            error::WalDataCorruptedSnafu {
                name: self.name(),
                message: format!(
                    "Not enough input buffer, expected data position={}, actual buffer length={}",
                    data_pos,
                    input.len()
                ),
            }
        );

        match PayloadType::from_i32(header.payload_type) {
            Some(PayloadType::None) => Ok((seq_num, header, None)),
            Some(PayloadType::WriteBatchArrow) => {
                let mutation_extras = std::mem::take(&mut header.mutation_extras);
                let decoder = WriteBatchArrowDecoder::new(mutation_extras);
                let write_batch = decoder
                    .decode(&input[data_pos..])
                    .map_err(BoxedError::new)
                    .context(error::ReadWalSnafu { name: self.name() })?;

                Ok((seq_num, header, Some(write_batch)))
            }
            Some(PayloadType::WriteBatchProto) => {
                todo!("protobuf decoder")
            }
            _ => error::WalDataCorruptedSnafu {
                name: self.name(),
                message: format!("invalid payload type={}", header.payload_type),
            }
            .fail(),
        }
    }
}

pub enum Payload<'a> {
    None, // only header
    WriteBatchArrow(&'a WriteBatch),
    #[allow(dead_code)]
    WriteBatchProto(&'a WriteBatch),
}

impl<'a> Payload<'a> {
    pub fn payload_type(&self) -> i32 {
        match self {
            Payload::None => PayloadType::None.into(),
            Payload::WriteBatchArrow(_) => PayloadType::WriteBatchArrow.into(),
            Payload::WriteBatchProto(_) => PayloadType::WriteBatchProto.into(),
        }
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
    type Item = (usize, WalHeader);
    type Error = Error;

    fn decode(&self, src: &[u8]) -> Result<(usize, WalHeader)> {
        let mut data_pos = prost::decode_length_delimiter(src)
            .map_err(|err| err.into())
            .context(error::DecodeWalHeaderSnafu)?;
        data_pos += prost::length_delimiter_len(data_pos);

        let wal_header = WalHeader::decode_length_delimited(src)
            .map_err(|err| err.into())
            .context(error::DecodeWalHeaderSnafu)?;

        Ok((data_pos, wal_header))
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

        let res = wal.write(0, b"test1").await.unwrap();

        assert_eq!(0, res.0);
        assert_eq!(0, res.1);

        let res = wal.write(1, b"test2").await.unwrap();

        assert_eq!(1, res.0);
        assert_eq!(29, res.1);
    }

    #[tokio::test]
    pub async fn test_read_wal_only_header() -> Result<()> {
        let (log_store, _tmp) =
            test_util::log_store_util::create_tmp_local_file_log_store("wal_test").await;
        let wal = Wal::new("test_region", Arc::new(log_store));
        let header = WalHeader::with_last_manifest_version(111);
        let (seq_num, _) = wal.write_to_wal(3, header, Payload::None).await?;

        assert_eq!(0, seq_num);

        let mut stream = wal.read_from_wal(seq_num).await?;
        let mut data = vec![];
        while let Some((seq_num, header, write_batch)) = stream.try_next().await? {
            data.push((seq_num, header, write_batch));
        }

        assert_eq!(1, data.len());
        assert_eq!(seq_num, data[0].0);
        assert_eq!(111, data[0].1.last_manifest_version);
        assert!(data[0].2.is_none());

        Ok(())
    }

    #[test]
    pub fn test_wal_header_codec() {
        let wal_header = WalHeader {
            payload_type: 1,
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

        let data_pos = res.0;
        assert_eq!(buf.len() - 3, data_pos);
    }
}
