use std::pin::Pin;
use std::sync::Arc;

use common_error::prelude::BoxedError;
use futures::{stream, Stream, TryStreamExt};
use prost::Message;
use snafu::{ensure, ResultExt};
use store_api::logstore::entry::Entry;
use store_api::logstore::{AppendResponse, LogStore};
use store_api::storage::{RegionId, SequenceNumber};

use crate::codec::{Decoder, Encoder};
use crate::error::{self, Error, Result};
use crate::proto::wal::{self, PayloadType, WalHeader};
use crate::write_batch::codec::{
    WriteBatchArrowDecoder, WriteBatchArrowEncoder, WriteBatchProtobufDecoder,
    WriteBatchProtobufEncoder,
};
use crate::write_batch::WriteBatch;

#[derive(Debug)]
pub struct Wal<S: LogStore> {
    region_id: RegionId,
    namespace: S::Namespace,
    store: Arc<S>,
}

pub type WriteBatchStream<'a> = Pin<
    Box<dyn Stream<Item = Result<(SequenceNumber, WalHeader, Option<WriteBatch>)>> + Send + 'a>,
>;

// Wal should be cheap to clone, so avoid holding things like String, Vec.
impl<S: LogStore> Clone for Wal<S> {
    fn clone(&self) -> Self {
        Self {
            region_id: self.region_id,
            namespace: self.namespace.clone(),
            store: self.store.clone(),
        }
    }
}

impl<S: LogStore> Wal<S> {
    pub fn new(region_id: RegionId, store: Arc<S>) -> Self {
        let namespace = store.namespace(region_id);
        Self {
            region_id,
            namespace,
            store,
        }
    }

    #[inline]
    pub fn region_id(&self) -> RegionId {
        self.region_id
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
    /// | Header Len(varint)  |  (last_manifest_version + mutation_types + ...)   | Data Chunk0  | Data Chunk1 |     ...      |
    /// |                     |                                                    |              |             |              |
    /// +---------------------+----------------------------------------------------+--------------+-------------+--------------+
    /// ```
    ///
    pub async fn write_to_wal(
        &self,
        seq: SequenceNumber,
        mut header: WalHeader,
        payload: Payload<'_>,
    ) -> Result<(u64, usize)> {
        header.payload_type = payload.payload_type();
        if let Payload::WriteBatchArrow(batch) = payload {
            header.mutation_types = wal::gen_mutation_types(batch);
        }

        let mut buf = vec![];

        // header
        let wal_header_encoder = WalHeaderEncoder {};
        wal_header_encoder.encode(&header, &mut buf)?;

        if let Payload::WriteBatchArrow(batch) = payload {
            // entry
            let encoder = WriteBatchArrowEncoder::new();
            // TODO(jiachun): provide some way to compute data size before encode, so we can preallocate an exactly sized buf.
            encoder
                .encode(batch, &mut buf)
                .map_err(BoxedError::new)
                .context(error::WriteWalSnafu {
                    region_id: self.region_id(),
                })?;
        } else if let Payload::WriteBatchProto(batch) = payload {
            // entry
            let encoder = WriteBatchProtobufEncoder {};
            // TODO(jiachun): provide some way to compute data size before encode, so we can preallocate an exactly sized buf.
            encoder
                .encode(batch, &mut buf)
                .map_err(BoxedError::new)
                .context(error::WriteWalSnafu {
                    region_id: self.region_id(),
                })?;
        }

        // write bytes to wal
        self.write(seq, &buf).await
    }

    pub async fn read_from_wal(&self, start_seq: SequenceNumber) -> Result<WriteBatchStream<'_>> {
        let stream = self
            .store
            .read(&self.namespace, start_seq)
            .await
            .map_err(BoxedError::new)
            .context(error::ReadWalSnafu {
                region_id: self.region_id(),
            })?
            // Handle the error when reading from the stream.
            .map_err(|e| Error::ReadWal {
                region_id: self.region_id(),
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
        let e = self.store.entry(bytes, seq, self.namespace.clone());

        let res = self
            .store
            .append(e)
            .await
            .map_err(BoxedError::new)
            .context(error::WriteWalSnafu {
                region_id: self.region_id(),
            })?;

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
                region_id: self.region_id(),
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
                let mutation_types = std::mem::take(&mut header.mutation_types);
                let decoder = WriteBatchArrowDecoder::new(mutation_types);
                let write_batch = decoder
                    .decode(&input[data_pos..])
                    .map_err(BoxedError::new)
                    .context(error::ReadWalSnafu {
                        region_id: self.region_id(),
                    })?;

                Ok((seq_num, header, Some(write_batch)))
            }
            Some(PayloadType::WriteBatchProto) => {
                let mutation_types = std::mem::take(&mut header.mutation_types);
                let decoder = WriteBatchProtobufDecoder::new(mutation_types);
                let write_batch = decoder
                    .decode(&input[data_pos..])
                    .map_err(BoxedError::new)
                    .context(error::ReadWalSnafu {
                        region_id: self.region_id(),
                    })?;

                Ok((seq_num, header, Some(write_batch)))
            }
            _ => error::WalDataCorruptedSnafu {
                region_id: self.region_id(),
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
        let wal = Wal::new(0, Arc::new(log_store));

        let res = wal.write(0, b"test1").await.unwrap();

        assert_eq!(0, res.0);
        assert_eq!(0, res.1);

        let res = wal.write(1, b"test2").await.unwrap();

        assert_eq!(1, res.0);
        assert_eq!(5 + 32, res.1);
    }

    #[tokio::test]
    pub async fn test_read_wal_only_header() -> Result<()> {
        common_telemetry::init_default_ut_logging();
        let (log_store, _tmp) =
            test_util::log_store_util::create_tmp_local_file_log_store("wal_test").await;
        let wal = Wal::new(0, Arc::new(log_store));
        let header = WalHeader::with_last_manifest_version(111);
        let (seq_num, _) = wal.write_to_wal(3, header, Payload::None).await?;

        assert_eq!(3, seq_num);

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
            mutation_types: vec![],
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
