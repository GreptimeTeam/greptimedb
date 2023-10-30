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

use std::pin::Pin;
use std::sync::Arc;

use common_error::ext::BoxedError;
use futures::{stream, Stream, TryStreamExt};
use prost::Message;
use snafu::{ensure, Location, ResultExt};
use store_api::logstore::entry::{Entry, Id};
use store_api::logstore::LogStore;
use store_api::storage::{RegionId, SequenceNumber};

use crate::codec::{Decoder, Encoder};
use crate::error::{
    DecodeWalHeaderSnafu, DeleteWalNamespaceSnafu, EncodeWalHeaderSnafu, Error,
    MarkWalObsoleteSnafu, ReadWalSnafu, Result, WalDataCorruptedSnafu, WriteWalSnafu,
};
use crate::proto::wal::{self, WalHeader};
use crate::write_batch::codec::{PayloadDecoder, PayloadEncoder};
use crate::write_batch::Payload;

#[derive(Debug)]
pub struct Wal<S: LogStore> {
    region_id: RegionId,
    namespace: S::Namespace,
    store: Arc<S>,
}

pub type PayloadStream<'a> =
    Pin<Box<dyn Stream<Item = Result<(SequenceNumber, WalHeader, Option<Payload>)>> + Send + 'a>>;

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
        let namespace = store.namespace(region_id.into());
        Self {
            region_id,
            namespace,
            store,
        }
    }

    pub async fn obsolete(&self, seq: SequenceNumber) -> Result<()> {
        self.store
            .obsolete(self.namespace.clone(), seq)
            .await
            .map_err(BoxedError::new)
            .context(MarkWalObsoleteSnafu {
                region_id: self.region_id,
            })
    }

    pub async fn delete_namespace(&self) -> Result<()> {
        self.store
            .delete_namespace(&self.namespace)
            .await
            .map_err(BoxedError::new)
            .context(DeleteWalNamespaceSnafu {
                region_id: self.region_id,
            })
    }

    #[inline]
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    #[cfg(test)]
    pub async fn close(&self) -> Result<()> {
        let _ = self.store.stop().await;
        Ok(())
    }
}

impl<S: LogStore> Wal<S> {
    /// Data format:
    ///
    /// ```text
    /// |                                                                          |
    /// |-------------------------->    Header Len   <-----------------------------|          Arrow IPC format
    /// |                                                                          |
    /// v                                                                          v
    /// +---------------------+----------------------------------------------------+--------------+-------------+--------------+
    /// |                     |                       Header                       |              |             |              |
    /// | Header Len(varint)  |  (last_manifest_version + mutation_types + ...)    |  Payload 0   |  Payload 1  |     ...      |
    /// |                     |                                                    |              |             |              |
    /// +---------------------+----------------------------------------------------+--------------+-------------+--------------+
    /// ```
    ///
    pub async fn write_to_wal(
        &self,
        seq: SequenceNumber,
        mut header: WalHeader,
        payload: Option<&Payload>,
    ) -> Result<Id> {
        let _timer = crate::metrics::LOG_STORE_WRITE_ELAPSED.start_timer();
        if let Some(p) = payload {
            header.mutation_types = wal::gen_mutation_types(p);
        }

        let mut buf = vec![];

        // Encode header
        let wal_header_encoder = WalHeaderEncoder {};
        wal_header_encoder.encode(&header, &mut buf)?;

        // Encode payload
        if let Some(p) = payload {
            let encoder = PayloadEncoder::new();
            // TODO(jiachun): provide some way to compute data size before encode, so we can preallocate an exactly sized buf.
            encoder
                .encode(p, &mut buf)
                .map_err(BoxedError::new)
                .context(WriteWalSnafu {
                    region_id: self.region_id(),
                })?;
        }

        // write bytes to wal
        self.write(seq, &buf).await
    }

    pub async fn read_from_wal(&self, start_seq: SequenceNumber) -> Result<PayloadStream<'_>> {
        let stream = self
            .store
            .read(&self.namespace, start_seq)
            .await
            .map_err(BoxedError::new)
            .context(ReadWalSnafu {
                region_id: self.region_id(),
            })?
            // Handle the error when reading from the stream.
            .map_err(|e| Error::ReadWal {
                region_id: self.region_id(),
                source: BoxedError::new(e),
                location: Location::default(),
            })
            .and_then(|entries| async {
                let iter = entries.into_iter().map(|x| self.decode_entry(x));

                Ok(stream::iter(iter))
            })
            .try_flatten();

        Ok(Box::pin(stream))
    }

    async fn write(&self, seq: SequenceNumber, bytes: &[u8]) -> Result<u64> {
        let e = self.store.entry(bytes, seq, self.namespace.clone());

        let response = self
            .store
            .append(e)
            .await
            .map_err(BoxedError::new)
            .context(WriteWalSnafu {
                region_id: self.region_id(),
            })?;

        Ok(response.entry_id)
    }

    fn decode_entry<E: Entry>(
        &self,
        entry: E,
    ) -> Result<(SequenceNumber, WalHeader, Option<Payload>)> {
        let seq_num = entry.id();
        let input = entry.data();

        let wal_header_decoder = WalHeaderDecoder {};
        let (data_pos, header) = wal_header_decoder.decode(input)?;

        ensure!(
            data_pos <= input.len(),
            WalDataCorruptedSnafu {
                region_id: self.region_id(),
                message: format!(
                    "Not enough input buffer, expected data position={}, actual buffer length={}",
                    data_pos,
                    input.len()
                ),
            }
        );

        if header.mutation_types.is_empty() {
            return Ok((seq_num, header, None));
        }

        let decoder = PayloadDecoder::new(&header.mutation_types);
        let payload = decoder
            .decode(&input[data_pos..])
            .map_err(BoxedError::new)
            .context(ReadWalSnafu {
                region_id: self.region_id(),
            })?;

        Ok((seq_num, header, Some(payload)))
    }
}

pub struct WalHeaderEncoder {}

impl Encoder for WalHeaderEncoder {
    type Item = WalHeader;
    type Error = Error;

    fn encode(&self, item: &WalHeader, dst: &mut Vec<u8>) -> Result<()> {
        item.encode_length_delimited(dst)
            .map_err(|err| err.into())
            .context(EncodeWalHeaderSnafu)
    }
}

pub struct WalHeaderDecoder {}

impl Decoder for WalHeaderDecoder {
    type Item = (usize, WalHeader);
    type Error = Error;

    fn decode(&self, src: &[u8]) -> Result<(usize, WalHeader)> {
        let mut data_pos = prost::decode_length_delimiter(src)
            .map_err(|err| err.into())
            .context(DecodeWalHeaderSnafu)?;
        data_pos += prost::length_delimiter_len(data_pos);

        let wal_header = WalHeader::decode_length_delimited(src)
            .map_err(|err| err.into())
            .context(DecodeWalHeaderSnafu)?;

        Ok((data_pos, wal_header))
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use log_store::test_util;

    use super::*;

    #[tokio::test]
    pub async fn test_write_wal() {
        let log_file_dir = create_temp_dir("wal_test");
        let log_file_dir_path = log_file_dir.path().to_str().unwrap();
        let log_store =
            test_util::log_store_util::create_tmp_local_file_log_store(log_file_dir_path).await;
        let wal = Wal::new(RegionId::from(0), Arc::new(log_store));

        let res = wal.write(0, b"test1").await.unwrap();

        assert_eq!(0, res);
        let res = wal.write(1, b"test2").await.unwrap();
        assert_eq!(1, res);
    }

    #[tokio::test]
    pub async fn test_read_wal_only_header() -> Result<()> {
        common_telemetry::init_default_ut_logging();
        let log_file_dir = create_temp_dir("wal_test");
        let log_file_dir_path = log_file_dir.path().to_str().unwrap();
        let log_store =
            test_util::log_store_util::create_tmp_local_file_log_store(log_file_dir_path).await;
        let wal = Wal::new(RegionId::from(0), Arc::new(log_store));
        let header = WalHeader::with_last_manifest_version(111);
        let seq_num = 3;
        let _ = wal.write_to_wal(seq_num, header, None).await?;

        let mut stream = wal.read_from_wal(seq_num).await?;
        let mut data = vec![];
        while let Some((seq_num, header, write_batch)) = stream.try_next().await? {
            data.push((seq_num, header, write_batch));
        }
        assert_eq!(1, data.len());
        assert_eq!(111, data[0].1.last_manifest_version);
        assert!(data[0].2.is_none());

        Ok(())
    }

    #[test]
    pub fn test_wal_header_codec() {
        let wal_header = WalHeader {
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
