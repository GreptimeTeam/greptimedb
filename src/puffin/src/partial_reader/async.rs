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
use std::ops::Range;

use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use common_base::range_read::{Metadata, RangeReader};

use crate::partial_reader::PartialReader;

#[async_trait]
impl<R: RangeReader> RangeReader for PartialReader<R> {
    async fn metadata(&self) -> io::Result<Metadata> {
        Ok(Metadata {
            content_length: self.size,
        })
    }

    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let absolute_range_start = self.offset + range.start;
        if absolute_range_start >= self.offset + self.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Start of range is out of bounds",
            ));
        }
        let absolute_range_end = (self.offset + range.end).min(self.offset + self.size);
        let absolute_range = absolute_range_start..absolute_range_end;

        let result = self.source.read(absolute_range.clone()).await?;
        Ok(result)
    }

    async fn read_into(&self, range: Range<u64>, buf: &mut (impl BufMut + Send)) -> io::Result<()> {
        let absolute_range_start = self.offset + range.start;
        if absolute_range_start >= self.offset + self.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Start of range is out of bounds",
            ));
        }
        let absolute_range_end = (self.offset + range.end).min(self.offset + self.size);
        let absolute_range = absolute_range_start..absolute_range_end;

        self.source.read_into(absolute_range.clone(), buf).await?;
        Ok(())
    }

    async fn read_vec(&self, ranges: &[Range<u64>]) -> io::Result<Vec<Bytes>> {
        let absolute_ranges = ranges
            .iter()
            .map(|range| {
                let start = self.offset + range.start;

                if start >= self.offset + self.size {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Start of range is out of bounds",
                    ));
                }

                let end = (self.offset + range.end).min(self.offset + self.size);
                Ok(start..end)
            })
            .collect::<io::Result<Vec<_>>>()?;

        let results = self.source.read_vec(&absolute_ranges).await?;

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_all_data_in_portion() {
        let data: Vec<u8> = (0..100).collect();
        let reader = PartialReader::new(data.clone(), 0, 100);
        let buf = reader.read(0..100).await.unwrap();
        assert_eq!(*buf, data);
    }

    #[tokio::test]
    async fn read_part_of_data_in_portion() {
        let data: Vec<u8> = (0..100).collect();
        let reader = PartialReader::new(data, 10, 30);
        let buf = reader.read(0..30).await.unwrap();
        assert_eq!(*buf, (10..40).collect::<Vec<u8>>());
    }

    #[tokio::test]
    async fn seek_past_end_of_portion_returns_error() {
        let data: Vec<u8> = (0..100).collect();
        let reader = PartialReader::new(data, 10, 30);
        // seeking past the portion returns an error
        assert!(reader.read(31..32).await.is_err());
    }

    #[tokio::test]
    async fn is_eof_returns_true_at_end_of_portion() {
        let data: Vec<u8> = (0..100).collect();
        let reader = PartialReader::new(data, 10, 30);
        let _ = reader.read(0..20).await.unwrap();
    }
}
