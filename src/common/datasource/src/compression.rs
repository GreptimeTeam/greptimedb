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

use std::fmt::Display;
use std::io;
use std::str::FromStr;

use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZstdDecoder};
use bytes::Bytes;
use futures::Stream;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::error::{self, Error, Result};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionType {
    /// Gzip-ed file
    Gzip,
    /// Bzip2-ed file
    Bzip2,
    /// Xz-ed file (liblzma)
    Xz,
    /// Zstd-ed file,
    Zstd,
    /// Uncompressed file
    Uncompressed,
}

impl FromStr for CompressionType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.to_uppercase();
        match s.as_str() {
            "GZIP" | "GZ" => Ok(Self::Gzip),
            "BZIP2" | "BZ2" => Ok(Self::Bzip2),
            "XZ" => Ok(Self::Xz),
            "ZST" | "ZSTD" => Ok(Self::Zstd),
            "" => Ok(Self::Uncompressed),
            _ => error::UnsupportedCompressionTypeSnafu {
                compression_type: s,
            }
            .fail(),
        }
    }
}

impl Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Gzip => "GZIP",
            Self::Bzip2 => "BZIP2",
            Self::Xz => "XZ",
            Self::Zstd => "ZSTD",
            Self::Uncompressed => "",
        })
    }
}

impl CompressionType {
    pub const fn is_compressed(&self) -> bool {
        !matches!(self, &Self::Uncompressed)
    }

    pub fn convert_async_read<T: AsyncRead + Unpin + Send + 'static>(
        &self,
        s: T,
    ) -> Box<dyn AsyncRead + Unpin + Send> {
        match self {
            CompressionType::Gzip => Box::new(GzipDecoder::new(BufReader::new(s))),
            CompressionType::Bzip2 => Box::new(BzDecoder::new(BufReader::new(s))),
            CompressionType::Xz => Box::new(XzDecoder::new(BufReader::new(s))),
            CompressionType::Zstd => Box::new(ZstdDecoder::new(BufReader::new(s))),
            CompressionType::Uncompressed => Box::new(s),
        }
    }

    pub fn convert_stream<T: Stream<Item = io::Result<Bytes>> + Unpin + Send + 'static>(
        &self,
        s: T,
    ) -> Box<dyn Stream<Item = io::Result<Bytes>> + Send + Unpin> {
        match self {
            CompressionType::Gzip => {
                Box::new(ReaderStream::new(GzipDecoder::new(StreamReader::new(s))))
            }
            CompressionType::Bzip2 => {
                Box::new(ReaderStream::new(BzDecoder::new(StreamReader::new(s))))
            }
            CompressionType::Xz => {
                Box::new(ReaderStream::new(XzDecoder::new(StreamReader::new(s))))
            }
            CompressionType::Zstd => {
                Box::new(ReaderStream::new(ZstdDecoder::new(StreamReader::new(s))))
            }
            CompressionType::Uncompressed => Box::new(s),
        }
    }
}
