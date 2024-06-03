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
use async_compression::tokio::write;
use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use strum::EnumIter;
use tokio::io::{AsyncRead, AsyncWriteExt, BufReader};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::error::{self, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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

    pub const fn file_extension(&self) -> &'static str {
        match self {
            Self::Gzip => "gz",
            Self::Bzip2 => "bz2",
            Self::Xz => "xz",
            Self::Zstd => "zst",
            Self::Uncompressed => "",
        }
    }
}

macro_rules! impl_compression_type {
    ($(($enum_item:ident, $prefix:ident)),*) => {
        paste::item! {
            impl CompressionType {
                pub async fn encode(&self, content: impl AsRef<[u8]>) -> io::Result<Vec<u8>> {
                    match self {
                        $(
                            CompressionType::$enum_item => {
                                let mut buffer = Vec::with_capacity(content.as_ref().len());
                                let mut encoder = write::[<$prefix Encoder>]::new(&mut buffer);
                                encoder.write_all(content.as_ref()).await?;
                                encoder.shutdown().await?;
                                Ok(buffer)
                            }
                        )*
                        CompressionType::Uncompressed => Ok(content.as_ref().to_vec()),
                    }
                }

                pub async fn decode(&self, content: impl AsRef<[u8]>) -> io::Result<Vec<u8>> {
                    match self {
                        $(
                            CompressionType::$enum_item => {
                                let mut buffer = Vec::with_capacity(content.as_ref().len() * 2);
                                let mut encoder = write::[<$prefix Decoder>]::new(&mut buffer);
                                encoder.write_all(content.as_ref()).await?;
                                encoder.shutdown().await?;
                                Ok(buffer)
                            }
                        )*
                        CompressionType::Uncompressed => Ok(content.as_ref().to_vec()),
                    }
                }

                pub fn convert_async_read<T: AsyncRead + Unpin + Send + 'static>(
                    &self,
                    s: T,
                ) -> Box<dyn AsyncRead + Unpin + Send> {
                    match self {
                        $(CompressionType::$enum_item => Box::new([<$prefix Decoder>]::new(BufReader::new(s))),)*
                        CompressionType::Uncompressed => Box::new(s),
                    }
                }

                pub fn convert_stream<T: Stream<Item = io::Result<Bytes>> + Unpin + Send + 'static>(
                    &self,
                    s: T,
                ) -> Box<dyn Stream<Item = io::Result<Bytes>> + Send + Unpin> {
                    match self {
                        $(CompressionType::$enum_item => Box::new(ReaderStream::new([<$prefix Decoder>]::new(StreamReader::new(s)))),)*
                        CompressionType::Uncompressed => Box::new(s),
                    }
                }
            }

            #[cfg(test)]
            mod tests {
                use super::CompressionType;

                $(
                #[tokio::test]
                async fn [<test_ $enum_item:lower _compression>]() {
                    let string = "foo_bar".as_bytes().to_vec();
                    let compress = CompressionType::$enum_item
                        .encode(&string)
                        .await
                        .unwrap();
                    let decompress = CompressionType::$enum_item
                        .decode(&compress)
                        .await
                        .unwrap();
                    assert_eq!(decompress, string);
                })*

                #[tokio::test]
                async fn test_uncompression() {
                    let string = "foo_bar".as_bytes().to_vec();
                    let compress = CompressionType::Uncompressed
                        .encode(&string)
                        .await
                        .unwrap();
                    let decompress = CompressionType::Uncompressed
                        .decode(&compress)
                        .await
                        .unwrap();
                    assert_eq!(decompress, string);
                }
            }
        }
    };
}

impl_compression_type!((Gzip, Gzip), (Bzip2, Bz), (Xz, Xz), (Zstd, Zstd));
