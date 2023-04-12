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
use std::str::FromStr;

use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZstdDecoder};
use tokio::io::{AsyncRead, BufReader};

use crate::error::{self, Error, Result};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionType {
    /// Gzip-ed file
    GZIP,
    /// Bzip2-ed file
    BZIP2,
    /// Xz-ed file (liblzma)
    XZ,
    /// Zstd-ed file,
    ZSTD,
    /// Uncompressed file
    UNCOMPRESSED,
}

impl FromStr for CompressionType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.to_uppercase();
        match s.as_str() {
            "GZIP" | "GZ" => Ok(Self::GZIP),
            "BZIP2" | "BZ2" => Ok(Self::BZIP2),
            "XZ" => Ok(Self::XZ),
            "ZST" | "ZSTD" => Ok(Self::ZSTD),
            "" => Ok(Self::UNCOMPRESSED),
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
            Self::GZIP => "GZIP",
            Self::BZIP2 => "BZIP2",
            Self::XZ => "XZ",
            Self::ZSTD => "ZSTD",
            Self::UNCOMPRESSED => "",
        })
    }
}

impl CompressionType {
    pub const fn is_compressed(&self) -> bool {
        !matches!(self, &Self::UNCOMPRESSED)
    }

    pub fn convert_async_read<T: AsyncRead + Unpin + Send + 'static>(
        &self,
        s: T,
    ) -> Box<dyn AsyncRead + Unpin + Send> {
        match self {
            CompressionType::GZIP => Box::new(GzipDecoder::new(BufReader::new(s))),
            CompressionType::BZIP2 => Box::new(BzDecoder::new(BufReader::new(s))),
            CompressionType::XZ => Box::new(XzDecoder::new(BufReader::new(s))),
            CompressionType::ZSTD => Box::new(ZstdDecoder::new(BufReader::new(s))),
            CompressionType::UNCOMPRESSED => Box::new(s),
        }
    }
}
