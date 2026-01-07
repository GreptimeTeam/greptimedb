// Copyright 2024 Greptime Team
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

//! Vector index blob format helpers.

use std::fmt;

use store_api::storage::{VectorDistanceMetric, VectorIndexEngineType};

pub(crate) const VECTOR_INDEX_BLOB_VERSION: u8 = 1;
pub(crate) const VECTOR_INDEX_BLOB_HEADER_SIZE: usize = 33;

#[derive(Debug, Clone, Copy)]
pub(crate) struct VectorIndexBlobHeader {
    pub engine_type: VectorIndexEngineType,
    pub dim: u32,
    pub metric: VectorDistanceMetric,
    pub connectivity: u16,
    pub expansion_add: u16,
    pub expansion_search: u16,
    pub total_rows: u64,
    pub indexed_rows: u64,
    pub null_bitmap_len: u32,
}

impl VectorIndexBlobHeader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        engine_type: VectorIndexEngineType,
        dim: u32,
        metric: VectorDistanceMetric,
        connectivity: u16,
        expansion_add: u16,
        expansion_search: u16,
        total_rows: u64,
        indexed_rows: u64,
        null_bitmap_len: u32,
    ) -> Result<Self, VectorIndexBlobFormatError> {
        if total_rows < indexed_rows {
            return Err(VectorIndexBlobFormatError::InvalidRowCounts {
                total: total_rows,
                indexed: indexed_rows,
            });
        }
        if total_rows > u64::from(u32::MAX) || indexed_rows > u64::from(u32::MAX) {
            return Err(VectorIndexBlobFormatError::RowsExceedU32 {
                total: total_rows,
                indexed: indexed_rows,
            });
        }
        Ok(Self {
            engine_type,
            dim,
            metric,
            connectivity,
            expansion_add,
            expansion_search,
            total_rows,
            indexed_rows,
            null_bitmap_len,
        })
    }

    pub(crate) fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.push(VECTOR_INDEX_BLOB_VERSION);
        buf.push(self.engine_type.as_u8());
        buf.extend_from_slice(&self.dim.to_le_bytes());
        buf.push(self.metric.as_u8());
        buf.extend_from_slice(&self.connectivity.to_le_bytes());
        buf.extend_from_slice(&self.expansion_add.to_le_bytes());
        buf.extend_from_slice(&self.expansion_search.to_le_bytes());
        buf.extend_from_slice(&self.total_rows.to_le_bytes());
        buf.extend_from_slice(&self.indexed_rows.to_le_bytes());
        buf.extend_from_slice(&self.null_bitmap_len.to_le_bytes());
    }

    pub(crate) fn decode(data: &[u8]) -> Result<(Self, usize), VectorIndexBlobFormatError> {
        if data.len() < VECTOR_INDEX_BLOB_HEADER_SIZE {
            return Err(VectorIndexBlobFormatError::Truncated("header"));
        }

        let mut offset = 0;
        let version = read_u8(data, &mut offset)?;
        if version != VECTOR_INDEX_BLOB_VERSION {
            return Err(VectorIndexBlobFormatError::UnsupportedVersion(version));
        }

        let engine_type = VectorIndexEngineType::try_from_u8(read_u8(data, &mut offset)?)
            .ok_or_else(|| VectorIndexBlobFormatError::UnknownEngine(data[offset - 1]))?;
        let dim = read_u32(data, &mut offset)?;
        let metric = VectorDistanceMetric::try_from_u8(read_u8(data, &mut offset)?)
            .ok_or_else(|| VectorIndexBlobFormatError::UnknownMetric(data[offset - 1]))?;
        let connectivity = read_u16(data, &mut offset)?;
        let expansion_add = read_u16(data, &mut offset)?;
        let expansion_search = read_u16(data, &mut offset)?;
        let total_rows = read_u64(data, &mut offset)?;
        let indexed_rows = read_u64(data, &mut offset)?;
        let null_bitmap_len = read_u32(data, &mut offset)?;

        let header = VectorIndexBlobHeader::new(
            engine_type,
            dim,
            metric,
            connectivity,
            expansion_add,
            expansion_search,
            total_rows,
            indexed_rows,
            null_bitmap_len,
        )?;
        Ok((header, offset))
    }
}

#[derive(Debug)]
pub(crate) enum VectorIndexBlobFormatError {
    Truncated(&'static str),
    UnsupportedVersion(u8),
    UnknownEngine(u8),
    UnknownMetric(u8),
    InvalidRowCounts { total: u64, indexed: u64 },
    RowsExceedU32 { total: u64, indexed: u64 },
}

impl fmt::Display for VectorIndexBlobFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated(label) => {
                write!(f, "Vector index blob truncated while reading {}", label)
            }
            Self::UnsupportedVersion(version) => {
                write!(f, "Unsupported vector index version {}", version)
            }
            Self::UnknownEngine(value) => write!(f, "Unknown vector index engine type {}", value),
            Self::UnknownMetric(value) => write!(f, "Unknown vector index metric {}", value),
            Self::InvalidRowCounts { total, indexed } => {
                write!(
                    f,
                    "Total rows {} is smaller than indexed rows {}",
                    total, indexed
                )
            }
            Self::RowsExceedU32 { total, indexed } => {
                write!(
                    f,
                    "Vector index rows exceed u32::MAX (total={}, indexed={})",
                    total, indexed
                )
            }
        }
    }
}

fn read_exact<const N: usize>(
    data: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<[u8; N], VectorIndexBlobFormatError> {
    if *offset + N > data.len() {
        return Err(VectorIndexBlobFormatError::Truncated(label));
    }
    let mut buf = [0u8; N];
    buf.copy_from_slice(&data[*offset..*offset + N]);
    *offset += N;
    Ok(buf)
}

fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, VectorIndexBlobFormatError> {
    Ok(read_exact::<1>(data, offset, "u8")?[0])
}

fn read_u16(data: &[u8], offset: &mut usize) -> Result<u16, VectorIndexBlobFormatError> {
    Ok(u16::from_le_bytes(read_exact::<2>(data, offset, "u16")?))
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32, VectorIndexBlobFormatError> {
    Ok(u32::from_le_bytes(read_exact::<4>(data, offset, "u32")?))
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, VectorIndexBlobFormatError> {
    Ok(u64::from_le_bytes(read_exact::<8>(data, offset, "u64")?))
}
