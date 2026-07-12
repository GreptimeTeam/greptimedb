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

//! ChunkReader implementation for in-memory parquet bytes.

use std::io::Cursor;

use bytes::Bytes;
use parquet::errors::{ParquetError, Result};
use parquet::file::reader::{ChunkReader, Length};

/// A [ChunkReader] implementation for in-memory parquet bytes.
///
/// This provides byte access to parquet data stored in memory (Bytes),
/// used for reading parquet data from bulk memtable.
#[derive(Clone)]
pub struct MemtableChunkReader {
    /// The in-memory parquet data.
    data: Bytes,
}

impl MemtableChunkReader {
    /// Creates a new [MemtableChunkReader] from the given bytes.
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }
}

impl Length for MemtableChunkReader {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl ChunkReader for MemtableChunkReader {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        let start = start as usize;
        if start > self.data.len() {
            return Err(ParquetError::IndexOutOfBound(start, self.data.len()));
        }
        Ok(Cursor::new(self.data.slice(start..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let start = start as usize;
        let end = start + length;
        if end > self.data.len() {
            return Err(ParquetError::IndexOutOfBound(end, self.data.len()));
        }
        Ok(self.data.slice(start..end))
    }
}
