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

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use datatypes::vectors::VectorRef;

use super::SstStatistics;
use crate::storage::SchemaRef;

/// A bunch of rows in columnar format.
#[derive(Debug)]
pub struct Chunk {
    pub columns: Vec<VectorRef>,
    // TODO(yingwen): Sequences.
}

impl Chunk {
    pub fn new(columns: Vec<VectorRef>) -> Chunk {
        Chunk { columns }
    }
}

/// `ChunkReader` is similar to async iterator of [Chunk].
#[async_trait]
pub trait ChunkReader: Send {
    type Error: ErrorExt + Send + Sync;

    /// Schema of the chunks returned by this reader.
    /// This schema does not contain internal columns.
    fn user_schema(&self) -> &SchemaRef;

    /// SST statistics of the chunks returned by this reader.
    fn statistics(&self) -> SstStatistics;

    /// Fetch next chunk from the reader.
    async fn next_chunk(&mut self) -> Result<Option<Chunk>, Self::Error>;

    // project the chunk according to required projection.
    fn project_chunk(&self, chunk: Chunk) -> Chunk;
}
