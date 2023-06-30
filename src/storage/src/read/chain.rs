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

use crate::error::Result;
use crate::read::{Batch, BatchReader};
use crate::schema::ProjectedSchemaRef;

pub struct ChainReader<R> {
    /// Schema to read
    pub schema: ProjectedSchemaRef,
    /// Each reader reads a slice of time window
    pub readers: Vec<R>,
}

impl<R> ChainReader<R> {
    pub fn new(schema: ProjectedSchemaRef, mut readers: Vec<R>) -> Self {
        // Reverse readers since we iter them backward.
        readers.reverse();
        Self { schema, readers }
    }
}

#[async_trait::async_trait]
impl<R> BatchReader for ChainReader<R>
where
    R: BatchReader,
{
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(reader) = self.readers.last_mut() {
            if let Some(batch) = reader.next_batch().await? {
                return Ok(Some(batch));
            } else {
                // Remove the exhausted reader.
                let _ = self.readers.pop();
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::read_util::{self, Batches, VecBatchReader};

    fn build_chain_reader(sources: &[Batches]) -> ChainReader<VecBatchReader> {
        let schema = read_util::new_projected_schema();
        let readers = sources
            .iter()
            .map(|source| read_util::build_vec_reader(source))
            .collect();

        ChainReader::new(schema, readers)
    }

    #[tokio::test]
    async fn test_chain_empty() {
        let mut reader = build_chain_reader(&[]);

        assert!(reader.next_batch().await.unwrap().is_none());
        // Call next_batch() again is allowed.
        assert!(reader.next_batch().await.unwrap().is_none());
    }
}
