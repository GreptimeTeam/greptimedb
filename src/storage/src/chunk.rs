use async_trait::async_trait;
use store_api::storage::{Chunk, ChunkReader, SchemaRef};

use crate::error::{Error, Result};
use crate::memtable::BatchIteratorPtr;

pub struct ChunkReaderImpl {
    schema: SchemaRef,
    // Now we only read data from one memtable, so we just holds the memtable iterator here.
    iter: BatchIteratorPtr,
}

#[async_trait]
impl ChunkReader for ChunkReaderImpl {
    type Error = Error;

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let mut batch = match self.iter.next()? {
            Some(b) => b,
            None => return Ok(None),
        };

        // TODO(yingwen): Check schema, now we assumes the schema is the same as key columns
        // combine with value columns.
        let mut columns = Vec::with_capacity(batch.keys.len() + batch.values.len());
        columns.append(&mut batch.keys);
        columns.append(&mut batch.values);

        Ok(Some(Chunk::new(columns)))
    }
}

impl ChunkReaderImpl {
    pub fn new(schema: SchemaRef, iter: BatchIteratorPtr) -> ChunkReaderImpl {
        ChunkReaderImpl { schema, iter }
    }
}
