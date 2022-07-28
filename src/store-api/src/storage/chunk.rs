use async_trait::async_trait;
use common_error::ext::ErrorExt;
use datatypes::vectors::VectorRef;

use crate::storage::SchemaRef;

/// A bunch of rows in columnar format.
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
    fn schema(&self) -> &SchemaRef;

    /// Fetch next chunk from the reader.
    async fn next_chunk(&mut self) -> Result<Option<Chunk>, Self::Error>;
}
