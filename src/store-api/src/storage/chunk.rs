
use datatypes::vectors::VectorRef;
use futures::stream::Stream;
use common_error::ext::ErrorExt;

struct Chunk {
    columns: Vec<VectorRef>,
}

trait ChunkStream: Stream<Item = Result<Chunk, Self::Error>> {
    type Error: ErrorExt + Send + Sync;
}
