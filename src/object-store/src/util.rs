use futures::TryStreamExt;

use crate::{DirEntry, DirStreamer};

pub async fn collect(stream: DirStreamer) -> Result<Vec<DirEntry>, std::io::Error> {
    stream.try_collect::<Vec<_>>().await
}
