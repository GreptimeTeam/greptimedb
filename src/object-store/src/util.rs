use futures::TryStreamExt;

use crate::{Object, ObjectStreamer};

pub async fn collect(stream: ObjectStreamer) -> Result<Vec<Object>, std::io::Error> {
    stream.try_collect::<Vec<_>>().await
}
