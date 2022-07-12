use futures::TryStreamExt;

use crate::{DirEntry, DirStreamer};

pub async fn collect(stream: DirStreamer) -> Result<Vec<DirEntry>, std::io::Error> {
    stream.try_collect::<Vec<_>>().await
}

/// Normalize a directory path, ensure it is ends with '/'
pub fn normalize_dir(dir: &str) -> String {
    let mut dir = dir.to_string();
    if !dir.ends_with('/') {
        dir.push('/')
    }

    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_dir() {
        assert_eq!("/", normalize_dir("/"));
        assert_eq!("/", normalize_dir(""));
        assert_eq!("/test/", normalize_dir("/test"));
    }
}
