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

use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read};

use futures::io::Cursor as AsyncCursor;
use futures::AsyncReadExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::file_format::reader::{PuffinAsyncReader, PuffinFileReader, PuffinSyncReader};
use crate::file_format::writer::{Blob, PuffinAsyncWriter, PuffinFileWriter, PuffinSyncWriter};

#[test]
fn test_read_empty_puffin_sync() {
    let path = "src/tests/resources/empty-puffin-uncompressed.puffin";

    let file = File::open(path).unwrap();
    let mut reader = PuffinFileReader::new(file);
    let metadata = reader.metadata().unwrap();
    assert_eq!(metadata.properties.len(), 0);
    assert_eq!(metadata.blobs.len(), 0);
}

#[tokio::test]
async fn test_read_empty_puffin_async() {
    let path = "src/tests/resources/empty-puffin-uncompressed.puffin";

    let file = tokio::fs::File::open(path).await.unwrap();
    let mut reader = PuffinFileReader::new(file.compat());
    let metadata = reader.metadata().await.unwrap();
    assert_eq!(metadata.properties.len(), 0);
    assert_eq!(metadata.blobs.len(), 0);
}

#[test]
fn test_sample_metric_data_puffin_sync() {
    let path = "src/tests/resources/sample-metric-data-uncompressed.puffin";

    let file = File::open(path).unwrap();
    let mut reader = PuffinFileReader::new(file);
    let metadata = reader.metadata().unwrap();

    assert_eq!(metadata.properties.len(), 1);
    assert_eq!(
        metadata.properties.get("created-by"),
        Some(&"Test 1234".to_string())
    );

    assert_eq!(metadata.blobs.len(), 2);
    assert_eq!(metadata.blobs[0].blob_type, "some-blob");
    assert_eq!(metadata.blobs[0].offset, 4);
    assert_eq!(metadata.blobs[0].length, 9);

    assert_eq!(metadata.blobs[1].blob_type, "some-other-blob");
    assert_eq!(metadata.blobs[1].offset, 13);
    assert_eq!(metadata.blobs[1].length, 83);

    let mut some_blob = reader.blob_reader(&metadata.blobs[0]).unwrap();
    let mut buf = String::new();
    some_blob.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "abcdefghi");

    let mut some_other_blob = reader.blob_reader(&metadata.blobs[1]).unwrap();
    let mut buf = Vec::new();
    some_other_blob.read_to_end(&mut buf).unwrap();
    let expected = include_bytes!("tests/resources/sample-metric-data.blob");
    assert_eq!(buf, expected);
}

#[tokio::test]
async fn test_sample_metric_data_puffin_async() {
    let path = "src/tests/resources/sample-metric-data-uncompressed.puffin";

    let file = tokio::fs::File::open(path).await.unwrap();
    let mut reader = PuffinFileReader::new(file.compat());
    let metadata = reader.metadata().await.unwrap();

    assert_eq!(metadata.properties.len(), 1);
    assert_eq!(
        metadata.properties.get("created-by"),
        Some(&"Test 1234".to_string())
    );

    assert_eq!(metadata.blobs.len(), 2);
    assert_eq!(metadata.blobs[0].blob_type, "some-blob");
    assert_eq!(metadata.blobs[0].offset, 4);
    assert_eq!(metadata.blobs[0].length, 9);

    assert_eq!(metadata.blobs[1].blob_type, "some-other-blob");
    assert_eq!(metadata.blobs[1].offset, 13);
    assert_eq!(metadata.blobs[1].length, 83);

    let mut some_blob = reader.blob_reader(&metadata.blobs[0]).unwrap();
    let mut buf = String::new();
    some_blob.read_to_string(&mut buf).await.unwrap();
    assert_eq!(buf, "abcdefghi");

    let mut some_other_blob = reader.blob_reader(&metadata.blobs[1]).unwrap();
    let mut buf = Vec::new();
    some_other_blob.read_to_end(&mut buf).await.unwrap();
    let expected = include_bytes!("tests/resources/sample-metric-data.blob");
    assert_eq!(buf, expected);
}

#[test]
fn test_writer_reader_with_empty_sync() {
    let mut buf = Cursor::new(vec![]);

    let mut writer = PuffinFileWriter::new(&mut buf);
    writer.set_properties(HashMap::from([(
        "created-by".to_string(),
        "Test 1234".to_string(),
    )]));

    let written_bytes = writer.finish().unwrap();
    assert!(written_bytes > 0);

    let mut buf = Cursor::new(buf.into_inner());
    let mut reader = PuffinFileReader::new(&mut buf);
    let metadata = reader.metadata().unwrap();

    assert_eq!(metadata.properties.len(), 1);
    assert_eq!(
        metadata.properties.get("created-by"),
        Some(&"Test 1234".to_string())
    );

    assert_eq!(metadata.blobs.len(), 0);
}

#[tokio::test]
async fn test_writer_reader_empty_async() {
    let mut buf = AsyncCursor::new(vec![]);

    let mut writer = PuffinFileWriter::new(&mut buf);
    writer.set_properties(HashMap::from([(
        "created-by".to_string(),
        "Test 1234".to_string(),
    )]));

    let written_bytes = writer.finish().await.unwrap();
    assert!(written_bytes > 0);

    let mut buf = AsyncCursor::new(buf.into_inner());
    let mut reader = PuffinFileReader::new(&mut buf);
    let metadata = reader.metadata().await.unwrap();

    assert_eq!(metadata.properties.len(), 1);
    assert_eq!(
        metadata.properties.get("created-by"),
        Some(&"Test 1234".to_string())
    );

    assert_eq!(metadata.blobs.len(), 0);
}

#[test]
fn test_writer_reader_sync() {
    let mut buf = Cursor::new(vec![]);

    let mut writer = PuffinFileWriter::new(&mut buf);

    let blob1 = "abcdefghi";
    writer
        .add_blob(Blob {
            data: Cursor::new(&blob1),
            blob_type: "some-blob".to_string(),
            properties: Default::default(),
        })
        .unwrap();

    let blob2 = include_bytes!("tests/resources/sample-metric-data.blob");
    writer
        .add_blob(Blob {
            data: Cursor::new(&blob2),
            blob_type: "some-other-blob".to_string(),
            properties: Default::default(),
        })
        .unwrap();

    writer.set_properties(HashMap::from([(
        "created-by".to_string(),
        "Test 1234".to_string(),
    )]));

    let written_bytes = writer.finish().unwrap();
    assert!(written_bytes > 0);

    let mut buf = Cursor::new(buf.into_inner());
    let mut reader = PuffinFileReader::new(&mut buf);
    let metadata = reader.metadata().unwrap();

    assert_eq!(metadata.properties.len(), 1);
    assert_eq!(
        metadata.properties.get("created-by"),
        Some(&"Test 1234".to_string())
    );

    assert_eq!(metadata.blobs.len(), 2);
    assert_eq!(metadata.blobs[0].blob_type, "some-blob");
    assert_eq!(metadata.blobs[0].offset, 4);
    assert_eq!(metadata.blobs[0].length, 9);

    assert_eq!(metadata.blobs[1].blob_type, "some-other-blob");
    assert_eq!(metadata.blobs[1].offset, 13);
    assert_eq!(metadata.blobs[1].length, 83);

    let mut some_blob = reader.blob_reader(&metadata.blobs[0]).unwrap();
    let mut buf = String::new();
    some_blob.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, blob1);

    let mut some_other_blob = reader.blob_reader(&metadata.blobs[1]).unwrap();
    let mut buf = Vec::new();
    some_other_blob.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, blob2);
}

#[tokio::test]
async fn test_writer_reader_async() {
    let mut buf = AsyncCursor::new(vec![]);

    let mut writer = PuffinFileWriter::new(&mut buf);

    let blob1 = "abcdefghi".as_bytes();
    writer
        .add_blob(Blob {
            data: AsyncCursor::new(blob1),
            blob_type: "some-blob".to_string(),
            properties: Default::default(),
        })
        .await
        .unwrap();

    let blob2 = include_bytes!("tests/resources/sample-metric-data.blob");
    writer
        .add_blob(Blob {
            data: AsyncCursor::new(&blob2),
            blob_type: "some-other-blob".to_string(),
            properties: Default::default(),
        })
        .await
        .unwrap();

    writer.set_properties(HashMap::from([(
        "created-by".to_string(),
        "Test 1234".to_string(),
    )]));

    let written_bytes = writer.finish().await.unwrap();
    assert!(written_bytes > 0);

    let mut buf = AsyncCursor::new(buf.into_inner());
    let mut reader = PuffinFileReader::new(&mut buf);
    let metadata = reader.metadata().await.unwrap();

    assert_eq!(metadata.properties.len(), 1);
    assert_eq!(
        metadata.properties.get("created-by"),
        Some(&"Test 1234".to_string())
    );

    assert_eq!(metadata.blobs.len(), 2);
    assert_eq!(metadata.blobs[0].blob_type, "some-blob");
    assert_eq!(metadata.blobs[0].offset, 4);
    assert_eq!(metadata.blobs[0].length, 9);

    assert_eq!(metadata.blobs[1].blob_type, "some-other-blob");
    assert_eq!(metadata.blobs[1].offset, 13);
    assert_eq!(metadata.blobs[1].length, 83);

    let mut some_blob = reader.blob_reader(&metadata.blobs[0]).unwrap();
    let mut buf = Vec::new();
    some_blob.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, blob1);

    let mut some_other_blob = reader.blob_reader(&metadata.blobs[1]).unwrap();
    let mut buf = Vec::new();
    some_other_blob.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, blob2);
}
