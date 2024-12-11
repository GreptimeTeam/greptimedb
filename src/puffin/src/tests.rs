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
use std::vec;

use common_base::range_read::{FileReader, RangeReader};
use futures::io::Cursor as AsyncCursor;

use crate::file_format::reader::{
    AsyncReader, PuffinFileFooterReader, PuffinFileReader, SyncReader,
};
use crate::file_format::writer::{AsyncWriter, Blob, PuffinFileWriter, SyncWriter};
use crate::file_metadata::FileMetadata;

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

    let reader = FileReader::new(path).await.unwrap();
    let mut reader = PuffinFileReader::new(reader);
    let metadata = reader.metadata().await.unwrap();
    assert_eq!(metadata.properties.len(), 0);
    assert_eq!(metadata.blobs.len(), 0);
}

async fn test_read_puffin_file_metadata(
    path: &str,
    file_size: u64,
    expeccted_metadata: FileMetadata,
) {
    for prefetch_size in [0, file_size / 2, file_size, file_size + 10] {
        let reader = FileReader::new(path).await.unwrap();
        let mut footer_reader = PuffinFileFooterReader::new(reader, file_size);
        if prefetch_size > 0 {
            footer_reader = footer_reader.with_prefetch_size(prefetch_size);
        }
        let metadata = footer_reader.metadata().await.unwrap();
        assert_eq!(metadata.properties, expeccted_metadata.properties,);
        assert_eq!(metadata.blobs, expeccted_metadata.blobs);
    }
}

#[tokio::test]
async fn test_read_puffin_file_metadata_async() {
    let paths = vec![
        "src/tests/resources/empty-puffin-uncompressed.puffin",
        "src/tests/resources/sample-metric-data-uncompressed.puffin",
    ];
    for path in paths {
        let mut reader = FileReader::new(path).await.unwrap();
        let file_size = reader.metadata().await.unwrap().content_length;
        let mut reader = PuffinFileReader::new(reader);
        let metadata = reader.metadata().await.unwrap();

        test_read_puffin_file_metadata(path, file_size, metadata).await;
    }
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

    let reader = FileReader::new(path).await.unwrap();
    let mut reader = PuffinFileReader::new(reader);
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
    let buf = read_all_range(&mut some_blob).await;
    assert_eq!(&buf, b"abcdefghi");

    let mut some_other_blob = reader.blob_reader(&metadata.blobs[1]).unwrap();
    let buf = read_all_range(&mut some_other_blob).await;
    let expected = include_bytes!("tests/resources/sample-metric-data.blob");
    assert_eq!(buf, expected);
}

#[test]
fn test_writer_reader_with_empty_sync() {
    fn test_writer_reader_with_empty_sync(footer_compressed: bool) {
        let mut buf = Cursor::new(vec![]);

        let mut writer = PuffinFileWriter::new(&mut buf);
        writer.set_properties(HashMap::from([(
            "created-by".to_string(),
            "Test 1234".to_string(),
        )]));

        writer.set_footer_lz4_compressed(footer_compressed);
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

    test_writer_reader_with_empty_sync(false);
    test_writer_reader_with_empty_sync(true);
}

#[tokio::test]
async fn test_writer_reader_empty_async() {
    async fn test_writer_reader_empty_async(footer_compressed: bool) {
        let mut buf = AsyncCursor::new(vec![]);

        let mut writer = PuffinFileWriter::new(&mut buf);
        writer.set_properties(HashMap::from([(
            "created-by".to_string(),
            "Test 1234".to_string(),
        )]));

        writer.set_footer_lz4_compressed(footer_compressed);
        let written_bytes = writer.finish().await.unwrap();
        assert!(written_bytes > 0);

        let mut reader = PuffinFileReader::new(buf.into_inner());
        let metadata = reader.metadata().await.unwrap();

        assert_eq!(metadata.properties.len(), 1);
        assert_eq!(
            metadata.properties.get("created-by"),
            Some(&"Test 1234".to_string())
        );

        assert_eq!(metadata.blobs.len(), 0);
    }

    test_writer_reader_empty_async(false).await;
    test_writer_reader_empty_async(true).await;
}

#[test]
fn test_writer_reader_sync() {
    fn test_writer_reader_sync(footer_compressed: bool) {
        let mut buf = Cursor::new(vec![]);

        let mut writer = PuffinFileWriter::new(&mut buf);

        let blob1 = "abcdefghi";
        writer
            .add_blob(Blob {
                compressed_data: Cursor::new(&blob1),
                blob_type: "some-blob".to_string(),
                properties: Default::default(),
                compression_codec: None,
            })
            .unwrap();

        let blob2 = include_bytes!("tests/resources/sample-metric-data.blob");
        writer
            .add_blob(Blob {
                compressed_data: Cursor::new(&blob2),
                blob_type: "some-other-blob".to_string(),
                properties: Default::default(),
                compression_codec: None,
            })
            .unwrap();

        writer.set_properties(HashMap::from([(
            "created-by".to_string(),
            "Test 1234".to_string(),
        )]));

        writer.set_footer_lz4_compressed(footer_compressed);
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

    test_writer_reader_sync(false);
    test_writer_reader_sync(true);
}

#[tokio::test]
async fn test_writer_reader_async() {
    async fn test_writer_reader_async(footer_compressed: bool) {
        let mut buf = AsyncCursor::new(vec![]);

        let mut writer = PuffinFileWriter::new(&mut buf);

        let blob1 = "abcdefghi".as_bytes();
        writer
            .add_blob(Blob {
                compressed_data: AsyncCursor::new(blob1),
                blob_type: "some-blob".to_string(),
                properties: Default::default(),
                compression_codec: None,
            })
            .await
            .unwrap();

        let blob2 = include_bytes!("tests/resources/sample-metric-data.blob");
        writer
            .add_blob(Blob {
                compressed_data: AsyncCursor::new(&blob2),
                blob_type: "some-other-blob".to_string(),
                properties: Default::default(),
                compression_codec: None,
            })
            .await
            .unwrap();

        writer.set_properties(HashMap::from([(
            "created-by".to_string(),
            "Test 1234".to_string(),
        )]));

        writer.set_footer_lz4_compressed(footer_compressed);
        let written_bytes = writer.finish().await.unwrap();
        assert!(written_bytes > 0);

        let mut reader = PuffinFileReader::new(buf.into_inner());
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
        let buf = read_all_range(&mut some_blob).await;
        assert_eq!(buf, blob1);

        let mut some_other_blob = reader.blob_reader(&metadata.blobs[1]).unwrap();
        let buf = read_all_range(&mut some_other_blob).await;
        assert_eq!(buf, blob2);
    }

    test_writer_reader_async(false).await;
    test_writer_reader_async(true).await;
}

async fn read_all_range(reader: &mut impl RangeReader) -> Vec<u8> {
    let m = reader.metadata().await.unwrap();
    let buf = reader.read(0..m.content_length).await.unwrap();
    buf.to_vec()
}
