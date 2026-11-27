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

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use opendal_core::{BytesRange, ErrorKind, Operator};
use opendal_secure_fs::SecureFs;
use tempfile::TempDir;

fn setup() -> (TempDir, Operator) {
    let directory = tempfile::tempdir().unwrap();
    let root = Dir::open_ambient_dir(directory.path(), ambient_authority()).unwrap();
    let operator = Operator::new(SecureFs::from_dir(root)).unwrap();
    (directory, operator)
}

#[test]
fn building_requires_a_directory() {
    let error = Operator::new(SecureFs::default()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
}

#[tokio::test]
async fn operators_use_the_directory_they_were_given() {
    let (_first_dir, first) = setup();
    let (_second_dir, second) = setup();

    first.write("file", "first").await.unwrap();
    second.write("file", "second").await.unwrap();
    assert_eq!(
        first.read("file").await.unwrap().to_bytes().as_ref(),
        b"first"
    );
    assert_eq!(
        second.read("file").await.unwrap().to_bytes().as_ref(),
        b"second"
    );
}

#[tokio::test]
async fn writes_lists_and_reads_ranges() {
    let (_directory, operator) = setup();
    operator.create_dir("nested/").await.unwrap();
    let root = operator.stat("/").await.unwrap();
    assert!(root.is_dir());

    operator.write("nested/file", "abcdef").await.unwrap();
    operator
        .write_with("nested/file", "ghi")
        .append(true)
        .await
        .unwrap();
    let metadata = operator.stat("nested/file").await.unwrap();
    assert_eq!(metadata.content_length(), 9);
    let range = operator.read_with("nested/file").range(2..5).await.unwrap();
    assert_eq!(range.to_bytes().as_ref(), b"cde");
    let suffix = operator
        .read_with("nested/file")
        .range(BytesRange::Suffix { size: 3 })
        .await
        .unwrap();
    assert_eq!(suffix.to_bytes().as_ref(), b"ghi");

    let entries = operator.list("nested/").await.unwrap();
    assert!(entries.iter().any(|entry| entry.path() == "nested/file"));
    assert!(operator.list("missing/").await.unwrap().is_empty());
    assert!(operator.list("nested/file/").await.unwrap().is_empty());
}

#[tokio::test]
async fn conditional_create_preserves_existing_data() {
    let (_directory, operator) = setup();
    operator
        .write_with("nested/file", "")
        .if_not_exists(true)
        .await
        .unwrap();
    let error = operator
        .write_with("nested/file", "replacement")
        .if_not_exists(true)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ConditionNotMatch);
    assert!(operator.read("nested/file").await.unwrap().is_empty());
}

#[tokio::test]
async fn deletes_descendants_but_preserves_root() {
    let (directory, operator) = setup();
    operator.write("nested/file", "data").await.unwrap();
    let error = operator.delete_with("/").recursive(true).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(
        std::fs::read(directory.path().join("nested/file")).unwrap(),
        b"data"
    );
    operator
        .delete_with("nested/")
        .recursive(true)
        .await
        .unwrap();
    assert!(!operator.exists("nested/file").await.unwrap());
    operator.delete("missing").await.unwrap();
}
