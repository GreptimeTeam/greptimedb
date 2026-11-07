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

use super::*;
use crate::blob_metadata::BlobMetadataBuilder;

#[test]
fn test_blob_index_preserves_first_duplicate() {
    let blob = |offset| {
        BlobMetadataBuilder::default()
            .blob_type("duplicate".to_string())
            .offset(offset)
            .length(4)
            .build()
            .unwrap()
    };
    let first = blob(4);
    let metadata = ReaderMetadata::new(Arc::new(FileMetadata {
        blobs: vec![first.clone(), blob(8)],
        properties: HashMap::new(),
    }));
    assert_eq!(first, metadata.blob("duplicate").unwrap());
    assert!(metadata.blob("unknown").is_err());
}
