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

use std::sync::Arc;

use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use storage::codec::{Codec, Decoder, Encoder};
use storage::error::{
    DecodeJsonSnafu, EncodeJsonSnafu, Error as StorageError, Result as StorageResult,
};
use storage::manifest::ImmutableManifestImpl;
use table::metadata::RawTableInfo;

use crate::error::{DeleteTableManifestSnafu, Result};
const IMMUTABLE_MANIFEST_FILE: &str = "_immutable_manifest";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ImmutableMetadata {
    pub table_info: RawTableInfo,
}

#[derive(Default, Debug)]
pub struct ImmutableMetadataCodec {}

impl ImmutableMetadataCodec {
    pub fn new() -> Self {
        ImmutableMetadataCodec::default()
    }
}

impl Encoder for ImmutableMetadataCodec {
    type Item = ImmutableMetadata;
    type Error = StorageError;

    fn encode(&self, item: &Self::Item, dst: &mut Vec<u8>) -> StorageResult<()> {
        serde_json::to_writer(dst, &item).context(EncodeJsonSnafu)
    }
}

impl Decoder for ImmutableMetadataCodec {
    type Item = ImmutableMetadata;
    type Error = StorageError;

    fn decode(&self, src: &[u8]) -> StorageResult<Self::Item> {
        serde_json::from_slice(src).context(DecodeJsonSnafu)
    }
}

impl Codec<ImmutableMetadata, StorageError> for ImmutableMetadataCodec {}

pub type ImmutableManifest = ImmutableManifestImpl<ImmutableMetadata>;

pub(crate) fn build_manifest(dir: &str, object_store: ObjectStore) -> ImmutableManifest {
    ImmutableManifestImpl::new(
        dir,
        IMMUTABLE_MANIFEST_FILE,
        object_store,
        Arc::new(ImmutableMetadataCodec::new()),
    )
}

pub(crate) async fn delete_manifest(dir: &str, object_store: ObjectStore) -> Result<()> {
    ImmutableManifest::delete(dir, IMMUTABLE_MANIFEST_FILE, object_store)
        .await
        .context(DeleteTableManifestSnafu)
}
