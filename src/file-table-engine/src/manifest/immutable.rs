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
use storage::manifest::ImmutableManifestObjectStore;
use table::metadata::RawTableInfo;

use crate::error::{
    DecodeJsonSnafu, DeleteTableManifestSnafu, EncodeJsonSnafu, ReadTableManifestSnafu, Result,
    WriteTableManifestSnafu,
};
const IMMUTABLE_MANIFEST_FILE: &str = "_immutable_manifest";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ImmutableMetadata {
    pub table_info: RawTableInfo,
}

pub fn encode_immutable_metadata(metadata: &ImmutableMetadata) -> Result<Vec<u8>> {
    serde_json::to_vec(metadata).context(DecodeJsonSnafu)
}

pub fn decode_immutable_metadata(bs: &[u8]) -> Result<ImmutableMetadata> {
    serde_json::from_slice(bs).context(EncodeJsonSnafu)
}

#[derive(Clone, Debug)]
pub struct ImmutableManifest {
    store: Arc<ImmutableManifestObjectStore>,
}

impl ImmutableManifest {
    pub fn new(dir: &str, object_store: ObjectStore) -> Self {
        ImmutableManifest {
            store: Arc::new(ImmutableManifestObjectStore::new(
                dir,
                IMMUTABLE_MANIFEST_FILE,
                object_store,
            )),
        }
    }

    pub async fn write(&self, metadata: &ImmutableMetadata) -> Result<()> {
        let bs = encode_immutable_metadata(metadata)?;

        self.store.write(&bs).await.context(WriteTableManifestSnafu)
    }

    pub async fn read(&self) -> Result<ImmutableMetadata> {
        let bs = self.store.read().await.context(ReadTableManifestSnafu)?;

        decode_immutable_metadata(&bs)
    }

    pub(crate) async fn delete(dir: &str, object_store: ObjectStore) -> Result<()> {
        object_store
            .delete(&format!("{}{}", dir, IMMUTABLE_MANIFEST_FILE))
            .await
            .context(DeleteTableManifestSnafu)
    }
}
