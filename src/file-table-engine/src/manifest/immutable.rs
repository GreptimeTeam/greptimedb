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

use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use table::metadata::RawTableInfo;

use crate::error::{
    CheckObjectSnafu, DecodeJsonSnafu, DeleteTableManifestSnafu, EncodeJsonSnafu,
    ReadTableManifestSnafu, Result, WriteImmutableManifestSnafu, WriteTableManifestSnafu,
};
const IMMUTABLE_MANIFEST_FILE: &str = "_immutable_manifest";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ImmutableMetadata {
    pub table_info: RawTableInfo,
}

fn encode_metadata(item: &ImmutableMetadata) -> Result<Vec<u8>> {
    serde_json::to_vec(&item).context(EncodeJsonSnafu)
}

fn decode_metadata(src: &[u8]) -> Result<ImmutableMetadata> {
    serde_json::from_slice(src).context(DecodeJsonSnafu)
}

fn manifest_path(dir: &str) -> String {
    format!("{}{}", dir, IMMUTABLE_MANIFEST_FILE)
}

pub(crate) async fn delete_table_manifest(
    table_name: &str,
    dir: &str,
    object_store: ObjectStore,
) -> Result<()> {
    object_store
        .delete(&manifest_path(dir))
        .await
        .context(DeleteTableManifestSnafu { table_name })
}

pub(crate) async fn write_table_manifest(
    table_name: &str,
    dir: &str,
    object_store: &ObjectStore,
    metadata: &ImmutableMetadata,
) -> Result<()> {
    let path = &manifest_path(dir);
    let exist = object_store
        .is_exist(path)
        .await
        .context(CheckObjectSnafu { path })?;

    ensure!(!exist, WriteImmutableManifestSnafu { path });

    let bs = encode_metadata(metadata)?;

    object_store
        .write(path, bs)
        .await
        .context(WriteTableManifestSnafu { table_name })
}

pub(crate) async fn read_table_manifest(
    table_name: &str,
    dir: &str,
    object_store: &ObjectStore,
) -> Result<ImmutableMetadata> {
    let path = manifest_path(dir);
    let bs = object_store
        .read(&path)
        .await
        .context(ReadTableManifestSnafu { table_name })?;

    decode_metadata(&bs)
}
