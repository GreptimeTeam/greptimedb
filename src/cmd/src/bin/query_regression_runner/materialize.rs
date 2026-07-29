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

use std::fs;
use std::path::Path;

use futures::{AsyncWriteExt as _, TryStreamExt};
use object_store::ObjectStore;
use object_store::factory::new_raw_object_store;
use object_store::services::Fs;

use crate::query_regression_runner::model::{
    CopyCounts, DestinationConfig, FixtureSummary, MaterializeResult,
};
use crate::query_regression_runner::{MaterializeArgs, Result};

pub(super) async fn run_materialize(args: MaterializeArgs) -> Result<()> {
    let destination: DestinationConfig = toml::from_str(&fs::read_to_string(args.destination)?)?;
    let result = materialize(&args.fixture_dir, destination).await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn materialize(
    fixture_dir: &Path,
    destination: DestinationConfig,
) -> Result<MaterializeResult> {
    let summary: FixtureSummary =
        serde_json::from_slice(&fs::read(fixture_dir.join("summary.json"))?)?;
    let region_dir = validate_region_dir(&summary.region_dir)?;
    let object_source = fs_operator(&fixture_dir.join("object-store"))?;
    let manifest_source = fs_operator(&fixture_dir.join("manifest"))?;
    let destination =
        new_raw_object_store(&destination.object_store, &destination.data_home).await?;

    let region_prefix = format!("{region_dir}/");
    destination
        .delete_with(&region_prefix)
        .recursive(true)
        .await?;

    let object_store = copy_tree(&object_source, &destination, "/", "").await?;
    let manifest = copy_tree(
        &manifest_source,
        &destination,
        "/",
        &format!("{region_dir}/manifest/"),
    )
    .await?;
    Ok(MaterializeResult {
        region_dir,
        object_store,
        manifest,
    })
}

fn fs_operator(root: &Path) -> Result<ObjectStore> {
    Ok(ObjectStore::new(Fs::default().root(&root.to_string_lossy()))?.finish())
}

fn validate_region_dir(region_dir: &str) -> Result<String> {
    let region_dir = region_dir.trim_end_matches('/');
    if region_dir.is_empty()
        || region_dir.starts_with('/')
        || region_dir.contains('\\')
        || region_dir
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(
            "region_dir must be a non-empty relative OpenDAL key without dot components".into(),
        );
    }
    Ok(region_dir.to_string())
}

async fn copy_tree(
    source: &ObjectStore,
    destination: &ObjectStore,
    source_prefix: &str,
    destination_prefix: &str,
) -> Result<CopyCounts> {
    let mut lister = source.lister_with(source_prefix).recursive(true).await?;
    let mut counts = CopyCounts::default();
    while let Some(entry) = lister.try_next().await? {
        if entry.metadata().is_dir() {
            continue;
        }
        let source_path = entry.path().to_string();
        let reader = source
            .reader(&source_path)
            .await?
            .into_futures_async_read(0..entry.metadata().content_length())
            .await?;
        let mut writer = destination
            .writer(&format!("{destination_prefix}{source_path}"))
            .await?
            .into_futures_async_write();
        let bytes = futures::io::copy(reader, &mut writer).await?;
        writer.close().await?;
        counts.files += 1;
        counts.bytes += bytes;
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use object_store::config::ObjectStoreConfig;

    use super::*;

    #[test]
    fn region_dir_must_be_a_relative_opendal_key() {
        assert_eq!(
            validate_region_dir("data/db/region/").unwrap(),
            "data/db/region"
        );
        for region_dir in [
            "",
            "/region",
            ".",
            "a/../region",
            "a/./region",
            "a\\region",
            "a//region",
        ] {
            assert!(validate_region_dir(region_dir).is_err(), "{region_dir}");
        }
    }

    #[tokio::test]
    async fn materializes_fixture_from_fs_to_fs() {
        let fixture = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir().unwrap();
        let region_dir = "data/public/metrics/00000000000000000001";
        let sst = fixture
            .path()
            .join("object-store")
            .join(region_dir)
            .join("00000000000000000001.parquet");
        fs::create_dir_all(sst.parent().unwrap()).unwrap();
        fs::write(&sst, b"sst").unwrap();
        let checkpoint = fixture
            .path()
            .join("manifest/00000000000000000001.checkpoint");
        fs::create_dir_all(checkpoint.parent().unwrap()).unwrap();
        fs::write(&checkpoint, b"checkpoint").unwrap();
        fs::write(fixture.path().join("manifest/_last_checkpoint"), b"last").unwrap();
        fs::write(
            fixture.path().join("summary.json"),
            format!(r#"{{"region_dir":"{region_dir}"}}"#),
        )
        .unwrap();
        fs::write(fixture.path().join("files.jsonl"), "metadata").unwrap();
        fs::create_dir_all(destination.path().join(region_dir)).unwrap();
        fs::write(destination.path().join(region_dir).join("stale"), "stale").unwrap();
        fs::write(destination.path().join("unrelated"), "keep").unwrap();

        let result = materialize(
            fixture.path(),
            DestinationConfig {
                data_home: destination.path().to_string_lossy().to_string(),
                object_store: ObjectStoreConfig::default(),
            },
        )
        .await
        .unwrap();

        assert_eq!(result.object_store.files, 1);
        assert_eq!(result.object_store.bytes, 3);
        assert_eq!(result.manifest.files, 2);
        assert_eq!(result.manifest.bytes, 14);
        assert_eq!(
            fs::read(
                destination
                    .path()
                    .join(region_dir)
                    .join("00000000000000000001.parquet")
            )
            .unwrap(),
            b"sst"
        );
        assert_eq!(
            fs::read(
                destination
                    .path()
                    .join(region_dir)
                    .join("manifest/00000000000000000001.checkpoint")
            )
            .unwrap(),
            b"checkpoint"
        );
        assert_eq!(
            fs::read(
                destination
                    .path()
                    .join(region_dir)
                    .join("manifest/_last_checkpoint")
            )
            .unwrap(),
            b"last"
        );
        assert!(!destination.path().join(region_dir).join("stale").exists());
        assert_eq!(
            fs::read(destination.path().join("unrelated")).unwrap(),
            b"keep"
        );
        assert!(!destination.path().join("summary.json").exists());
        assert!(!destination.path().join("files.jsonl").exists());
    }
}
