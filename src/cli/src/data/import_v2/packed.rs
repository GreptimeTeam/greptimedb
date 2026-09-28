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

use std::collections::HashSet;

use common_datasource::packed_snapshot::{PACK_INDEX_FILE, PackIndex};
use snafu::ResultExt;
use sql::dialect::GreptimeDbDialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::statement::Statement;

use crate::data::export_v2::manifest::{ChunkStatus, Manifest};
use crate::data::import_v2::error::{InvalidPackedSnapshotSnafu, Result, SnapshotStorageSnafu};
use crate::data::path::{data_dir_for_schema_chunk, ddl_path_for_schema};
use crate::data::snapshot_storage::SnapshotStorage;

/// Matches every completed schema chunk to its source DDL before target writes.
pub(crate) async fn validate_snapshot(
    storage: &dyn SnapshotStorage,
    manifest: &Manifest,
    schemas: &[String],
) -> Result<()> {
    manifest
        .validate_layout()
        .map_err(|reason| InvalidPackedSnapshotSnafu { reason }.build())?;
    if manifest.schema_only
        && !storage
            .list_files_recursive("data/")
            .await
            .context(SnapshotStorageSnafu)?
            .is_empty()
    {
        return InvalidPackedSnapshotSnafu {
            reason: "schema-only snapshot contains data objects",
        }
        .fail();
    }
    for schema in schemas {
        let ddl = storage
            .read_text(&ddl_path_for_schema(schema))
            .await
            .context(SnapshotStorageSnafu)?;
        let statements = ParserContext::create_with_dialect(
            &ddl,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .map_err(|e| {
            InvalidPackedSnapshotSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
        let mut names = HashSet::new();
        for statement in statements {
            if let Statement::CreateTable(create) = statement {
                if create.engine.eq_ignore_ascii_case("metric")
                    && create.options.get("on_physical_table").is_none()
                {
                    continue;
                }
                let parts: Vec<_> = create
                    .name
                    .0
                    .iter()
                    .map(|p| p.as_ident().map(|i| i.value.as_str()))
                    .collect();
                let allowed = match parts.as_slice() {
                    [Some(_)] => true,
                    [Some(s), Some(_)] => *s == schema,
                    [Some(c), Some(s), Some(_)] => *c == manifest.catalog && *s == schema,
                    _ => false,
                };
                if !allowed {
                    return InvalidPackedSnapshotSnafu {
                        reason: "snapshot table DDL escapes its catalog/schema",
                    }
                    .fail();
                }
                let name = parts.last().and_then(|p| *p).unwrap_or_default();
                if !names.insert(name.to_string()) {
                    return InvalidPackedSnapshotSnafu {
                        reason: "duplicate snapshot table DDL",
                    }
                    .fail();
                }
            }
        }
        for chunk in &manifest.chunks {
            if chunk.status == ChunkStatus::Skipped && !names.is_empty() {
                return InvalidPackedSnapshotSnafu {
                    reason: "packed chunks require zero-row streams for empty tables",
                }
                .fail();
            }
            if chunk.status != ChunkStatus::Completed {
                continue;
            }
            let path = format!(
                "{}{PACK_INDEX_FILE}",
                data_dir_for_schema_chunk(schema, chunk.id)
            );
            let text = storage
                .read_text(&path)
                .await
                .context(SnapshotStorageSnafu)?;
            let index: PackIndex = serde_json::from_str(&text).map_err(|e| {
                InvalidPackedSnapshotSnafu {
                    reason: e.to_string(),
                }
                .build()
            })?;
            let prefix = data_dir_for_schema_chunk(schema, chunk.id);
            let expected: HashSet<_> = std::iter::once(path)
                .chain(index.objects.iter().map(|o| format!("{prefix}{}", o.path)))
                .collect();
            let actual: HashSet<_> = chunk
                .files
                .iter()
                .filter(|file| file.starts_with(&prefix))
                .cloned()
                .collect();
            if actual != expected {
                return InvalidPackedSnapshotSnafu {
                    reason: "chunk manifest inventory differs from indexed objects",
                }
                .fail();
            }
            index
                .validate_membership(names.iter().map(String::as_str))
                .map_err(|e| {
                    InvalidPackedSnapshotSnafu {
                        reason: e.to_string(),
                    }
                    .build()
                })?;
            for object in &index.objects {
                let path = format!("{prefix}{}", object.path);
                let length = storage
                    .file_size(&path)
                    .await
                    .context(SnapshotStorageSnafu)?;
                if length != Some(object.length) {
                    return InvalidPackedSnapshotSnafu {
                        reason: format!(
                            "object {path}: expected length {}, actual {length:?}",
                            object.length
                        ),
                    }
                    .fail();
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::export_v2::manifest::{ChunkMeta, TimeRange};
    use crate::data::snapshot_storage::OpenDalStorage;

    #[tokio::test]
    async fn packed_membership_uses_literal_names_and_excludes_physical_and_views() {
        let dir = tempfile::Builder::new()
            .prefix("packed snapshot # ")
            .tempdir()
            .unwrap();
        let uri = url::Url::from_file_path(dir.path()).unwrap();
        let storage = OpenDalStorage::from_uri(uri.as_str(), &Default::default()).unwrap();
        storage.write_text("schema/ddl/public.sql", "CREATE TABLE p (ts TIMESTAMP TIME INDEX) ENGINE=metric WITH(physical_metric_table=''); CREATE TABLE \"logical.name\" (ts TIMESTAMP TIME INDEX) ENGINE=metric WITH(on_physical_table='p'); CREATE VIEW v AS SELECT * FROM \"logical.name\";").await.unwrap();
        let path = "data/public/1/pack-index.json";
        let index = serde_json::json!({"version":1,"objects":[{"path":"pack-0.bin","kind":"pack","length":12}],"tables":[{"table_name":"logical.name","object":"pack-0.bin","offset":0,"length":12,"row_count":0}]});
        storage.write_text(path, &index.to_string()).await.unwrap();
        assert!(dir.path().join(path).is_file());
        storage
            .write_text("data/public/1/pack-0.bin", "abcdefghijkl")
            .await
            .unwrap();
        let mut manifest = Manifest::new_schema_only("greptime".into(), vec!["public".into()]);
        let mut chunk = ChunkMeta::new(1, TimeRange::unbounded());
        chunk.mark_completed(vec![path.into(), "data/public/1/pack-0.bin".into()], None);
        manifest.version = 2;
        manifest.data_layout = Some("metric-parquet-packs".into());
        manifest.schema_only = false;
        manifest.chunks.push(chunk);
        validate_snapshot(&storage, &manifest, &manifest.schemas)
            .await
            .unwrap();
        let mut missing = index.clone();
        missing["tables"] = serde_json::json!([]);
        storage
            .write_text(path, &missing.to_string())
            .await
            .unwrap();
        assert!(
            validate_snapshot(&storage, &manifest, &manifest.schemas)
                .await
                .is_err()
        );
        let mut escaped = index;
        escaped["objects"][0]["path"] = "../pack-0.bin".into();
        storage
            .write_text(path, &escaped.to_string())
            .await
            .unwrap();
        assert!(
            validate_snapshot(&storage, &manifest, &manifest.schemas)
                .await
                .is_err()
        );
    }
}
