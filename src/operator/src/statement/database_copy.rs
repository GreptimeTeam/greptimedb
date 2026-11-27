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

//! Shared selection and destination rules for database COPY and prepared exports.

use std::collections::HashMap;

use common_datasource::object_store::{FILE_SCHEMA, FS_SCHEMA, parse_url};
use common_stat::get_total_cpu_cores;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME};
use table::TableRef;
use table::metadata::TableType;
use table::requests::CopyDatabaseRequest;
use table::table_reference::TableReference;
use tokio::sync::Semaphore;
use url::Url;

use crate::error::{self, Result};
use crate::statement::StatementExecutor;

fn is_directory_location(location: &str) -> bool {
    if location.ends_with('/') {
        return true;
    }

    #[cfg(windows)]
    {
        location.ends_with(std::path::MAIN_SEPARATOR)
            && matches!(
                parse_url(location),
                Ok((schema, _, _)) if schema.eq_ignore_ascii_case(FS_SCHEMA)
            )
    }

    #[cfg(not(windows))]
    false
}

/// Get parallelism from options, default to total CPU cores.
pub(crate) fn parse_parallelism_from_option_map(options: &HashMap<String, String>) -> usize {
    options
        .get("parallelism")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(get_total_cpu_cores)
        .clamp(1, Semaphore::MAX_PERMITS)
}

/// Rejects layouts unsupported by the per-table export path.
pub(crate) fn validate_database_export_layout(options: &HashMap<String, String>) -> Result<()> {
    if let Some(layout) = options.get("metric_data_layout") {
        return error::InvalidCopyParameterSnafu {
            key: "metric_data_layout",
            value: layout,
        }
        .fail();
    }
    Ok(())
}

pub(crate) fn validate_database_directory(location: &str) -> Result<()> {
    ensure!(
        is_directory_location(location),
        error::InvalidCopyDatabasePathSnafu { value: location }
    );
    #[cfg(windows)]
    if common_datasource::object_store::handle_windows_path(location).is_some() {
        return Ok(());
    }
    let parsed_directory = match Url::parse(location) {
        Ok(url) => {
            url.query().is_none() && url.fragment().is_none() && is_directory_location(url.path())
        }
        Err(_) => true,
    };
    ensure!(
        parsed_directory,
        error::InvalidCopyDatabasePathSnafu { value: location }
    );
    Ok(())
}

/// The writer key and its externally reported location, resolved together.
pub(crate) struct DatabaseExportFile {
    pub(crate) path: String,
    pub(crate) location: String,
}

impl DatabaseExportFile {
    pub(crate) fn new(directory: &str, name: &str, suffix: &str) -> Result<Self> {
        let filename = format!("{name}{suffix}");
        #[cfg(windows)]
        if common_datasource::object_store::handle_windows_path(directory).is_some() {
            return Ok(Self {
                location: format!("{directory}{filename}"),
                path: filename,
            });
        }
        match Url::parse(directory) {
            Ok(mut url) => {
                url.path_segments_mut()
                    .map_err(|_| error::InvalidCopyDatabasePathSnafu { value: directory }.build())?
                    .pop_if_empty()
                    .push(&filename);
                // File URLs are decoded by the filesystem backend; object-store
                // backends use the encoded URL path as their key.
                let path = if url.scheme().eq_ignore_ascii_case(FILE_SCHEMA) {
                    filename
                } else {
                    url.path()
                        .rsplit('/')
                        .next()
                        .unwrap_or_default()
                        .to_string()
                };
                Ok(Self {
                    path,
                    location: url.into(),
                })
            }
            Err(url::ParseError::RelativeUrlWithoutBase) => Ok(Self {
                location: format!("{directory}{filename}"),
                path: filename,
            }),
            Err(source) => Err(source)
                .context(common_datasource::error::InvalidUrlSnafu { url: directory })
                .context(error::BuildBackendSnafu),
        }
    }
}

/// Resolves every output before any writer starts. Packed exports use generated
/// object names instead of table names on disk.
pub(crate) fn prepare_database_export_files(
    directory: &str,
    names: impl IntoIterator<Item = String>,
    suffix: &str,
    packed: bool,
) -> Result<Vec<DatabaseExportFile>> {
    let (scheme, _, _) = parse_url(directory).context(error::BuildBackendSnafu)?;
    let local = scheme.eq_ignore_ascii_case(FS_SCHEMA) || scheme.eq_ignore_ascii_case(FILE_SCHEMA);
    let mut filenames = HashMap::new();
    let mut outputs = Vec::new();
    for name in names {
        let output = DatabaseExportFile::new(directory, &name, suffix)?;
        if cfg!(windows) && local && !packed {
            common_datasource::object_store::validate_windows_file_name(&output.path).map_err(
                |reason| {
                    error::InvalidDatabaseExportSnafu {
                        reason: format!("table {name}: {reason}"),
                    }
                    .build()
                },
            )?;
        }
        // Use platform defaults for per-table files. This does not model
        // filesystem-specific Unicode equivalences such as APFS normalization.
        let key = if local && !packed && cfg!(any(windows, target_os = "macos")) {
            output.path.to_lowercase()
        } else {
            output.path.clone()
        };
        if let Some(previous) = filenames.insert(key, name.clone()) {
            return error::InvalidDatabaseExportSnafu {
                reason: format!(
                    "duplicate output name: {name} conflicts with {previous} ({})",
                    output.path
                ),
            }
            .fail();
        }
        outputs.push(output);
    }
    Ok(outputs)
}

/// Resolve a listed writer key back to its table name and COPY input location.
pub(crate) fn database_import_source(directory: &str, path: &str) -> Result<(String, String)> {
    let mut filename = path.rsplit('/').next().unwrap_or(path).to_string();
    let mut location = format!("{directory}{path}");
    #[cfg(windows)]
    let literal_path = common_datasource::object_store::handle_windows_path(directory).is_some();
    #[cfg(not(windows))]
    let literal_path = false;
    if !literal_path && let Ok(mut url) = Url::parse(directory) {
        if url.scheme().eq_ignore_ascii_case(FILE_SCHEMA) {
            url.path_segments_mut()
                .map_err(|_| error::InvalidCopyDatabasePathSnafu { value: directory }.build())?
                .pop_if_empty()
                .extend(path.split('/'));
        } else {
            // Listed object keys already contain the export URL's escaping.
            url.set_path(&format!("{}{path}", url.path()));
            filename = percent_encoding::percent_decode_str(&filename)
                .decode_utf8()
                .ok()
                .context(error::InvalidTableNameSnafu { table_name: path })?
                .into_owned();
        }
        location = url.into();
    }
    let table_name = filename
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .filter(|stem| !stem.is_empty())
        .context(error::InvalidTableNameSnafu { table_name: path })?
        .to_string();
    Ok((table_name, location))
}

impl StatementExecutor {
    /// Capture each selected data table once. Views, temporary and Metric physical
    /// tables do not have data outputs. `None` selects the whole schema.
    pub async fn capture_database_export_tables(
        &self,
        req: &CopyDatabaseRequest,
        names: Option<&[String]>,
        ctx: &QueryContextRef,
    ) -> Result<Vec<TableRef>> {
        let mut names = match names {
            Some(names) => names.to_vec(),
            None => self
                .catalog_manager
                .table_names(&req.catalog_name, &req.schema_name, Some(ctx))
                .await
                .context(error::CatalogSnafu)?,
        };
        names.sort();
        let mut tables = Vec::new();
        for name in names {
            let table = self
                .get_table(&TableReference::full(
                    &req.catalog_name,
                    &req.schema_name,
                    &name,
                ))
                .await?;
            let info = table.table_info();
            if table.table_type() == TableType::Base
                && (info.meta.engine != METRIC_ENGINE_NAME
                    || info
                        .meta
                        .options
                        .extra_options
                        .contains_key(LOGICAL_TABLE_METADATA_KEY))
            {
                tables.push(table);
            }
        }
        Ok(tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_export_file_names_are_distinct() {
        for directory in ["backup/", "file:///copy/backup/"] {
            for names in [["Events", "events"], ["Éa", "éa"]] {
                for packed in [false, true] {
                    let result = prepare_database_export_files(
                        directory,
                        names.map(String::from),
                        ".parquet",
                        packed,
                    );
                    if !packed && cfg!(any(windows, target_os = "macos")) {
                        assert!(matches!(result,
                            Err(error::Error::InvalidDatabaseExport { reason })
                            if names.iter().all(|name| reason.contains(name))));
                    } else {
                        assert_eq!(result.unwrap().len(), 2);
                    }
                }
            }
        }
        let overlong_name = "z".repeat(252);
        for name in [
            "job:rate5m",
            "a:b",
            ":leading",
            "con",
            "nul.part",
            &overlong_name,
        ] {
            let local = prepare_database_export_files("backup/", [name.into()], ".parquet", false);
            assert_eq!(local.is_err(), cfg!(windows), "{name}");
            prepare_database_export_files("backup/", [name.into()], ".parquet", true).unwrap();
        }
        let remote = prepare_database_export_files(
            "s3://bucket/backup/",
            [
                "Events".into(),
                "events".into(),
                "job:rate5m".into(),
                "con".into(),
                overlong_name,
            ],
            ".parquet",
            false,
        )
        .unwrap();
        assert_eq!(remote.len(), 5);
    }

    #[test]
    fn directory_url_components_cannot_capture_output_names() {
        for location in [
            "file:///copy/fresh?attempt=/",
            "file:///copy/fresh#attempt/",
            "s3://bucket/fresh?attempt=/",
            "s3://bucket/fresh#attempt/",
            "file:///copy/fresh",
        ] {
            assert!(validate_database_directory(location).is_err(), "{location}");
        }
        for location in ["/copy/fresh/", "file:///copy/fresh/", "s3://bucket/fresh/"] {
            validate_database_directory(location).unwrap();
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_directory_names_are_literal_paths() {
        for location in ["C:/copy/fresh#1/", r"C:\copy\fresh#1\"] {
            validate_database_directory(location).unwrap();
        }
        assert!(validate_database_directory("C:/copy/fresh#1").is_err());
    }

    #[tokio::test]
    async fn output_locations_resolve_to_writer_keys() {
        use common_datasource::object_store::{
            LocalFileAccess, RemoteStorageAccess, build_backend_for_write_with_path,
        };

        let dir = common_test_util::temp_dir::create_temp_dir("database_export_paths");
        let access = LocalFileAccess::sandboxed(dir.path()).unwrap();
        let file_url = Url::from_directory_path(dir.path()).unwrap().to_string();
        let connection = HashMap::from([
            ("region".into(), "us-east-1".into()),
            ("access_key_id".into(), "test-key".into()),
            ("secret_access_key".into(), "test-secret".into()),
        ]);
        for directory in [
            format!("{}/", dir.path().display()),
            file_url,
            "s3://export-bucket/data/".into(),
        ] {
            for name in ["a#b", "a:b"] {
                if cfg!(windows) && name.contains(':') && !directory.starts_with("s3:") {
                    continue;
                }
                let file = DatabaseExportFile::new(&directory, name, ".parquet").unwrap();
                let backend = build_backend_for_write_with_path(
                    &file.location,
                    &connection,
                    &access,
                    &RemoteStorageAccess::unrestricted(),
                )
                .await
                .unwrap();
                assert_eq!(backend.object_path.as_deref(), Some(file.path.as_str()));
                let (table_name, input_location) =
                    database_import_source(&directory, &file.path).unwrap();
                assert_eq!(table_name, name);
                assert_eq!(input_location, file.location);
                let (table_name, nested_location) =
                    database_import_source(&directory, &format!("nested/{}", file.path)).unwrap();
                assert_eq!(table_name, name);
                assert_eq!(
                    nested_location,
                    file.location
                        .replace(&directory, &format!("{directory}nested/"))
                );
                if !directory.starts_with("s3:") {
                    backend
                        .object_store
                        .write(&file.path, "test")
                        .await
                        .unwrap();
                    assert_eq!(
                        std::fs::read(dir.path().join(format!("{name}.parquet"))).unwrap(),
                        b"test"
                    );
                } else {
                    assert_eq!(
                        file.path,
                        if name == "a#b" {
                            "a%23b.parquet"
                        } else {
                            "a:b.parquet"
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn test_parse_parallelism_from_option_map() {
        let options = HashMap::new();
        assert_eq!(
            parse_parallelism_from_option_map(&options),
            get_total_cpu_cores()
        );

        let options = HashMap::from([("parallelism".to_string(), "0".to_string())]);
        assert_eq!(parse_parallelism_from_option_map(&options), 1);

        let options = HashMap::from([("parallelism".to_string(), usize::MAX.to_string())]);
        assert_eq!(
            parse_parallelism_from_option_map(&options),
            Semaphore::MAX_PERMITS
        );
    }
}
