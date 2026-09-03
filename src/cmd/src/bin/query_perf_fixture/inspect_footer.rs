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

use std::collections::BTreeSet;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Args as ClapArgs;
use datafusion_object_store::path::Path as StorePath;
use datafusion_object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use object_store::config::ObjectStoreConfig;
use object_store::factory::new_raw_object_store;
use object_store::services::Fs;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use serde::{Deserialize, Serialize};

/// Same shape as `query_regression_runner::model::DestinationConfig`. The two
/// binaries are separate crate roots, so the serde-compatible struct is defined
/// here instead of being shared; it reuses the `object-store` crate's
/// `ObjectStoreConfig` deserialization.
#[derive(Debug, Deserialize)]
struct DestinationConfig {
    data_home: String,
    object_store: ObjectStoreConfig,
}

#[derive(Debug, ClapArgs)]
pub(super) struct InspectFooterArgs {
    /// Local data home (convenience shortcut for a File destination).
    #[arg(long)]
    root: Option<PathBuf>,
    /// TOML file: data_home = "..." and object_store = { type = "File" | "S3" | ... }.
    #[arg(long)]
    destination: Option<PathBuf>,
    #[arg(long, default_value = "greptime_value")]
    column: String,
    #[arg(long)]
    include_metadata_files: bool,
}

#[derive(Debug, Serialize)]
struct FooterReport {
    root: String,
    summary: FooterSummary,
    files: Vec<FooterFileReport>,
}
#[derive(Debug, Default, Serialize)]
struct FooterSummary {
    column: String,
    file_count: usize,
    files_with_column: usize,
    total_file_size: u64,
    total_rows: i64,
    column_compressed_size: i64,
    column_uncompressed_size: i64,
    unique_encodings: Vec<String>,
}
#[derive(Debug, Serialize)]
struct FooterFileReport {
    path: String,
    relative_path: String,
    file_size: u64,
    num_rows: i64,
    num_row_groups: usize,
    columns: Vec<FooterColumnChunkReport>,
}
#[derive(Debug, Serialize)]
struct FooterColumnChunkReport {
    row_group_index: usize,
    column_index: usize,
    column_path: String,
    encodings: Vec<String>,
    compression: String,
    compressed_size: i64,
    uncompressed_size: i64,
    num_values: i64,
}

/// A parquet data file discovered by listing the store, with the metadata needed
/// to read its footer without any extra stat/head call.
#[derive(Debug)]
struct ListedFile {
    location: StorePath,
    size: u64,
    path: String,
    relative_path: String,
}

/// An asynchronous Parquet reader backed directly by an object store.
///
/// The file size is retained from the listing so footer reads use bounded
/// ranges without an additional stat/head request.
#[derive(Clone, Debug)]
struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: StorePath,
    file_size: u64,
}

impl ObjectStoreReader {
    fn new(store: Arc<dyn ObjectStore>, path: StorePath, file_size: u64) -> Self {
        Self {
            store,
            path,
            file_size,
        }
    }
}

fn to_parquet_error(error: datafusion_object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, ParquetResult<prost::bytes::Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(to_parquet_error)
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<prost::bytes::Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(to_parquet_error)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let file_size = self.file_size;
        async move {
            let metadata = ParquetMetaDataReader::new()
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

pub(super) async fn run_inspect_footer(
    args: InspectFooterArgs,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (destination, root) = match (args.destination, args.root) {
        (Some(_), Some(_)) => {
            return Err("--destination and --root are mutually exclusive; pass exactly one".into());
        }
        (None, None) => return Err("one of --destination or --root is required".into()),
        (Some(path), None) => {
            let destination: DestinationConfig = toml::from_str(&fs::read_to_string(path)?)?;
            (Some(destination), None)
        }
        (None, root) => (None, root),
    };
    let store = build_store(destination.as_ref(), root.as_deref()).await?;
    let root_display = root
        .as_deref()
        .map(|root| root.display().to_string())
        .unwrap_or_else(|| {
            destination
                .as_ref()
                .expect("destination is set when root is not")
                .data_home
                .clone()
        });
    let files =
        collect_parquet_files(store.as_ref(), root.as_deref(), args.include_metadata_files).await?;
    let mut reports = futures::stream::iter(files.iter())
        .map(|file| inspect_file(Arc::clone(&store), file, &args.column))
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;
    reports.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    println!(
        "{}",
        serde_json::to_string_pretty(&FooterReport {
            root: root_display,
            summary: summarize_footer(&args.column, &reports),
            files: reports
        })?
    );
    Ok(())
}

/// Builds an arrow `object_store::ObjectStore` for the given destination. The
/// File backend (and the `--root` shortcut) uses the opendal `Fs` builder
/// directly to avoid the create_dir_all / clean_temp_dir side effects of
/// `object_store::factory::new_fs_object_store`; every other backend goes
/// through `object_store::factory::new_raw_object_store`.
async fn build_store(
    destination: Option<&DestinationConfig>,
    root: Option<&Path>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let operator = match destination {
        Some(destination) => match &destination.object_store {
            ObjectStoreConfig::File(_) => {
                object_store::ObjectStore::new(Fs::default().root(&destination.data_home))?.finish()
            }
            _ => new_raw_object_store(&destination.object_store, &destination.data_home).await?,
        },
        None => {
            let root = root.expect("root must be set when destination is None");
            object_store::ObjectStore::new(Fs::default().root(&root.to_string_lossy()))?.finish()
        }
    };
    Ok(Arc::new(object_store_opendal::OpendalStore::new(operator)))
}

async fn collect_parquet_files(
    store: &dyn ObjectStore,
    root: Option<&Path>,
    include_metadata_files: bool,
) -> Result<Vec<ListedFile>, Box<dyn std::error::Error + Send + Sync>> {
    let metas = store.list(None).try_collect::<Vec<ObjectMeta>>().await?;
    let mut files = Vec::new();
    for meta in metas {
        if !is_parquet_data_file(&meta) {
            continue;
        }
        let is_metadata = meta
            .location
            .parts()
            .any(|part| part.as_ref() == "metadata");
        if !include_metadata_files && is_metadata {
            continue;
        }
        let relative_path = meta.location.to_string();
        let path = match root {
            Some(root) => root.join(&relative_path).display().to_string(),
            None => relative_path.clone(),
        };
        files.push(ListedFile {
            location: meta.location,
            size: meta.size,
            path,
            relative_path,
        });
    }
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(files)
}

/// Keeps only real parquet data files: `.parquet` keys with a non-zero size.
/// Zero-size keys (and keys ending in `/`) are directory markers emitted by
/// some backends and are skipped even when they happen to end in `.parquet`,
/// since an empty parquet file has no readable footer anyway.
fn is_parquet_data_file(meta: &ObjectMeta) -> bool {
    meta.location.extension() == Some("parquet")
        && meta.size > 0
        && !meta.location.as_ref().ends_with('/')
}

async fn inspect_file(
    store: Arc<dyn ObjectStore>,
    file: &ListedFile,
    column: &str,
) -> Result<FooterFileReport, Box<dyn std::error::Error + Send + Sync>> {
    let mut reader = ObjectStoreReader::new(store, file.location.clone(), file.size);
    let metadata = reader.get_metadata(None).await?;
    let file_metadata = metadata.file_metadata();
    let row_groups = metadata.row_groups();
    let mut columns = Vec::new();
    for (row_group_index, rg) in row_groups.iter().enumerate() {
        for (column_index, chunk) in rg.columns().iter().enumerate() {
            let column_path = chunk.column_path().string();
            if column_path == column {
                columns.push(FooterColumnChunkReport {
                    row_group_index,
                    column_index,
                    column_path,
                    encodings: chunk.encodings().map(|e| format!("{e:?}")).collect(),
                    compression: format!("{:?}", chunk.compression()),
                    compressed_size: chunk.compressed_size(),
                    uncompressed_size: chunk.uncompressed_size(),
                    num_values: chunk.num_values(),
                });
            }
        }
    }
    Ok(FooterFileReport {
        path: file.path.clone(),
        relative_path: file.relative_path.clone(),
        file_size: file.size,
        num_rows: file_metadata.num_rows(),
        num_row_groups: row_groups.len(),
        columns,
    })
}
fn summarize_footer(column: &str, files: &[FooterFileReport]) -> FooterSummary {
    let mut encodings = BTreeSet::new();
    let mut s = FooterSummary {
        column: column.to_string(),
        file_count: files.len(),
        ..Default::default()
    };
    for file in files {
        s.total_file_size += file.file_size;
        s.total_rows += file.num_rows;
        if !file.columns.is_empty() {
            s.files_with_column += 1;
        }
        for c in &file.columns {
            s.column_compressed_size += c.compressed_size;
            s.column_uncompressed_size += c.uncompressed_size;
            encodings.extend(c.encodings.iter().cloned());
        }
    }
    s.unique_encodings = encodings.into_iter().collect();
    s
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};

    use parquet::column::writer::ColumnWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use tempfile::TempDir;

    use super::*;

    /// Writes a minimal valid parquet file with a single `value` Int64 column
    /// and `rows` values, so the footer can be read back through the object
    /// store path.
    fn write_parquet_file(path: &Path, rows: i64) {
        let schema = Arc::new(
            parse_message_type("message schema { REQUIRED INT64 value; }").expect("valid schema"),
        );
        let props = Arc::new(WriterProperties::builder().build());
        let file = File::create(path).expect("create parquet file");
        let mut writer = SerializedFileWriter::new(file, schema, props).expect("init writer");
        let mut row_group = writer.next_row_group().expect("next row group");
        let mut column = row_group
            .next_column()
            .expect("next column")
            .expect("has column");
        match column.untyped() {
            ColumnWriter::Int64ColumnWriter(typed) => {
                typed
                    .write_batch(&(1..=rows).collect::<Vec<_>>(), None, None)
                    .expect("write batch");
            }
            _ => panic!("unexpected column writer"),
        }
        column.close().expect("close column");
        row_group.close().expect("close row group");
        writer.close().expect("close writer");
    }

    fn fixture_tree(dir: &Path) {
        // Nested data tree with a `metadata` subdirectory, a non-parquet file
        // and a zero-size fake file.
        let table = dir.join("data/table");
        fs::create_dir_all(&table).expect("create table dir");
        fs::create_dir_all(table.join("metadata")).expect("create metadata dir");
        write_parquet_file(&table.join("0001.parquet"), 3);
        write_parquet_file(&table.join("0002.parquet"), 5);
        write_parquet_file(&table.join("metadata/0001.parquet"), 7);
        fs::write(table.join("notes.txt"), b"not parquet").expect("write notes");
        File::create(table.join("empty.parquet")).expect("create empty file");
    }

    /// Runs the full list -> filter -> footer-read path against a File backend
    /// rooted at `dir` and returns the collected file reports.
    async fn inspect_fixture(dir: &Path, include_metadata_files: bool) -> Vec<FooterFileReport> {
        let root = Some(dir.to_path_buf());
        let store = build_store(None, root.as_deref())
            .await
            .expect("build store");
        let files = collect_parquet_files(store.as_ref(), root.as_deref(), include_metadata_files)
            .await
            .expect("collect files");
        let mut reports = futures::stream::iter(files.iter())
            .map(|file| inspect_file(Arc::clone(&store), file, "value"))
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await
            .expect("inspect files");
        reports.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        reports
    }

    #[tokio::test]
    async fn lists_and_inspects_footer_files_without_metadata_by_default() {
        let dir = TempDir::new().expect("tempdir");
        fixture_tree(dir.path());

        let reports = inspect_fixture(dir.path(), false).await;

        let relative_paths = reports
            .iter()
            .map(|report| report.relative_path.as_str())
            .collect::<Vec<_>>();
        // metadata/0001.parquet excluded, notes.txt and empty.parquet excluded.
        assert_eq!(
            relative_paths,
            vec!["data/table/0001.parquet", "data/table/0002.parquet"]
        );
        assert!(reports.iter().all(|report| report.file_size > 0));
        assert!(reports.iter().all(|report| report.num_row_groups == 1));
        assert!(reports.iter().all(|report| report.num_rows > 0));
        assert!(
            reports
                .iter()
                .all(|report| report.columns.len() == 1 && report.columns[0].column_path == "value")
        );
        // Default writer properties use dictionary encoding, so the column chunk
        // reports PLAIN (dictionary page) plus RLE_DICTIONARY (data pages).
        assert!(
            reports
                .iter()
                .all(|report| report.columns[0].encodings.contains(&"PLAIN".to_string()))
        );
        assert!(
            reports
                .iter()
                .all(|report| report.columns[0].compression == "UNCOMPRESSED")
        );
    }

    #[tokio::test]
    async fn includes_metadata_files_when_requested() {
        let dir = TempDir::new().expect("tempdir");
        fixture_tree(dir.path());

        let reports = inspect_fixture(dir.path(), true).await;

        let relative_paths = reports
            .iter()
            .map(|report| report.relative_path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            relative_paths,
            vec![
                "data/table/0001.parquet",
                "data/table/0002.parquet",
                "data/table/metadata/0001.parquet"
            ]
        );
        let metadata_report = reports
            .iter()
            .find(|report| report.relative_path == "data/table/metadata/0001.parquet")
            .expect("metadata report");
        assert_eq!(metadata_report.num_rows, 7);
    }

    #[tokio::test]
    async fn accepts_a_file_destination_toml() {
        let dir = TempDir::new().expect("tempdir");
        fixture_tree(dir.path());
        let destination = dir.path().join("destination.toml");
        fs::write(
            &destination,
            format!(
                "data_home = \"{}\"\n[object_store]\ntype = \"File\"\n",
                dir.path().display()
            ),
        )
        .expect("write destination toml");

        let destination: DestinationConfig =
            toml::from_str(&fs::read_to_string(&destination).expect("read toml"))
                .expect("parse toml");
        let store = build_store(Some(&destination), None)
            .await
            .expect("build store");
        let files = collect_parquet_files(store.as_ref(), None, false)
            .await
            .expect("collect files");

        // Destination mode has no root prefix: paths are store-relative keys.
        assert_eq!(
            files
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["data/table/0001.parquet", "data/table/0002.parquet"]
        );
        assert!(files.iter().all(|file| file.path == file.relative_path));
    }

    #[test]
    fn requires_exactly_one_of_root_or_destination() {
        use futures::FutureExt;

        let both = run_inspect_footer(InspectFooterArgs {
            root: Some(PathBuf::from("/tmp/root")),
            destination: Some(PathBuf::from("/tmp/destination.toml")),
            column: "value".to_string(),
            include_metadata_files: false,
        })
        .now_or_never();
        assert!(matches!(both, Some(Err(_))));

        let neither = run_inspect_footer(InspectFooterArgs {
            root: None,
            destination: None,
            column: "value".to_string(),
            include_metadata_files: false,
        })
        .now_or_never();
        assert!(matches!(neither, Some(Err(_))));
    }
}
