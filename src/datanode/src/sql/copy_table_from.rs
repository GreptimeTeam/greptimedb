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

use async_compat::CompatExt;
use common_query::Output;
use common_recordbatch::error::DataTypesSnafu;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::{Helper, VectorRef};
use futures::future;
use futures_util::TryStreamExt;
use object_store::services::{Fs, S3};
use object_store::{Object, ObjectStore, ObjectStoreBuilder};
use regex::Regex;
use snafu::{ensure, ResultExt};
use table::engine::TableReference;
use table::requests::{CopyTableFromRequest, InsertRequest};
use tokio::io::BufReader;
use url::{ParseError, Url};

use crate::error::{self, Result};
use crate::sql::SqlHandler;

const S3_SCHEMA: &str = "S3";
const ENDPOINT_URL: &str = "ENDPOINT_URL";
const ACCESS_KEY_ID: &str = "ACCESS_KEY_ID";
const SECRET_ACCESS_KEY: &str = "SECRET_ACCESS_KEY";
const SESSION_TOKEN: &str = "SESSION_TOKEN";
const REGION: &str = "REGION";
const ENABLE_VIRTUAL_HOST_STYLE: &str = "ENABLE_VIRTUAL_HOST_STYLE";

impl SqlHandler {
    pub(crate) async fn copy_table_from(&self, req: CopyTableFromRequest) -> Result<Output> {
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };
        let table = self.get_table(&table_ref)?;

        let datasource = DataSource::new(&req.from, req.pattern, req.connection)?;

        let objects = datasource.list().await?;

        let mut buf: Vec<RecordBatch> = Vec::new();

        for obj in objects.iter() {
            let reader = obj.reader().await.context(error::ReadObjectSnafu {
                path: &obj.path().to_string(),
            })?;

            let buf_reader = BufReader::new(reader.compat());

            let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
                .await
                .context(error::ReadParquetSnafu)?;

            ensure!(
                builder.schema() == table.schema().arrow_schema(),
                error::InvalidSchemaSnafu {
                    table_schema: table.schema().arrow_schema().to_string(),
                    file_schema: (*(builder.schema())).to_string()
                }
            );

            let stream = builder
                .build()
                .context(error::BuildParquetRecordBatchStreamSnafu)?;

            let chunk = stream
                .try_collect::<Vec<_>>()
                .await
                .context(error::ReadParquetSnafu)?;

            buf.extend(chunk.into_iter());
        }

        let fields = table
            .schema()
            .arrow_schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();

        // Vec<Columns>
        let column_chunks = buf
            .into_iter()
            .map(|c| Helper::try_into_vectors(c.columns()).context(DataTypesSnafu))
            .collect::<Vec<_>>();

        let mut futs = Vec::with_capacity(column_chunks.len());

        for column_chunk in column_chunks.into_iter() {
            let column_chunk = column_chunk.context(error::ParseDataTypesSnafu)?;
            let columns_values = fields
                .iter()
                .cloned()
                .zip(column_chunk.into_iter())
                .collect::<HashMap<String, VectorRef>>();

            futs.push(table.insert(InsertRequest {
                catalog_name: req.catalog_name.to_string(),
                schema_name: req.schema_name.to_string(),
                table_name: req.table_name.to_string(),
                columns_values,
                //TODO: support multi-regions
                region_number: 0,
            }))
        }

        let result = futures::future::try_join_all(futs)
            .await
            .context(error::InsertSnafu {
                table_name: req.table_name.to_string(),
            })?;

        Ok(Output::AffectedRows(result.iter().sum()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    Filename(String),
    Dir,
}

struct DataSource {
    object_store: ObjectStore,
    source: Source,
    path: String,
    regex: Option<Regex>,
}

impl DataSource {
    fn from_path(url: &str, regex: Option<Regex>) -> Result<DataSource> {
        let result = if url.ends_with('/') {
            Url::from_directory_path(url)
        } else {
            Url::from_file_path(url)
        };

        match result {
            Ok(url) => {
                let path = url.path();

                let (path, filename) = DataSource::find_dir_and_filename(path);

                let source = if let Some(filename) = filename {
                    Source::Filename(filename)
                } else {
                    Source::Dir
                };

                let accessor = Fs::default()
                    .root(&path)
                    .build()
                    .context(error::BuildBackendSnafu)?;

                Ok(DataSource {
                    object_store: ObjectStore::new(accessor).finish(),
                    source,
                    path,
                    regex,
                })
            }
            Err(()) => error::InvalidPathSnafu {
                path: url.to_string(),
            }
            .fail(),
        }
    }

    fn build_s3_backend(
        host: Option<&str>,
        path: &str,
        connection: HashMap<String, String>,
    ) -> Result<ObjectStore> {
        let mut builder = S3::default();

        builder.root(path);

        if let Some(bucket) = host {
            builder.bucket(bucket);
        }

        if let Some(endpoint) = connection.get(ENDPOINT_URL) {
            builder.endpoint(endpoint);
        }

        if let Some(region) = connection.get(REGION) {
            builder.region(region);
        }

        if let Some(key_id) = connection.get(ACCESS_KEY_ID) {
            builder.access_key_id(key_id);
        }

        if let Some(key) = connection.get(SECRET_ACCESS_KEY) {
            builder.secret_access_key(key);
        }

        if let Some(session_token) = connection.get(SESSION_TOKEN) {
            builder.security_token(session_token);
        }

        if let Some(enable_str) = connection.get(ENABLE_VIRTUAL_HOST_STYLE) {
            let enable = enable_str.as_str().parse::<bool>().map_err(|e| {
                error::InvalidConnectionSnafu {
                    msg: format!(
                        "failed to parse the option {}={}, {}",
                        ENABLE_VIRTUAL_HOST_STYLE, enable_str, e
                    ),
                }
                .build()
            })?;
            if enable {
                builder.enable_virtual_host_style();
            }
        }

        let accessor = builder.build().context(error::BuildBackendSnafu)?;

        Ok(ObjectStore::new(accessor).finish())
    }

    fn from_url(
        url: Url,
        regex: Option<Regex>,
        connection: HashMap<String, String>,
    ) -> Result<DataSource> {
        let host = url.host_str();

        let path = url.path();

        let schema = url.scheme();

        let (dir, filename) = DataSource::find_dir_and_filename(path);

        let source = if let Some(filename) = filename {
            Source::Filename(filename)
        } else {
            Source::Dir
        };

        let object_store = match schema.to_uppercase().as_str() {
            S3_SCHEMA => DataSource::build_s3_backend(host, &dir, connection)?,
            _ => {
                return error::UnsupportedBackendProtocolSnafu {
                    protocol: schema.to_string(),
                }
                .fail()
            }
        };

        Ok(DataSource {
            object_store,
            source,
            path: dir,
            regex,
        })
    }

    pub fn new(
        url: &str,
        pattern: Option<String>,
        connection: HashMap<String, String>,
    ) -> Result<DataSource> {
        let regex = if let Some(pattern) = pattern {
            let regex = Regex::new(&pattern).context(error::BuildRegexSnafu)?;
            Some(regex)
        } else {
            None
        };
        let result = Url::parse(url);

        match result {
            Ok(url) => DataSource::from_url(url, regex, connection),
            Err(err) => {
                if ParseError::RelativeUrlWithoutBase == err {
                    DataSource::from_path(url, regex)
                } else {
                    Err(error::Error::InvalidUrl {
                        url: url.to_string(),
                        source: err,
                    })
                }
            }
        }
    }

    pub async fn list(&self) -> Result<Vec<Object>> {
        match &self.source {
            Source::Dir => {
                let streamer = self
                    .object_store
                    .object("/")
                    .list()
                    .await
                    .context(error::ListObjectsSnafu { path: &self.path })?;
                streamer
                    .try_filter(|f| {
                        let res = if let Some(regex) = &self.regex {
                            regex.is_match(f.name())
                        } else {
                            true
                        };
                        future::ready(res)
                    })
                    .try_collect::<Vec<_>>()
                    .await
                    .context(error::ListObjectsSnafu { path: &self.path })
            }
            Source::Filename(filename) => {
                let obj = self.object_store.object(filename);

                Ok(vec![obj])
            }
        }
    }

    fn find_dir_and_filename(path: &str) -> (String, Option<String>) {
        if path.is_empty() {
            ("/".to_string(), None)
        } else if path.ends_with('/') {
            (path.to_string(), None)
        } else if let Some(idx) = path.rfind('/') {
            (
                path[..idx + 1].to_string(),
                Some(path[idx + 1..].to_string()),
            )
        } else {
            ("/".to_string(), Some(path.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {

    use url::Url;

    use super::*;
    #[test]
    fn test_parse_uri() {
        struct Test<'a> {
            uri: &'a str,
            expected_path: &'a str,
            expected_schema: &'a str,
        }

        let tests = [
            Test {
                uri: "s3://bucket/to/path/",
                expected_path: "/to/path/",
                expected_schema: "s3",
            },
            Test {
                uri: "fs:///to/path/",
                expected_path: "/to/path/",
                expected_schema: "fs",
            },
            Test {
                uri: "fs:///to/path/file",
                expected_path: "/to/path/file",
                expected_schema: "fs",
            },
        ];
        for test in tests {
            let parsed_uri = Url::parse(test.uri).unwrap();
            assert_eq!(parsed_uri.path(), test.expected_path);
            assert_eq!(parsed_uri.scheme(), test.expected_schema);
        }
    }

    #[test]
    fn test_parse_path_and_dir() {
        let parsed = Url::from_file_path("/to/path/file").unwrap();
        assert_eq!(parsed.path(), "/to/path/file");

        let parsed = Url::from_directory_path("/to/path/").unwrap();
        assert_eq!(parsed.path(), "/to/path/");
    }

    #[test]
    fn test_find_dir_and_filename() {
        struct Test<'a> {
            path: &'a str,
            expected_dir: &'a str,
            expected_filename: Option<String>,
        }

        let tests = [
            Test {
                path: "to/path/",
                expected_dir: "to/path/",
                expected_filename: None,
            },
            Test {
                path: "to/path/filename",
                expected_dir: "to/path/",
                expected_filename: Some("filename".into()),
            },
            Test {
                path: "/to/path/filename",
                expected_dir: "/to/path/",
                expected_filename: Some("filename".into()),
            },
            Test {
                path: "/",
                expected_dir: "/",
                expected_filename: None,
            },
            Test {
                path: "filename",
                expected_dir: "/",
                expected_filename: Some("filename".into()),
            },
            Test {
                path: "",
                expected_dir: "/",
                expected_filename: None,
            },
        ];

        for test in tests {
            let (path, filename) = DataSource::find_dir_and_filename(test.path);
            assert_eq!(test.expected_dir, path);
            assert_eq!(test.expected_filename, filename)
        }
    }
}
