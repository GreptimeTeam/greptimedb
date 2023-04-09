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

use arrow::csv::reader::infer_reader_schema as infer_csv_schema;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::compression::CompressionType;
use crate::error::{self, Result};
use crate::file_format::{self, FileFormat};
use crate::util::io::SyncIoBridge;

#[derive(Debug)]
pub struct CsvFormat {
    pub has_header: bool,
    pub delimiter: u8,
    pub schema_infer_max_record: Option<usize>,
    pub compression_type: CompressionType,
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            schema_infer_max_record: Some(file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD),
            compression_type: CompressionType::UNCOMPRESSED,
        }
    }
}

#[async_trait]
impl FileFormat for CsvFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: String) -> Result<SchemaRef> {
        let reader = store
            .reader(&path)
            .await
            .context(error::ReadObjectSnafu { path: &path })?;

        let decoded = self.compression_type.convert_async_read(reader);

        let reader = SyncIoBridge::new(decoded);

        let (schema, _records_read) = infer_csv_schema(
            reader,
            self.delimiter,
            self.schema_infer_max_record,
            self.has_header,
        )
        .context(error::InferSchemaSnafu { path: &path })?;

        Ok(Arc::new(schema))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::file_format::FileFormat;
    use crate::test_util::{self, format_schema, test_store};

    fn test_data_root() -> String {
        test_util::get_data_dir("tests/csv").display().to_string()
    }

    #[tokio::test]
    async fn infer_schema_basic() {
        let csv = CsvFormat::default();
        let store = test_store(&test_data_root());
        let schema = csv
            .infer_schema(&store, "simple.csv".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "c1: Utf8",
                "c2: Int64",
                "c3: Int64",
                "c4: Int64",
                "c5: Int64",
                "c6: Int64",
                "c7: Int64",
                "c8: Int64",
                "c9: Int64",
                "c10: Int64",
                "c11: Float64",
                "c12: Float64",
                "c13: Utf8"
            ],
            formatted,
        );
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let json = CsvFormat {
            schema_infer_max_record: Some(3),
            ..CsvFormat::default()
        };
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "schema_infer_limit.csv".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec!["a: Int64", "b: Float64", "c: Int64", "d: Int64"],
            formatted
        );

        let json = CsvFormat::default();
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "schema_infer_limit.csv".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec!["a: Int64", "b: Float64", "c: Int64", "d: Utf8"],
            formatted
        );
    }
}
