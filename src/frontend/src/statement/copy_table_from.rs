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
use std::future::Future;

use async_compat::CompatExt;
use common_base::readable_size::ReadableSize;
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::{build_backend, parse_url};
use common_datasource::util::find_dir_and_filename;
use common_query::Output;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use datatypes::vectors::Helper;
use futures_util::StreamExt;
use regex::Regex;
use snafu::ResultExt;
use table::engine::TableReference;
use table::requests::{CopyTableRequest, InsertRequest};
use tokio::io::BufReader;

use crate::error::{self, IntoVectorsSnafu, Result};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    pub(crate) async fn copy_table_from(&self, req: CopyTableRequest) -> Result<Output> {
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };
        let table = self.get_table(&table_ref).await?;

        let (_schema, _host, path) = parse_url(&req.location).context(error::ParseUrlSnafu)?;

        let object_store =
            build_backend(&req.location, req.connection).context(error::BuildBackendSnafu)?;

        let (dir, filename) = find_dir_and_filename(&path);
        let regex = req
            .pattern
            .as_ref()
            .map(|x| Regex::new(x))
            .transpose()
            .context(error::BuildRegexSnafu)?;

        let source = if let Some(filename) = filename {
            Source::Filename(filename)
        } else {
            Source::Dir
        };

        let lister = Lister::new(object_store.clone(), source, dir, regex);

        let entries = lister.list().await.context(error::ListObjectsSnafu)?;

        let fields = table
            .schema()
            .arrow_schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();

        let mut rows_inserted = 0;
        for entry in entries.iter() {
            let path = entry.path();
            let reader = object_store
                .reader(path)
                .await
                .context(error::ReadObjectSnafu { path })?;

            let buf_reader = BufReader::new(reader.compat());

            let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
                .await
                .context(error::ReadParquetSnafu)?;

            ensure_schema_matches_ignore_timezone(builder.schema(), table.schema().arrow_schema())?;

            let mut stream = builder
                .build()
                .context(error::BuildParquetRecordBatchStreamSnafu)?;

            // TODO(hl): make this configurable through options.
            let pending_mem_threshold = ReadableSize::mb(32).as_bytes();
            let mut pending_mem_size = 0;
            let mut pending = vec![];

            while let Some(r) = stream.next().await {
                let record_batch = r.context(error::ReadParquetSnafu)?;
                let vectors =
                    Helper::try_into_vectors(record_batch.columns()).context(IntoVectorsSnafu)?;

                pending_mem_size += vectors.iter().map(|v| v.memory_size()).sum::<usize>();

                let columns_values = fields
                    .iter()
                    .cloned()
                    .zip(vectors.into_iter())
                    .collect::<HashMap<_, _>>();

                pending.push(table.insert(InsertRequest {
                    catalog_name: req.catalog_name.to_string(),
                    schema_name: req.schema_name.to_string(),
                    table_name: req.table_name.to_string(),
                    columns_values,
                    //TODO: support multi-regions
                    region_number: 0,
                }));

                if pending_mem_size as u64 >= pending_mem_threshold {
                    rows_inserted +=
                        batch_insert(&mut pending, &mut pending_mem_size, &req.table_name).await?;
                }
            }

            if !pending.is_empty() {
                rows_inserted +=
                    batch_insert(&mut pending, &mut pending_mem_size, &req.table_name).await?;
            }
        }

        Ok(Output::AffectedRows(rows_inserted))
    }
}

/// Executes all pending inserts all at once, drain pending requests and reset pending bytes.
async fn batch_insert(
    pending: &mut Vec<impl Future<Output = table::error::Result<usize>>>,
    pending_bytes: &mut usize,
    table_name: &str,
) -> Result<usize> {
    let batch = pending.drain(..);
    let res: usize = futures::future::try_join_all(batch)
        .await
        .context(error::InsertSnafu { table_name })?
        .iter()
        .sum();
    *pending_bytes = 0;
    Ok(res)
}

fn ensure_schema_matches_ignore_timezone(left: &SchemaRef, right: &SchemaRef) -> Result<()> {
    let not_match = left
        .fields
        .iter()
        .zip(right.fields.iter())
        .map(|(l, r)| (l.data_type(), r.data_type()))
        .enumerate()
        .find(|(_, (l, r))| !data_type_equals_ignore_timezone(l, r));

    if let Some((index, _)) = not_match {
        error::InvalidSchemaSnafu {
            index,
            table_schema: left.to_string(),
            file_schema: right.to_string(),
        }
        .fail()
    } else {
        Ok(())
    }
}

fn data_type_equals_ignore_timezone(l: &DataType, r: &DataType) -> bool {
    match (l, r) {
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            a.is_nullable() == b.is_nullable()
                && data_type_equals_ignore_timezone(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, a_size), DataType::FixedSizeList(b, b_size)) => {
            a_size == b_size
                && a.is_nullable() == b.is_nullable()
                && data_type_equals_ignore_timezone(a.data_type(), b.data_type())
        }
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter().zip(b).all(|(a, b)| {
                    a.is_nullable() == b.is_nullable()
                        && data_type_equals_ignore_timezone(a.data_type(), b.data_type())
                })
        }
        (DataType::Map(a_field, a_is_sorted), DataType::Map(b_field, b_is_sorted)) => {
            a_field == b_field && a_is_sorted == b_is_sorted
        }
        (DataType::Timestamp(l_unit, _), DataType::Timestamp(r_unit, _)) => l_unit == r_unit,
        _ => l == r,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::datatypes::{Field, Schema};

    use super::*;

    fn test_schema_matches(l: (DataType, bool), r: (DataType, bool), matches: bool) {
        let s1 = Arc::new(Schema::new(vec![Field::new("col", l.0, l.1)]));
        let s2 = Arc::new(Schema::new(vec![Field::new("col", r.0, r.1)]));
        let res = ensure_schema_matches_ignore_timezone(&s1, &s2);
        assert_eq!(matches, res.is_ok())
    }

    #[test]
    fn test_ensure_datatype_matches_ignore_timezone() {
        test_schema_matches(
            (
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                true,
            ),
            (
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("PDT".into()),
                ),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Millisecond,
                    Some("UTC".into()),
                ),
                true,
            ),
            false,
        );

        test_schema_matches((DataType::Int8, true), (DataType::Int8, true), true);

        test_schema_matches((DataType::Int8, true), (DataType::Int16, true), false);
    }
}
