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

use api::v1::{
    ColumnSchema, PrimaryKeyEncoding as PrimaryKeyEncodingProto, Row, Rows, Value, WriteHint,
};
use common_telemetry::{error, info};
use fxhash::FxHashMap;
use snafu::{OptionExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_request::{
    AffectedRows, RegionDeleteRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{RegionId, TableId};

use crate::engine::MetricEngineInner;
use crate::error::{
    ColumnNotFoundSnafu, ForbiddenPhysicalAlterSnafu, InvalidRequestSnafu,
    LogicalRegionNotFoundSnafu, PhysicalRegionNotFoundSnafu, Result, UnsupportedRegionRequestSnafu,
};
use crate::metrics::{FORBIDDEN_OPERATION_COUNT, MITO_OPERATION_ELAPSED};
use crate::row_modifier::{RowsIter, TableIdInput};
use crate::utils::to_data_region_id;

impl MetricEngineInner {
    /// Dispatch region put request
    pub async fn put_region(
        &self,
        region_id: RegionId,
        request: RegionPutRequest,
    ) -> Result<AffectedRows> {
        let is_putting_physical_region =
            self.state.read().unwrap().exist_physical_region(region_id);

        if is_putting_physical_region {
            info!(
                "Metric region received put request {request:?} on physical region {region_id:?}"
            );
            FORBIDDEN_OPERATION_COUNT.inc();

            ForbiddenPhysicalAlterSnafu.fail()
        } else {
            self.put_logical_region(region_id, request).await
        }
    }

    /// Batch write multiple logical regions to the same physical region.
    ///
    /// Dispatch region put requests in batch.
    ///
    /// Requests may span multiple physical regions. We group them by physical
    /// region and write sequentially. This method fails fast on validation or
    /// preparation errors within a group and stops at the first failure.
    /// Writes in earlier physical-region groups are not rolled back if a later
    /// group fails.
    pub async fn put_regions_batch(
        &self,
        requests: impl ExactSizeIterator<Item = (RegionId, RegionPutRequest)>,
    ) -> Result<AffectedRows> {
        let len = requests.len();

        if len == 0 {
            return Ok(0);
        }

        let _timer = MITO_OPERATION_ELAPSED
            .with_label_values(&["put_batch"])
            .start_timer();

        // Fast path: single request, no batching overhead
        if len == 1 {
            let (logical_id, req) = requests.into_iter().next().unwrap();
            return self.put_logical_region(logical_id, req).await;
        }

        let mut requests_per_physical: HashMap<RegionId, Vec<(RegionId, RegionPutRequest)>> =
            HashMap::new();
        for (logical_region_id, request) in requests {
            let physical_region_id = self.find_physical_region_id(logical_region_id)?;
            requests_per_physical
                .entry(physical_region_id)
                .or_default()
                .push((logical_region_id, request));
        }

        let mut total_affected_rows: AffectedRows = 0;
        for (physical_region_id, requests) in requests_per_physical {
            let affected_rows = self
                .put_regions_batch_single_physical(physical_region_id, requests)
                .await?;
            total_affected_rows += affected_rows;
        }

        Ok(total_affected_rows)
    }

    /// Write a batch of requests that all belong to the same physical region.
    ///
    /// This function orchestrates the batch write process:
    /// 1. Validates all requests
    /// 2. Merges requests according to the encoding strategy (sparse or dense)
    /// 3. Writes the merged batch to the physical region
    async fn put_regions_batch_single_physical(
        &self,
        physical_region_id: RegionId,
        requests: Vec<(RegionId, RegionPutRequest)>,
    ) -> Result<AffectedRows> {
        if requests.is_empty() {
            return Ok(0);
        }

        let data_region_id = to_data_region_id(physical_region_id);
        let primary_key_encoding = self.get_primary_key_encoding(data_region_id)?;

        // Validate all requests
        self.validate_batch_requests(physical_region_id, &requests)
            .await?;

        // Merge requests according to encoding strategy
        let (merged_request, total_affected_rows) = match primary_key_encoding {
            PrimaryKeyEncoding::Sparse => self.merge_sparse_batch(physical_region_id, requests)?,
            PrimaryKeyEncoding::Dense => self.merge_dense_batch(data_region_id, requests)?,
        };

        // Write once to the physical region
        self.data_region
            .write_data(data_region_id, RegionRequest::Put(merged_request))
            .await?;

        Ok(total_affected_rows)
    }

    /// Get primary key encoding for a data region.
    fn get_primary_key_encoding(&self, data_region_id: RegionId) -> Result<PrimaryKeyEncoding> {
        let state = self.state.read().unwrap();
        state
            .get_primary_key_encoding(data_region_id)
            .context(PhysicalRegionNotFoundSnafu {
                region_id: data_region_id,
            })
    }

    /// Validates all requests in a batch.
    async fn validate_batch_requests(
        &self,
        physical_region_id: RegionId,
        requests: &[(RegionId, RegionPutRequest)],
    ) -> Result<()> {
        for (logical_region_id, request) in requests {
            self.verify_rows(*logical_region_id, physical_region_id, &request.rows)
                .await?;
        }
        Ok(())
    }

    /// Merges multiple requests using sparse primary key encoding.
    fn merge_sparse_batch(
        &self,
        physical_region_id: RegionId,
        requests: Vec<(RegionId, RegionPutRequest)>,
    ) -> Result<(RegionPutRequest, AffectedRows)> {
        let total_rows: usize = requests.iter().map(|(_, req)| req.rows.rows.len()).sum();
        let mut merged_rows = Vec::with_capacity(total_rows);
        let mut total_affected_rows: AffectedRows = 0;
        let mut output_schema: Option<Vec<ColumnSchema>> = None;
        let mut merged_version: Option<u64> = None;

        // Modify and collect rows from each request
        for (logical_region_id, mut request) in requests {
            if let Some(request_version) = request.partition_expr_version {
                if let Some(merged_version) = merged_version {
                    ensure!(
                        merged_version == request_version,
                        InvalidRequestSnafu {
                            region_id: physical_region_id,
                            reason: "inconsistent partition expr version in batch"
                        }
                    );
                } else {
                    merged_version = Some(request_version);
                }
            }
            self.modify_rows(
                physical_region_id,
                logical_region_id.table_id(),
                &mut request.rows,
                PrimaryKeyEncoding::Sparse,
            )?;

            let row_count = request.rows.rows.len();
            total_affected_rows += row_count as AffectedRows;

            // Capture the output schema from the first modified request
            if output_schema.is_none() {
                output_schema = Some(request.rows.schema.clone());
            }

            merged_rows.extend(request.rows.rows);
        }

        // Safe to unwrap: requests is guaranteed non-empty by caller
        let schema = output_schema.unwrap();

        let merged_request = RegionPutRequest {
            rows: Rows {
                schema,
                rows: merged_rows,
            },
            hint: Some(WriteHint {
                primary_key_encoding: PrimaryKeyEncodingProto::Sparse.into(),
            }),
            partition_expr_version: merged_version,
        };

        Ok((merged_request, total_affected_rows))
    }

    /// Merges multiple requests using dense primary key encoding.
    ///
    /// In dense mode, different requests can have different columns.
    /// We merge all schemas into a union schema, align each row to this schema,
    /// then batch-modify all rows together (adding __table_id and __tsid).
    fn merge_dense_batch(
        &self,
        data_region_id: RegionId,
        requests: Vec<(RegionId, RegionPutRequest)>,
    ) -> Result<(RegionPutRequest, AffectedRows)> {
        // Build union schema from all requests
        let merged_schema = Self::build_union_schema(&requests);

        // Align all rows to the merged schema and collect table_ids
        let (merged_rows, table_ids, merged_version) =
            Self::align_requests_to_schema(requests, &merged_schema)?;

        // Batch-modify all rows (add __table_id and __tsid columns)
        let final_rows = {
            let state = self.state.read().unwrap();
            let name_to_id = state
                .physical_region_states()
                .get(&data_region_id)
                .with_context(|| PhysicalRegionNotFoundSnafu {
                    region_id: data_region_id,
                })?
                .physical_columns();

            let iter = RowsIter::new(
                Rows {
                    schema: merged_schema,
                    rows: merged_rows,
                },
                name_to_id,
            );

            self.row_modifier.modify_rows(
                iter,
                TableIdInput::Batch(&table_ids),
                PrimaryKeyEncoding::Dense,
            )?
        };

        let merged_request = RegionPutRequest {
            rows: final_rows,
            hint: None,
            partition_expr_version: merged_version,
        };

        Ok((merged_request, table_ids.len() as AffectedRows))
    }

    /// Builds a union schema containing all columns from all requests.
    fn build_union_schema(requests: &[(RegionId, RegionPutRequest)]) -> Vec<ColumnSchema> {
        let mut schema_map: HashMap<&str, ColumnSchema> = HashMap::new();
        for (_, request) in requests {
            for col in &request.rows.schema {
                schema_map
                    .entry(col.column_name.as_str())
                    .or_insert_with(|| col.clone());
            }
        }
        schema_map.into_values().collect()
    }

    fn align_requests_to_schema(
        requests: Vec<(RegionId, RegionPutRequest)>,
        merged_schema: &[ColumnSchema],
    ) -> Result<(Vec<Row>, Vec<TableId>, Option<u64>)> {
        // Pre-calculate total capacity
        let total_rows: usize = requests.iter().map(|(_, req)| req.rows.rows.len()).sum();
        let mut merged_rows = Vec::with_capacity(total_rows);
        let mut table_ids = Vec::with_capacity(total_rows);
        let mut merged_version: Option<u64> = None;

        let null_value = Value { value_data: None };

        for (logical_region_id, request) in requests {
            if let Some(request_version) = request.partition_expr_version {
                if let Some(merged_version) = merged_version {
                    ensure!(
                        merged_version == request_version,
                        InvalidRequestSnafu {
                            region_id: logical_region_id,
                            reason: "inconsistent partition expr version in batch"
                        }
                    );
                } else {
                    merged_version = Some(request_version);
                }
            }
            let table_id = logical_region_id.table_id();

            // Build column name to index mapping once per request
            let col_name_to_idx: FxHashMap<&str, usize> = request
                .rows
                .schema
                .iter()
                .enumerate()
                .map(|(idx, col)| (col.column_name.as_str(), idx))
                .collect();

            // Build column mapping array once per request
            // col_mapping[i] = Some(idx) means merged_schema[i] is at request.schema[idx]
            // col_mapping[i] = None means merged_schema[i] doesn't exist in request.schema
            let col_mapping: Vec<Option<usize>> = merged_schema
                .iter()
                .map(|merged_col| {
                    col_name_to_idx
                        .get(merged_col.column_name.as_str())
                        .copied()
                })
                .collect();

            // Apply the mapping to all rows
            for mut row in request.rows.rows {
                let mut aligned_values = Vec::with_capacity(merged_schema.len());
                for &opt_idx in &col_mapping {
                    aligned_values.push(match opt_idx {
                        Some(idx) => std::mem::take(&mut row.values[idx]),
                        None => null_value.clone(),
                    });
                }
                merged_rows.push(Row {
                    values: aligned_values,
                });
                table_ids.push(table_id);
            }
        }

        Ok((merged_rows, table_ids, merged_version))
    }

    /// Find the physical region id for a logical region.
    fn find_physical_region_id(&self, logical_region_id: RegionId) -> Result<RegionId> {
        let state = self.state.read().unwrap();
        state
            .logical_regions()
            .get(&logical_region_id)
            .copied()
            .context(LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            })
    }

    /// Dispatch region delete request
    pub async fn delete_region(
        &self,
        region_id: RegionId,
        request: RegionDeleteRequest,
    ) -> Result<AffectedRows> {
        if self.is_physical_region(region_id) {
            info!(
                "Metric region received delete request {request:?} on physical region {region_id:?}"
            );
            FORBIDDEN_OPERATION_COUNT.inc();

            UnsupportedRegionRequestSnafu {
                request: RegionRequest::Delete(request),
            }
            .fail()
        } else {
            self.delete_logical_region(region_id, request).await
        }
    }

    async fn put_logical_region(
        &self,
        logical_region_id: RegionId,
        mut request: RegionPutRequest,
    ) -> Result<AffectedRows> {
        let _timer = MITO_OPERATION_ELAPSED
            .with_label_values(&["put"])
            .start_timer();

        let (physical_region_id, data_region_id, primary_key_encoding) =
            self.find_data_region_meta(logical_region_id)?;

        self.verify_rows(logical_region_id, physical_region_id, &request.rows)
            .await?;

        // write to data region
        // TODO: retrieve table name
        self.modify_rows(
            physical_region_id,
            logical_region_id.table_id(),
            &mut request.rows,
            primary_key_encoding,
        )?;
        if primary_key_encoding == PrimaryKeyEncoding::Sparse {
            request.hint = Some(WriteHint {
                primary_key_encoding: PrimaryKeyEncodingProto::Sparse.into(),
            });
        }
        self.data_region
            .write_data(data_region_id, RegionRequest::Put(request))
            .await
    }

    async fn delete_logical_region(
        &self,
        logical_region_id: RegionId,
        mut request: RegionDeleteRequest,
    ) -> Result<AffectedRows> {
        let _timer = MITO_OPERATION_ELAPSED
            .with_label_values(&["delete"])
            .start_timer();

        let (physical_region_id, data_region_id, primary_key_encoding) =
            self.find_data_region_meta(logical_region_id)?;

        self.verify_rows(logical_region_id, physical_region_id, &request.rows)
            .await?;

        // write to data region
        // TODO: retrieve table name
        self.modify_rows(
            physical_region_id,
            logical_region_id.table_id(),
            &mut request.rows,
            primary_key_encoding,
        )?;
        if primary_key_encoding == PrimaryKeyEncoding::Sparse {
            request.hint = Some(WriteHint {
                primary_key_encoding: PrimaryKeyEncodingProto::Sparse.into(),
            });
        }
        self.data_region
            .write_data(data_region_id, RegionRequest::Delete(request))
            .await
    }

    fn find_data_region_meta(
        &self,
        logical_region_id: RegionId,
    ) -> Result<(RegionId, RegionId, PrimaryKeyEncoding)> {
        let state = self.state.read().unwrap();
        let physical_region_id = *state
            .logical_regions()
            .get(&logical_region_id)
            .with_context(|| LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            })?;
        let data_region_id = to_data_region_id(physical_region_id);
        let primary_key_encoding = state.get_primary_key_encoding(data_region_id).context(
            PhysicalRegionNotFoundSnafu {
                region_id: data_region_id,
            },
        )?;
        Ok((physical_region_id, data_region_id, primary_key_encoding))
    }

    /// Verifies a request for a logical region against its corresponding metadata region.
    ///
    /// Includes:
    /// - Check if the logical region exists
    /// - Check if the columns exist
    async fn verify_rows(
        &self,
        logical_region_id: RegionId,
        physical_region_id: RegionId,
        rows: &Rows,
    ) -> Result<()> {
        // Check if the region exists
        let data_region_id = to_data_region_id(physical_region_id);
        let state = self.state.read().unwrap();
        if !state.is_logical_region_exist(logical_region_id) {
            error!("Trying to write to an nonexistent region {logical_region_id}");
            return LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            }
            .fail();
        }

        // Check if a physical column exists
        let physical_columns = state
            .physical_region_states()
            .get(&data_region_id)
            .context(PhysicalRegionNotFoundSnafu {
                region_id: data_region_id,
            })?
            .physical_columns();
        for col in &rows.schema {
            ensure!(
                physical_columns.contains_key(&col.column_name),
                ColumnNotFoundSnafu {
                    name: col.column_name.clone(),
                    region_id: logical_region_id,
                }
            );
        }

        Ok(())
    }

    /// Perform metric engine specific logic to incoming rows.
    /// - Add table_id column
    /// - Generate tsid
    fn modify_rows(
        &self,
        physical_region_id: RegionId,
        table_id: TableId,
        rows: &mut Rows,
        encoding: PrimaryKeyEncoding,
    ) -> Result<()> {
        let input = std::mem::take(rows);
        let iter = {
            let state = self.state.read().unwrap();
            let name_to_id = state
                .physical_region_states()
                .get(&physical_region_id)
                .with_context(|| PhysicalRegionNotFoundSnafu {
                    region_id: physical_region_id,
                })?
                .physical_columns();
            RowsIter::new(input, name_to_id)
        };
        let output =
            self.row_modifier
                .modify_rows(iter, TableIdInput::Single(table_id), encoding)?;
        *rows = output;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_function::utils::partition_expr_version;
    use common_recordbatch::RecordBatches;
    use datatypes::value::Value as PartitionValue;
    use partition::expr::col;
    use store_api::metric_engine_consts::{
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
        MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING,
    };
    use store_api::path_utils::table_dir;
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::{
        EnterStagingRequest, RegionRequest, StagingPartitionDirective,
    };
    use store_api::storage::ScanRequest;
    use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

    use super::*;
    use crate::test_util::{self, TestEnv};

    fn assert_merged_schema(rows: &Rows, expect_sparse: bool) {
        let column_names: HashSet<String> = rows
            .schema
            .iter()
            .map(|col| col.column_name.clone())
            .collect();

        if expect_sparse {
            assert!(
                column_names.contains(PRIMARY_KEY_COLUMN_NAME),
                "sparse encoding should include primary key column"
            );
            assert!(
                !column_names.contains(DATA_SCHEMA_TABLE_ID_COLUMN_NAME),
                "sparse encoding should not include table id column"
            );
            assert!(
                !column_names.contains(DATA_SCHEMA_TSID_COLUMN_NAME),
                "sparse encoding should not include tsid column"
            );
            assert!(
                !column_names.contains("job"),
                "sparse encoding should not include tag columns"
            );
            assert!(
                !column_names.contains("instance"),
                "sparse encoding should not include tag columns"
            );
        } else {
            assert!(
                !column_names.contains(PRIMARY_KEY_COLUMN_NAME),
                "dense encoding should not include primary key column"
            );
            assert!(
                column_names.contains(DATA_SCHEMA_TABLE_ID_COLUMN_NAME),
                "dense encoding should include table id column"
            );
            assert!(
                column_names.contains(DATA_SCHEMA_TSID_COLUMN_NAME),
                "dense encoding should include tsid column"
            );
            assert!(
                column_names.contains("job"),
                "dense encoding should keep tag columns"
            );
            assert!(
                column_names.contains("instance"),
                "dense encoding should keep tag columns"
            );
        }
    }

    fn job_partition_expr_json() -> String {
        let expr = col("job")
            .gt_eq(PartitionValue::String("job-0".into()))
            .and(col("job").lt(PartitionValue::String("job-9".into())));
        expr.as_json_str().unwrap()
    }

    async fn create_logical_region_with_tags(
        env: &TestEnv,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        tags: &[&str],
    ) {
        let region_create_request = test_util::create_logical_region_request(
            tags,
            physical_region_id,
            &table_dir("test", logical_region_id.table_id()),
        );
        env.metric()
            .handle_request(
                logical_region_id,
                RegionRequest::Create(region_create_request),
            )
            .await
            .unwrap();
    }

    async fn run_batch_write_with_schema_variants(
        env: &TestEnv,
        physical_region_id: RegionId,
        options: Vec<(String, String)>,
        expect_sparse: bool,
    ) {
        env.create_physical_region(physical_region_id, &TestEnv::default_table_dir(), options)
            .await;

        let logical_region_1 = env.default_logical_region_id();
        let logical_region_2 = RegionId::new(1024, 1);

        create_logical_region_with_tags(env, physical_region_id, logical_region_1, &["job"]).await;
        create_logical_region_with_tags(
            env,
            physical_region_id,
            logical_region_2,
            &["job", "instance"],
        )
        .await;

        let schema_1 = test_util::row_schema_with_tags(&["job"]);
        let schema_2 = test_util::row_schema_with_tags(&["job", "instance"]);

        let data_region_id = RegionId::new(physical_region_id.table_id(), 2);
        let primary_key_encoding = env
            .metric()
            .inner
            .get_primary_key_encoding(data_region_id)
            .unwrap();
        assert_eq!(
            primary_key_encoding,
            if expect_sparse {
                PrimaryKeyEncoding::Sparse
            } else {
                PrimaryKeyEncoding::Dense
            }
        );

        let build_requests = || {
            let rows_1 = test_util::build_rows(1, 3);
            let rows_2 = test_util::build_rows(2, 2);

            vec![
                (
                    logical_region_1,
                    RegionPutRequest {
                        rows: Rows {
                            schema: schema_1.clone(),
                            rows: rows_1,
                        },
                        hint: None,
                        partition_expr_version: None,
                    },
                ),
                (
                    logical_region_2,
                    RegionPutRequest {
                        rows: Rows {
                            schema: schema_2.clone(),
                            rows: rows_2,
                        },
                        hint: None,
                        partition_expr_version: None,
                    },
                ),
            ]
        };

        let merged_request = if expect_sparse {
            let (merged_request, _) = env
                .metric()
                .inner
                .merge_sparse_batch(physical_region_id, build_requests())
                .unwrap();
            let hint = merged_request
                .hint
                .as_ref()
                .expect("missing sparse write hint");
            assert_eq!(
                hint.primary_key_encoding,
                PrimaryKeyEncodingProto::Sparse as i32
            );
            merged_request
        } else {
            let (merged_request, _) = env
                .metric()
                .inner
                .merge_dense_batch(data_region_id, build_requests())
                .unwrap();
            assert!(merged_request.hint.is_none());
            merged_request
        };

        assert_merged_schema(&merged_request.rows, expect_sparse);

        let affected_rows = env
            .metric()
            .inner
            .put_regions_batch(build_requests().into_iter())
            .await
            .unwrap();
        assert_eq!(affected_rows, 5);

        let request = ScanRequest::default();
        let stream = env
            .mito()
            .scan_to_stream(data_region_id, request)
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();

        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
    }

    #[tokio::test]
    async fn test_write_logical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        // prepare data
        let schema = test_util::row_schema_with_tags(&["job"]);
        let rows = test_util::build_rows(1, 5);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
            hint: None,
            partition_expr_version: None,
        });

        // write data
        let logical_region_id = env.default_logical_region_id();
        let result = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 5);

        // read data from physical region
        let physical_region_id = env.default_physical_region_id();
        let request = ScanRequest::default();
        let stream = env
            .metric()
            .scan_to_stream(physical_region_id, request)
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+-------------------------+----------------+------------+---------------------+-------+
| greptime_timestamp      | greptime_value | __table_id | __tsid              | job   |
+-------------------------+----------------+------------+---------------------+-------+
| 1970-01-01T00:00:00     | 0.0            | 3          | 2955007454552897459 | tag_0 |
| 1970-01-01T00:00:00.001 | 1.0            | 3          | 2955007454552897459 | tag_0 |
| 1970-01-01T00:00:00.002 | 2.0            | 3          | 2955007454552897459 | tag_0 |
| 1970-01-01T00:00:00.003 | 3.0            | 3          | 2955007454552897459 | tag_0 |
| 1970-01-01T00:00:00.004 | 4.0            | 3          | 2955007454552897459 | tag_0 |
+-------------------------+----------------+------------+---------------------+-------+";
        assert_eq!(expected, batches.pretty_print().unwrap(), "physical region");

        // read data from logical region
        let request = ScanRequest::default();
        let stream = env
            .metric()
            .scan_to_stream(logical_region_id, request)
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+-------------------------+----------------+-------+
| greptime_timestamp      | greptime_value | job   |
+-------------------------+----------------+-------+
| 1970-01-01T00:00:00     | 0.0            | tag_0 |
| 1970-01-01T00:00:00.001 | 1.0            | tag_0 |
| 1970-01-01T00:00:00.002 | 2.0            | tag_0 |
| 1970-01-01T00:00:00.003 | 3.0            | tag_0 |
| 1970-01-01T00:00:00.004 | 4.0            | tag_0 |
+-------------------------+----------------+-------+";
        assert_eq!(expected, batches.pretty_print().unwrap(), "logical region");
    }

    #[tokio::test]
    async fn test_write_logical_region_row_count() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        // add columns
        let logical_region_id = env.default_logical_region_id();
        let columns = &["odd", "even", "Ev_En"];
        let alter_request = test_util::alter_logical_region_add_tag_columns(123456, columns);
        engine
            .handle_request(logical_region_id, RegionRequest::Alter(alter_request))
            .await
            .unwrap();

        // prepare data
        let schema = test_util::row_schema_with_tags(columns);
        let rows = test_util::build_rows(3, 100);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
            hint: None,
            partition_expr_version: None,
        });

        // write data
        let result = engine
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(100, result.affected_rows);
    }

    #[tokio::test]
    async fn test_write_physical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        let physical_region_id = env.default_physical_region_id();
        let schema = test_util::row_schema_with_tags(&["abc"]);
        let rows = test_util::build_rows(1, 100);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
            hint: None,
            partition_expr_version: None,
        });

        engine
            .handle_request(physical_region_id, request)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_write_nonexist_logical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        let logical_region_id = RegionId::new(175, 8345);
        let schema = test_util::row_schema_with_tags(&["def"]);
        let rows = test_util::build_rows(1, 100);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
            hint: None,
            partition_expr_version: None,
        });

        engine
            .handle_request(logical_region_id, request)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_batch_write_multiple_logical_regions() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        // Create two additional logical regions
        let physical_region_id = env.default_physical_region_id();
        let logical_region_1 = env.default_logical_region_id();
        let logical_region_2 = RegionId::new(1024, 1);
        let logical_region_3 = RegionId::new(1024, 2);

        env.create_logical_region(physical_region_id, logical_region_2)
            .await;
        env.create_logical_region(physical_region_id, logical_region_3)
            .await;

        // Prepare batch requests with non-overlapping timestamps
        let schema = test_util::row_schema_with_tags(&["job"]);

        // Use build_rows_with_ts to create non-overlapping timestamps
        // logical_region_1: ts 0, 1, 2
        // logical_region_2: ts 10, 11  (offset to avoid overlap)
        // logical_region_3: ts 20, 21, 22, 23, 24  (offset to avoid overlap)
        let rows1 = test_util::build_rows(1, 3);
        let mut rows2 = test_util::build_rows(1, 2);
        let mut rows3 = test_util::build_rows(1, 5);

        // Adjust timestamps to avoid conflicts
        use api::v1::value::ValueData;
        for (i, row) in rows2.iter_mut().enumerate() {
            if let Some(ValueData::TimestampMillisecondValue(ts)) =
                row.values.get_mut(0).and_then(|v| v.value_data.as_mut())
            {
                *ts = (10 + i) as i64;
            }
        }
        for (i, row) in rows3.iter_mut().enumerate() {
            if let Some(ValueData::TimestampMillisecondValue(ts)) =
                row.values.get_mut(0).and_then(|v| v.value_data.as_mut())
            {
                *ts = (20 + i) as i64;
            }
        }

        let requests = vec![
            (
                logical_region_1,
                RegionPutRequest {
                    rows: Rows {
                        schema: schema.clone(),
                        rows: rows1,
                    },
                    hint: None,
                    partition_expr_version: None,
                },
            ),
            (
                logical_region_2,
                RegionPutRequest {
                    rows: Rows {
                        schema: schema.clone(),
                        rows: rows2,
                    },
                    hint: None,
                    partition_expr_version: None,
                },
            ),
            (
                logical_region_3,
                RegionPutRequest {
                    rows: Rows {
                        schema: schema.clone(),
                        rows: rows3,
                    },
                    hint: None,
                    partition_expr_version: None,
                },
            ),
        ];

        // Batch write
        let affected_rows = engine
            .inner
            .put_regions_batch(requests.into_iter())
            .await
            .unwrap();
        assert_eq!(affected_rows, 10);

        // Verify physical region contains data from all logical regions
        let request = ScanRequest::default();
        let stream = env
            .metric()
            .scan_to_stream(physical_region_id, request)
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();

        // Should have 3 + 2 + 5 = 10 rows total
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
    }

    #[tokio::test]
    async fn test_batch_write_with_partial_failure() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        let physical_region_id = env.default_physical_region_id();
        let logical_region_1 = env.default_logical_region_id();
        let logical_region_2 = RegionId::new(1024, 1);
        let nonexistent_region = RegionId::new(9999, 9999);

        env.create_logical_region(physical_region_id, logical_region_2)
            .await;

        // Prepare batch with one invalid region
        let schema = test_util::row_schema_with_tags(&["job"]);
        let requests = vec![
            (
                logical_region_1,
                RegionPutRequest {
                    rows: Rows {
                        schema: schema.clone(),
                        rows: test_util::build_rows(1, 3),
                    },
                    hint: None,
                    partition_expr_version: None,
                },
            ),
            (
                nonexistent_region,
                RegionPutRequest {
                    rows: Rows {
                        schema: schema.clone(),
                        rows: test_util::build_rows(1, 2),
                    },
                    hint: None,
                    partition_expr_version: None,
                },
            ),
            (
                logical_region_2,
                RegionPutRequest {
                    rows: Rows {
                        schema: schema.clone(),
                        rows: test_util::build_rows(1, 5),
                    },
                    hint: None,
                    partition_expr_version: None,
                },
            ),
        ];

        // Batch write
        let result = engine.inner.put_regions_batch(requests.into_iter()).await;
        assert!(result.is_err());

        // Invalid region is detected before any write, so the physical region remains empty.
        // Fail-fast is per physical-region group; cross-group partial success is possible.
        let request = ScanRequest::default();
        let stream = env
            .metric()
            .scan_to_stream(physical_region_id, request)
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();

        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn test_batch_write_single_request_fast_path() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        let logical_region_id = env.default_logical_region_id();
        let schema = test_util::row_schema_with_tags(&["job"]);

        // Single request should use fast path
        let requests = vec![(
            logical_region_id,
            RegionPutRequest {
                rows: Rows {
                    schema,
                    rows: test_util::build_rows(1, 5),
                },
                hint: None,
                partition_expr_version: None,
            },
        )];

        let affected_rows = engine
            .inner
            .put_regions_batch(requests.into_iter())
            .await
            .unwrap();
        assert_eq!(affected_rows, 5);
    }

    #[tokio::test]
    async fn test_batch_write_empty_requests() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        // Empty batch should return zero affected rows
        let requests = vec![];
        let affected_rows = engine
            .inner
            .put_regions_batch(requests.into_iter())
            .await
            .unwrap();

        assert_eq!(affected_rows, 0);
    }

    #[tokio::test]
    async fn test_batch_write_sparse_encoding() {
        let env = TestEnv::new().await;
        let physical_region_id = env.default_physical_region_id();

        run_batch_write_with_schema_variants(
            &env,
            physical_region_id,
            vec![(
                MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING.to_string(),
                "sparse".to_string(),
            )],
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn test_batch_write_dense_encoding() {
        let env = TestEnv::new().await;
        let physical_region_id = env.default_physical_region_id();

        run_batch_write_with_schema_variants(
            &env,
            physical_region_id,
            vec![(
                MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING.to_string(),
                "dense".to_string(),
            )],
            false,
        )
        .await;
    }

    #[tokio::test]
    async fn test_metric_put_rejects_bad_partition_expr_version() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let logical_region_id = env.default_logical_region_id();
        let rows = Rows {
            schema: test_util::row_schema_with_tags(&["job"]),
            rows: test_util::build_rows(1, 3),
        };

        let err = env
            .metric()
            .handle_request(
                logical_region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows,
                    hint: None,
                    partition_expr_version: Some(1),
                }),
            )
            .await
            .unwrap_err();

        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    }

    #[tokio::test]
    async fn test_metric_put_respects_staging_partition_expr_version() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let logical_region_id = env.default_logical_region_id();
        let physical_region_id = env.default_physical_region_id();
        let partition_expr = job_partition_expr_json();
        env.metric()
            .handle_request(
                physical_region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::PartitionExpr(
                        partition_expr.clone(),
                    ),
                }),
            )
            .await
            .unwrap();

        let expected_version = partition_expr_version(Some(&partition_expr));
        let rows = Rows {
            schema: test_util::row_schema_with_tags(&["job"]),
            rows: test_util::build_rows(1, 3),
        };

        let err = env
            .metric()
            .handle_request(
                logical_region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: rows.clone(),
                    hint: None,
                    partition_expr_version: Some(expected_version.wrapping_add(1)),
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);

        let response = env
            .metric()
            .handle_request(
                logical_region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: rows.clone(),
                    hint: None,
                    partition_expr_version: None,
                }),
            )
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 3);

        let response = env
            .metric()
            .handle_request(
                logical_region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows,
                    hint: None,
                    partition_expr_version: Some(expected_version),
                }),
            )
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 3);
    }
}
