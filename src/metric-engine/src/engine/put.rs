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

use api::v1::{Rows, WriteHint};
use common_telemetry::{error, info};
use snafu::{OptionExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_request::{
    AffectedRows, RegionDeleteRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{RegionId, TableId};

use crate::engine::MetricEngineInner;
use crate::error::{
    ColumnNotFoundSnafu, ForbiddenPhysicalAlterSnafu, LogicalRegionNotFoundSnafu,
    PhysicalRegionNotFoundSnafu, Result, UnsupportedRegionRequestSnafu,
};
use crate::metrics::{FORBIDDEN_OPERATION_COUNT, MITO_OPERATION_ELAPSED};
use crate::row_modifier::RowsIter;
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
    /// Precondition: All requests should belong to the same physical region.
    /// This is typically ensured by the frontend routing layer.
    ///
    /// Returns a map of per-logical-region results. Individual request failures
    /// (e.g., in verify_rows) don't block other requests. However, if the final
    /// physical write fails, all previously successful requests are marked as failed.
    pub async fn put_regions_batch(
        &self,
        requests: Vec<(RegionId, RegionPutRequest)>,
    ) -> Result<HashMap<RegionId, Result<AffectedRows>>> {
        if requests.is_empty() {
            return Ok(HashMap::new());
        }

        let _timer = MITO_OPERATION_ELAPSED
            .with_label_values(&["put_batch"])
            .start_timer();

        // Fast path: single request, no batching overhead
        if requests.len() == 1 {
            let (logical_id, req) = requests.into_iter().next().unwrap();
            let result = self.put_logical_region(logical_id, req).await;
            return Ok([(logical_id, result)].into_iter().collect());
        }

        // Get physical region metadata from the first logical region
        let first_logical_id = requests[0].0;
        let (physical_region_id, data_region_id, primary_key_encoding) =
            self.find_data_region_meta(first_logical_id)?;

        // Process each request: verify and modify
        let mut merged_schema = None;
        let mut merged_rows = Vec::new();
        let mut results = HashMap::with_capacity(requests.len());

        for (logical_region_id, mut request) in requests {
            // Verify rows
            match self
                .verify_rows(logical_region_id, physical_region_id, &request.rows)
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    results.insert(logical_region_id, Err(e));
                    continue;
                }
            }

            // Modify rows (adds __table_id and __tsid)
            match self.modify_rows(
                physical_region_id,
                logical_region_id.table_id(),
                &mut request.rows,
                primary_key_encoding,
            ) {
                Ok(()) => {}
                Err(e) => {
                    results.insert(logical_region_id, Err(e));
                    continue;
                }
            }

            // Accumulate for merging
            let row_count = request.rows.rows.len();
            if merged_schema.is_none() {
                merged_schema = Some(request.rows.schema.clone());
            }
            merged_rows.extend(request.rows.rows);
            results.insert(logical_region_id, Ok(row_count as AffectedRows));
        }

        // If all requests failed, return early
        if merged_rows.is_empty() {
            return Ok(results);
        }

        // Merge into a single physical write
        let merged_request = RegionPutRequest {
            rows: Rows {
                schema: merged_schema.unwrap(),
                rows: merged_rows,
            },
            hint: if primary_key_encoding == PrimaryKeyEncoding::Sparse {
                Some(WriteHint {
                    primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
                })
            } else {
                None
            },
        };

        // Write once to the physical region
        self.data_region
            .write_data(data_region_id, RegionRequest::Put(merged_request))
            .await?;

        Ok(results)
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
                primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
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
                primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
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
        let output = self.row_modifier.modify_rows(iter, table_id, encoding)?;
        *rows = output;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common_recordbatch::RecordBatches;
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::RegionRequest;
    use store_api::storage::ScanRequest;

    use super::*;
    use crate::test_util::{self, TestEnv};

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
                },
            ),
        ];

        // Batch write
        let results = engine.inner.put_regions_batch(requests).await.unwrap();

        // Verify all succeeded
        assert_eq!(results.len(), 3);
        assert_eq!(
            *results.get(&logical_region_1).unwrap().as_ref().unwrap(),
            3
        );
        assert_eq!(
            *results.get(&logical_region_2).unwrap().as_ref().unwrap(),
            2
        );
        assert_eq!(
            *results.get(&logical_region_3).unwrap().as_ref().unwrap(),
            5
        );

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
                },
            ),
        ];

        // Batch write
        let results = engine.inner.put_regions_batch(requests).await.unwrap();

        // Verify results
        assert_eq!(results.len(), 3);
        assert!(results.get(&logical_region_1).unwrap().is_ok());
        assert!(results.get(&nonexistent_region).unwrap().is_err());
        assert!(results.get(&logical_region_2).unwrap().is_ok());

        // Verify physical region contains data from valid regions only
        let request = ScanRequest::default();
        let stream = env
            .metric()
            .scan_to_stream(physical_region_id, request)
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();

        // Should have 3 + 5 = 8 rows (nonexistent_region skipped)
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 8);
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
            },
        )];

        let results = engine.inner.put_regions_batch(requests).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(
            *results.get(&logical_region_id).unwrap().as_ref().unwrap(),
            5
        );
    }

    #[tokio::test]
    async fn test_batch_write_empty_requests() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        // Empty batch should return empty results
        let requests = vec![];
        let results = engine.inner.put_regions_batch(requests).await.unwrap();

        assert_eq!(results.len(), 0);
    }
}
