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

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, RowInsertRequests, SemanticType};
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use catalog::CatalogManagerRef;
use client::Output;
use common_error::ext::BoxedError;
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use servers::error::{
    AuthSnafu, CatalogSnafu, Error, TimestampOverflowSnafu, UnexpectedResultSnafu,
};
use servers::influxdb::InfluxdbRequest;
use servers::interceptor::{LineProtocolInterceptor, LineProtocolInterceptorRef};
use servers::query_handler::InfluxdbLineProtocolHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::instance::Instance;

#[async_trait]
impl InfluxdbLineProtocolHandler for Instance {
    async fn exec(
        &self,
        request: InfluxdbRequest,
        ctx: QueryContextRef,
    ) -> servers::error::Result<Output> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::LineProtocol)
            .context(AuthSnafu)?;

        let interceptor_ref = self.plugins.get::<LineProtocolInterceptorRef<Error>>();
        interceptor_ref.pre_execute(&request.lines, ctx.clone())?;

        let requests = request.try_into()?;

        let aligner = InfluxdbLineTimestampAligner {
            catalog_manager: self.catalog_manager(),
        };
        let requests = aligner.align_timestamps(requests, &ctx).await?;

        let requests = interceptor_ref
            .post_lines_conversion(requests, ctx.clone())
            .await?;

        self.handle_influx_row_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .context(servers::error::ExecuteGrpcQuerySnafu)
    }
}

/// Align the timestamp precisions in Influxdb lines (after they are converted to the GRPC row
/// inserts) to the time index columns' time units of the created tables (if there are any).
struct InfluxdbLineTimestampAligner<'a> {
    catalog_manager: &'a CatalogManagerRef,
}

impl InfluxdbLineTimestampAligner<'_> {
    async fn align_timestamps(
        &self,
        requests: RowInsertRequests,
        query_context: &QueryContextRef,
    ) -> servers::error::Result<RowInsertRequests> {
        let mut inserts = requests.inserts;
        for insert in inserts.iter_mut() {
            let Some(rows) = &mut insert.rows else {
                continue;
            };

            let Some(target_time_unit) = self
                .catalog_manager
                .table(
                    query_context.current_catalog(),
                    &query_context.current_schema(),
                    &insert.table_name,
                    Some(query_context),
                )
                .await
                .context(CatalogSnafu)?
                .map(|x| x.schema())
                .and_then(|schema| {
                    schema.timestamp_column().map(|col| {
                        col.data_type
                            .as_timestamp()
                            .expect("Time index column is not of timestamp type?!")
                            .unit()
                    })
                })
            else {
                continue;
            };

            let target_timestamp_type = match target_time_unit {
                TimeUnit::Second => ColumnDataType::TimestampSecond,
                TimeUnit::Millisecond => ColumnDataType::TimestampMillisecond,
                TimeUnit::Microsecond => ColumnDataType::TimestampMicrosecond,
                TimeUnit::Nanosecond => ColumnDataType::TimestampNanosecond,
            };
            let Some(to_be_aligned) = rows.schema.iter().enumerate().find_map(|(i, x)| {
                if x.semantic_type() == SemanticType::Timestamp
                    && x.datatype() != target_timestamp_type
                {
                    Some(i)
                } else {
                    None
                }
            }) else {
                continue;
            };

            // Indexing safety: `to_be_aligned` is guaranteed to be a valid index because it's got
            // from "enumerate" the schema vector above.
            rows.schema[to_be_aligned].datatype = target_timestamp_type as i32;

            for row in rows.rows.iter_mut() {
                let Some(time_value) = row
                    .values
                    .get_mut(to_be_aligned)
                    .and_then(|x| x.value_data.as_mut())
                else {
                    continue;
                };
                *time_value = align_time_unit(time_value, target_time_unit)?;
            }
        }
        Ok(RowInsertRequests { inserts })
    }
}

fn align_time_unit(value: &ValueData, target: TimeUnit) -> servers::error::Result<ValueData> {
    let timestamp = match value {
        ValueData::TimestampSecondValue(x) => Timestamp::new_second(*x),
        ValueData::TimestampMillisecondValue(x) => Timestamp::new_millisecond(*x),
        ValueData::TimestampMicrosecondValue(x) => Timestamp::new_microsecond(*x),
        ValueData::TimestampNanosecondValue(x) => Timestamp::new_nanosecond(*x),
        _ => {
            return UnexpectedResultSnafu {
                reason: format!("Timestamp value '{:?}' is not of timestamp type!", value),
            }
            .fail();
        }
    };

    let timestamp = timestamp
        .convert_to(target)
        .with_context(|| TimestampOverflowSnafu {
            error: format!("{:?} convert to {}", timestamp, target),
        })?;

    Ok(match target {
        TimeUnit::Second => ValueData::TimestampSecondValue(timestamp.value()),
        TimeUnit::Millisecond => ValueData::TimestampMillisecondValue(timestamp.value()),
        TimeUnit::Microsecond => ValueData::TimestampMicrosecondValue(timestamp.value()),
        TimeUnit::Nanosecond => ValueData::TimestampNanosecondValue(timestamp.value()),
    })
}
