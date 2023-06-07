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

//! Prometheus remote write helper functions.
use std::collections::{HashMap, HashSet};

use api::prometheus::remote::WriteRequest;
use api::v1::{InsertRequest as GrpcInsertRequest, InsertRequests};
use common_grpc::writer::{LinesWriter, Precision};
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::prometheus::{
    find_table_by_name, FIELD_COLUMN_NAME, METRIC_NAME_LABEL, TIMESTAMP_COLUMN_NAME,
};

pub type MetricsLabelsMap = HashMap<String, HashSet<String>>;

pub fn to_grpc_insert_requests(
    request: WriteRequest,
) -> Result<(InsertRequests, usize, MetricsLabelsMap)> {
    let mut writers: HashMap<String, LinesWriter> = HashMap::new();
    let mut metrics_labels = MetricsLabelsMap::new();

    for mut timeseries in request.timeseries {
        let name = timeseries.labels.iter().find_map(|label| {
            if label.name == METRIC_NAME_LABEL {
                Some(&label.value)
            } else {
                None
            }
        });
        let table_name = find_table_by_name(name);

        let writer = writers
            .entry(table_name)
            .or_insert_with(|| LinesWriter::with_lines(16));
        let labels = metrics_labels
            .entry(
                name.context(error::InvalidPromRemoteRequestSnafu {
                    msg: "Missing __name__ label in timeseries",
                })?
                .to_string(),
            )
            .or_insert_with(HashSet::new);

        // For each sample
        for sample in &timeseries.samples {
            // Insert labels first.
            for label in &timeseries.labels {
                writer
                    .write_tag(&label.name, &label.value)
                    .context(error::PromSeriesWriteSnafu)?;
            }
            // Insert sample timestamp.
            writer
                .write_ts(
                    TIMESTAMP_COLUMN_NAME,
                    (sample.timestamp, Precision::Millisecond),
                )
                .context(error::PromSeriesWriteSnafu)?;
            // Insert sample value.
            writer
                .write_f64(FIELD_COLUMN_NAME, sample.value)
                .context(error::PromSeriesWriteSnafu)?;

            writer.commit();
        }

        for label in std::mem::take(&mut timeseries.labels) {
            if label.name != METRIC_NAME_LABEL {
                labels.insert(label.name);
            }
        }
    }

    let mut sample_counts = 0;
    let inserts = writers
        .into_iter()
        .map(|(table_name, writer)| {
            let (columns, row_count) = writer.finish();
            sample_counts += row_count as usize;
            GrpcInsertRequest {
                table_name,
                region_number: 0,
                columns,
                row_count,
            }
        })
        .collect();
    Ok((InsertRequests { inserts }, sample_counts, metrics_labels))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prometheus::mock_timeseries;

    #[test]
    fn test_write_request_to_insert_exprs() {
        let write_request = WriteRequest {
            timeseries: mock_timeseries(),
            ..Default::default()
        };

        let mut exprs = to_grpc_insert_requests(write_request).unwrap().0.inserts;
        assert_eq!(1, exprs.len());
        assert_eq!("prometheus_metrics", exprs[0].table_name);

        let expr = exprs.get_mut(0).unwrap();
        expr.columns
            .sort_unstable_by(|l, r| l.column_name.cmp(&r.column_name));

        let columns = &expr.columns;
        let row_count = expr.row_count;

        assert_eq!(7, row_count);
        assert_eq!(columns.len(), 7);

        let column_names = columns
            .iter()
            .map(|c| c.column_name.clone())
            .collect::<Vec<_>>();

        assert_eq!(
            vec![
                "__name__",
                "app",
                "greptime_timestamp",
                "greptime_value",
                "idc",
                "instance",
                "job"
            ],
            column_names
        );

        assert_eq!(
            columns[0].values.as_ref().unwrap().string_values,
            vec!["metric1", "metric1", "metric2", "metric2", "metric3", "metric3", "metric3"]
        );
        assert_eq!(
            columns[1].values.as_ref().unwrap().string_values,
            vec!["biz", "biz", "biz"],
        );
        assert_eq!(
            columns[2].values.as_ref().unwrap().ts_millisecond_values,
            vec![1000, 2000, 1000, 2000, 1000, 2000, 3000]
        );
        assert_eq!(
            columns[3].values.as_ref().unwrap().f64_values,
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        );
        assert_eq!(
            columns[4].values.as_ref().unwrap().string_values,
            vec!["z001", "z001", "z002", "z002", "z002"]
        );
        assert_eq!(
            columns[5].values.as_ref().unwrap().string_values,
            vec!["test_host1", "test_host1"]
        );
        assert_eq!(
            columns[6].values.as_ref().unwrap().string_values,
            vec!["spark", "spark"]
        );
    }
}
