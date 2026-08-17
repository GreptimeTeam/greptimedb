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

//! OTel-native resource descriptor synthesized from OTLP metrics requests.
//!
//! Ordinary OTLP metrics scatter (filtered) resource attributes as tags over
//! every emitted metric table, which is useless for entity extraction: the
//! entity graph would have to scan every logical table and would still miss
//! attributes dropped by the promote filter. Instead, each request projects
//! its distinct resources into one info-metric-shaped table,
//! [`OTEL_RESOURCE_INFO_TABLE_NAME`], which the entity-graph conventions
//! whitelist by name.
//!
//! Columns are a fixed allowlist keyed by the raw OTel attribute names —
//! deliberately independent of both the per-request label translation
//! strategy (the conventions match fixed column names) and the resource-attr
//! promote/ignore headers (this is entity metadata, not label promotion).
//! `job`/`instance` are derived compatibility columns aligning the service
//! identity with Prometheus-sourced `target_info`.

use std::collections::BTreeMap;

use api::v1::RowInsertRequests;
use common_grpc::precision::Precision;
use common_query::prelude::{greptime_timestamp, greptime_value};
use otel_arrow_rust::proto::opentelemetry::common::v1::KeyValue;
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{ResourceMetrics, metric};

use super::{INSTANCE_KEY, JOB_KEY, scalar_value_string, service_identity};
use crate::error::Result;
use crate::otlp::trace::{KEY_SERVICE_NAME, KEY_SERVICE_NAMESPACE};
use crate::row_writer::{self, MultiTableData};

/// Table name of the synthesized resource descriptor; reserved in the sense
/// that an incoming metric with the same name suppresses synthesis for its
/// request.
pub const OTEL_RESOURCE_INFO_TABLE_NAME: &str = "otel_resource_info";

/// Resource attributes projected verbatim under their raw OTel keys.
/// `service.instance.id` is not listed: it lands unchanged in `instance`.
const RESOURCE_INFO_ATTRS: [&str; 9] = [
    KEY_SERVICE_NAME,
    KEY_SERVICE_NAMESPACE,
    "host.id",
    "host.name",
    "container.id",
    "container.name",
    "k8s.pod.uid",
    "k8s.pod.name",
    "k8s.namespace.name",
];

/// Request-local resource snapshots: one row per distinct projected attribute
/// set, stamped with the newest data-point timestamp that observed it.
/// Cross-request dedup is left to the storage engine's last-row merge.
#[derive(Debug, Default)]
pub struct ResourceInfoData {
    rows: BTreeMap<Vec<(String, String)>, i64>,
}

impl ResourceInfoData {
    /// Projects one resource's raw (unfiltered) attributes; resources whose
    /// projection is empty contribute nothing.
    pub fn observe(&mut self, raw_attrs: &[KeyValue], max_ts_nanos: i64) {
        let mut tags = BTreeMap::new();
        let (job, instance) = service_identity(raw_attrs);
        if let Some(job) = job {
            tags.insert(JOB_KEY.to_string(), job);
        }
        if let Some(instance) = instance {
            tags.insert(INSTANCE_KEY.to_string(), instance);
        }
        for kv in raw_attrs {
            if RESOURCE_INFO_ATTRS.contains(&kv.key.as_str())
                && let Some(value) = scalar_value_string(kv.value.as_ref())
            {
                tags.insert(kv.key.clone(), value);
            }
        }
        if tags.is_empty() {
            return;
        }

        let entry = self.rows.entry(tags.into_iter().collect()).or_insert(i64::MIN);
        *entry = (*entry).max(max_ts_nanos);
    }

    /// Builds the descriptor insert: all projected attributes as tags plus
    /// `greptime_value = 1.0`, timestamps in milliseconds. `None` when no
    /// resource was observed.
    pub fn into_row_insert_requests(self) -> Result<Option<RowInsertRequests>> {
        if self.rows.is_empty() {
            return Ok(None);
        }

        let mut writer = MultiTableData::default();
        let table = writer.get_or_default_table_data(
            OTEL_RESOURCE_INFO_TABLE_NAME,
            RESOURCE_INFO_ATTRS.len() + 4,
            self.rows.len(),
        );
        for (tags, ts_nanos) in self.rows {
            let mut row = table.alloc_one_row();
            row_writer::write_tags(table, tags.into_iter(), &mut row)?;
            row_writer::write_f64(table, greptime_value(), 1.0, &mut row)?;
            row_writer::write_ts_to_millis(
                table,
                greptime_timestamp(),
                Some(ts_nanos),
                Precision::Nanosecond,
                &mut row,
            )?;
            table.add_row(row);
        }

        let (requests, _) = writer.into_row_insert_requests();
        Ok(Some(requests))
    }
}

/// The newest data-point timestamp under a resource, the descriptor row's
/// observation time. `None` when the resource carries no data points at all.
pub(crate) fn max_data_point_time_nanos(resource: &ResourceMetrics) -> Option<i64> {
    let mut max_ts: Option<u64> = None;
    let mut fold = |ts: u64| max_ts = Some(max_ts.map_or(ts, |cur| cur.max(ts)));
    for scope in &resource.scope_metrics {
        for m in &scope.metrics {
            match &m.data {
                Some(metric::Data::Gauge(g)) => {
                    g.data_points.iter().for_each(|p| fold(p.time_unix_nano))
                }
                Some(metric::Data::Sum(s)) => {
                    s.data_points.iter().for_each(|p| fold(p.time_unix_nano))
                }
                Some(metric::Data::Histogram(h)) => {
                    h.data_points.iter().for_each(|p| fold(p.time_unix_nano))
                }
                Some(metric::Data::ExponentialHistogram(h)) => {
                    h.data_points.iter().for_each(|p| fold(p.time_unix_nano))
                }
                Some(metric::Data::Summary(s)) => {
                    s.data_points.iter().for_each(|p| fold(p.time_unix_nano))
                }
                None => {}
            }
        }
    }
    max_ts.map(|ts| ts as i64)
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use api::v1::value::ValueData;
    use common_query::prelude::set_default_prefix;
    use otel_arrow_rust::proto::opentelemetry::common::v1::{AnyValue, any_value};

    use super::*;

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
        }
    }

    #[test]
    fn observe_projects_allowlist_and_dedups_per_request() {
        let mut data = ResourceInfoData::default();
        let attrs = vec![
            kv("service.name", "api"),
            kv("service.namespace", "shop"),
            kv("service.instance.id", "inst-1"),
            kv("host.id", "h-1"),
            kv("os.type", "linux"),
        ];
        data.observe(&attrs, 100);
        // the same resource seen again keeps one row with the newest timestamp
        data.observe(&attrs, 50);
        assert_eq!(data.rows.len(), 1);
        let (tags, ts) = data.rows.iter().next().unwrap();
        assert_eq!(*ts, 100);
        assert!(tags.contains(&("job".to_string(), "shop/api".to_string())));
        assert!(tags.contains(&("instance".to_string(), "inst-1".to_string())));
        assert!(tags.contains(&("service.name".to_string(), "api".to_string())));
        // not allowlisted / folded into instance
        assert!(
            tags.iter()
                .all(|(k, _)| k != "os.type" && k != "service.instance.id")
        );

        data.observe(&[kv("host.id", "h-2")], 10);
        assert_eq!(data.rows.len(), 2);
    }

    #[test]
    fn observe_skips_resources_with_empty_projection() {
        let mut data = ResourceInfoData::default();
        data.observe(&[kv("os.type", "linux")], 100);
        assert!(data.rows.is_empty());
        assert!(data.into_row_insert_requests().unwrap().is_none());
    }

    #[test]
    fn rows_carry_raw_key_tags_value_and_millis_timestamp() {
        set_default_prefix(None).unwrap();
        let mut data = ResourceInfoData::default();
        data.observe(
            &[kv("service.name", "api"), kv("host.id", "h-1")],
            1_700_000_000_123_456_789,
        );
        let requests = data.into_row_insert_requests().unwrap().unwrap();
        assert_eq!(requests.inserts.len(), 1);
        let insert = &requests.inserts[0];
        assert_eq!(insert.table_name, OTEL_RESOURCE_INFO_TABLE_NAME);

        let rows = insert.rows.as_ref().unwrap();
        let names = rows
            .schema
            .iter()
            .map(|c| c.column_name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "host.id",
                "job",
                "service.name",
                greptime_value(),
                greptime_timestamp()
            ]
        );
        for column in &rows.schema[..3] {
            assert_eq!(column.semantic_type, SemanticType::Tag as i32);
        }

        assert_eq!(rows.rows.len(), 1);
        let values = &rows.rows[0].values;
        assert_eq!(values[3].value_data, Some(ValueData::F64Value(1.0)));
        assert_eq!(
            values[4].value_data,
            Some(ValueData::TimestampMillisecondValue(1_700_000_000_123))
        );
    }
}
