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

//! Resource descriptor synthesized from OTLP metrics requests, so the entity
//! graph reads one table instead of scanning every logical metric table for
//! attributes the promote filter may have dropped.
//!
//! Columns are a fixed allowlist under the raw OTel attribute names: the
//! conventions whitelist matches fixed names, so they must not follow the
//! per-request label translation strategy or the promote/ignore headers.

use std::collections::BTreeMap;

use api::v1::RowInsertRequests;
use common_catalog::consts::SEMANTIC_GRAPH_WINDOW_NANOS;
use common_grpc::precision::Precision;
use common_query::prelude::{greptime_timestamp, greptime_value};
use otel_arrow_rust::proto::opentelemetry::common::v1::KeyValue;
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{ResourceMetrics, metric};

use crate::error::Result;
use crate::otlp::metrics::{INSTANCE_KEY, JOB_KEY, scalar_value_string, service_identity};
use crate::otlp::trace::{
    KEY_CONTAINER_ID, KEY_CONTAINER_NAME, KEY_HOST_ID, KEY_HOST_NAME, KEY_K8S_NAMESPACE_NAME,
    KEY_K8S_POD_NAME, KEY_K8S_POD_UID, KEY_SERVICE_NAME, KEY_SERVICE_NAMESPACE,
};
use crate::row_writer::{self, MultiTableData};

/// Prefixed like the other engine-managed tables, so a user metric is
/// unlikely to claim the name.
pub const OTEL_RESOURCE_INFO_TABLE_NAME: &str = "greptime_otel_resource_info";

/// Attributes projected under their raw OTel keys. `service.instance.id` is
/// absent on purpose: it lands in `instance`.
///
/// Matched instead of scanned: this runs for every attribute of every
/// resource, and the compiler turns it into a length-and-prefix dispatch.
fn is_projected_attr(key: &str) -> bool {
    matches!(
        key,
        KEY_SERVICE_NAME
            | KEY_SERVICE_NAMESPACE
            | KEY_HOST_ID
            | KEY_HOST_NAME
            | KEY_CONTAINER_ID
            | KEY_CONTAINER_NAME
            | KEY_K8S_POD_UID
            | KEY_K8S_POD_NAME
            | KEY_K8S_NAMESPACE_NAME
    )
}

/// Upper bound of [`is_projected_attr`] plus the derived `job`/`instance`,
/// used to size the per-row buffers.
const MAX_PROJECTED_TAGS: usize = 11;

/// Projected attributes (sorted `(name, value)` pairs) -> graph window ->
/// the newest data-point time seen in that window, which is what the row for
/// that window is stamped with.
///
/// Windows are an inner map so the attributes are stored, and moved, once per
/// resource. They are keyed separately because one request may carry data for
/// several of them: a single row per resource would describe only the newest
/// window, leaving the earlier ones with metric rows but no entities.
#[derive(Debug, Default)]
pub struct ResourceInfoData {
    rows: BTreeMap<Vec<(String, String)>, BTreeMap<i64, i64>>,
}

impl ResourceInfoData {
    /// Takes the raw attributes, before the promote filter runs on them, and
    /// the times of the data points this resource actually writes.
    pub fn observe(&mut self, raw_attrs: &[KeyValue], resource: &ResourceMetrics) {
        let mut tags = Vec::with_capacity(MAX_PROJECTED_TAGS);
        let (job, instance) = service_identity(raw_attrs);
        if let Some(job) = job {
            tags.push((JOB_KEY.to_string(), job));
        }
        if let Some(instance) = instance {
            tags.push((INSTANCE_KEY.to_string(), instance));
        }
        for kv in raw_attrs {
            if is_projected_attr(&kv.key)
                && let Some(value) = scalar_value_string(kv.value.as_ref())
            {
                tags.push((kv.key.clone(), value));
            }
        }
        if tags.is_empty() {
            return;
        }
        // Sorted so equal attribute sets share a key, and so the emitted
        // columns keep a stable order.
        tags.sort_unstable();

        let mut observed: BTreeMap<i64, i64> = BTreeMap::new();
        for_each_encoded_time(resource, |ts| {
            let window = ts - ts.rem_euclid(SEMANTIC_GRAPH_WINDOW_NANOS);
            observed
                .entry(window)
                .and_modify(|newest| *newest = (*newest).max(ts))
                .or_insert(ts);
        });
        if observed.is_empty() {
            return;
        }

        let windows = self.rows.entry(tags).or_default();
        for (window, newest) in observed {
            windows
                .entry(window)
                .and_modify(|seen| *seen = (*seen).max(newest))
                .or_insert(newest);
        }
    }

    /// Every projected attribute becomes a tag, so auto-create puts it in the
    /// primary key where the conventions expect it.
    pub fn into_row_insert_requests(self) -> Result<Option<RowInsertRequests>> {
        if self.rows.is_empty() {
            return Ok(None);
        }

        let mut writer = MultiTableData::default();
        let table = writer.get_or_default_table_data(
            OTEL_RESOURCE_INFO_TABLE_NAME,
            MAX_PROJECTED_TAGS + 2,
            self.rows.values().map(BTreeMap::len).sum(),
        );
        for (tags, windows) in &self.rows {
            for ts_nanos in windows.values().copied() {
                let mut row = table.alloc_one_row();
                row_writer::write_tags(table, tags.iter().cloned(), &mut row)?;
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
        }

        let (requests, _) = writer.into_row_insert_requests();
        Ok(Some(requests))
    }
}

/// Visits the times of the data points the encoder writes rows for, so a
/// resource is described exactly where it is measured. Exponential histograms
/// are excluded: [`super::encode_metrics`] drops them, and describing a
/// resource whose only data was dropped would invent an entity out of nothing.
fn for_each_encoded_time(resource: &ResourceMetrics, mut visit: impl FnMut(i64)) {
    let mut visit_all = |points: &mut dyn Iterator<Item = u64>| {
        for ts in points {
            visit(ts as i64);
        }
    };
    for scope in &resource.scope_metrics {
        for m in &scope.metrics {
            match &m.data {
                Some(metric::Data::Gauge(g)) => {
                    visit_all(&mut g.data_points.iter().map(|p| p.time_unix_nano))
                }
                Some(metric::Data::Sum(s)) => {
                    visit_all(&mut s.data_points.iter().map(|p| p.time_unix_nano))
                }
                Some(metric::Data::Histogram(h)) => {
                    visit_all(&mut h.data_points.iter().map(|p| p.time_unix_nano))
                }
                Some(metric::Data::Summary(s)) => {
                    visit_all(&mut s.data_points.iter().map(|p| p.time_unix_nano))
                }
                Some(metric::Data::ExponentialHistogram(_)) | None => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use api::v1::value::ValueData;
    use common_query::prelude::set_default_prefix;
    use otel_arrow_rust::proto::opentelemetry::common::v1::{AnyValue, any_value};
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
        ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Metric, NumberDataPoint,
        ScopeMetrics,
    };

    use super::*;

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
        }
    }

    fn gauge_at(times: &[i64]) -> ResourceMetrics {
        ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: times
                            .iter()
                            .map(|ts| NumberDataPoint {
                                time_unix_nano: *ts as u64,
                                ..Default::default()
                            })
                            .collect(),
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
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
        data.observe(&attrs, &gauge_at(&[100, 50]));
        assert_eq!(data.rows.len(), 1);
        let (tags, windows) = data.rows.iter().next().unwrap();
        assert_eq!(windows.values().copied().collect::<Vec<_>>(), vec![100]);
        assert!(tags.contains(&("job".to_string(), "shop/api".to_string())));
        assert!(tags.contains(&("instance".to_string(), "inst-1".to_string())));
        assert!(tags.contains(&("service.name".to_string(), "api".to_string())));
        assert!(
            tags.iter()
                .all(|(k, _)| k != "os.type" && k != "service.instance.id")
        );

        data.observe(&[kv("host.id", "h-2")], &gauge_at(&[10]));
        assert_eq!(data.rows.len(), 2);

        // nothing allowlisted: no row at all, rather than an empty one
        let mut empty = ResourceInfoData::default();
        empty.observe(&[kv("os.type", "linux")], &gauge_at(&[100]));
        assert!(empty.into_row_insert_requests().unwrap().is_none());
    }

    /// Earlier windows would keep their metric rows but lose their entities.
    #[test]
    fn observe_keeps_one_row_per_graph_window() {
        let window = SEMANTIC_GRAPH_WINDOW_NANOS;
        let mut data = ResourceInfoData::default();
        data.observe(
            &[kv("service.name", "api")],
            &gauge_at(&[window + 1, window + 2, 3 * window + 7]),
        );

        let windows = data.rows.values().next().unwrap();
        assert_eq!(
            windows.iter().collect::<Vec<_>>(),
            vec![(&window, &(window + 2)), (&(3 * window), &(3 * window + 7))]
        );
    }

    /// Describing a resource whose only data the encoder drops invents an
    /// entity with no measurements.
    #[test]
    fn observe_ignores_data_the_encoder_drops() {
        let mut data = ResourceInfoData::default();
        let dropped = ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                        data_points: vec![ExponentialHistogramDataPoint {
                            time_unix_nano: 100,
                            ..Default::default()
                        }],
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        data.observe(&[kv("service.name", "api")], &dropped);
        assert!(data.into_row_insert_requests().unwrap().is_none());
    }

    #[test]
    fn rows_carry_raw_key_tags_value_and_millis_timestamp() {
        set_default_prefix(None).unwrap();
        let mut data = ResourceInfoData::default();
        data.observe(
            &[kv("service.name", "api"), kv("host.id", "h-1")],
            &gauge_at(&[1_700_000_000_123_456_789]),
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
