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
use common_grpc::precision::Precision;
use common_query::prelude::{greptime_timestamp, greptime_value};
use otel_arrow_rust::proto::opentelemetry::common::v1::KeyValue;
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{ResourceMetrics, metric};

use crate::error::Result;
use crate::otlp::metrics::{INSTANCE_KEY, JOB_KEY, scalar_value_string, service_identity};
use crate::otlp::trace::{KEY_SERVICE_NAME, KEY_SERVICE_NAMESPACE};
use crate::row_writer::{self, MultiTableData};

pub const OTEL_RESOURCE_INFO_TABLE_NAME: &str = "otel_resource_info";

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
            | "host.id"
            | "host.name"
            | "container.id"
            | "container.name"
            | "k8s.pod.uid"
            | "k8s.pod.name"
            | "k8s.namespace.name"
    )
}

/// Upper bound of [`is_projected_attr`] plus the derived `job`/`instance`,
/// used to size the per-row buffers.
const MAX_PROJECTED_TAGS: usize = 11;

/// One row per distinct projected attribute set in a request, stamped with
/// the newest data-point time that observed it. Dedup is request-local; the
/// storage engine merges repeats across requests.
#[derive(Debug, Default)]
pub struct ResourceInfoData {
    rows: BTreeMap<Vec<(String, String)>, i64>,
}

impl ResourceInfoData {
    /// Takes the raw attributes, before the promote filter runs on them.
    pub fn observe(&mut self, raw_attrs: &[KeyValue], max_ts_nanos: i64) {
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

        self.rows
            .entry(tags)
            .and_modify(|ts| *ts = (*ts).max(max_ts_nanos))
            .or_insert(max_ts_nanos);
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

/// The descriptor row's observation time, so it shares a window with the
/// metric rows it describes. `None` when the resource has no data points.
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
        data.observe(&attrs, 50);
        assert_eq!(data.rows.len(), 1);
        let (tags, ts) = data.rows.iter().next().unwrap();
        assert_eq!(*ts, 100);
        assert!(tags.contains(&("job".to_string(), "shop/api".to_string())));
        assert!(tags.contains(&("instance".to_string(), "inst-1".to_string())));
        assert!(tags.contains(&("service.name".to_string(), "api".to_string())));
        assert!(
            tags.iter()
                .all(|(k, _)| k != "os.type" && k != "service.instance.id")
        );

        data.observe(&[kv("host.id", "h-2")], 10);
        assert_eq!(data.rows.len(), 2);

        // nothing allowlisted: no row at all, rather than an empty one
        let mut empty = ResourceInfoData::default();
        empty.observe(&[kv("os.type", "linux")], 100);
        assert!(empty.into_row_insert_requests().unwrap().is_none());
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
