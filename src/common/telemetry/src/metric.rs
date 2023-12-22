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

use greptime_proto::prometheus::remote::{Sample, TimeSeries};
use greptime_proto::prometheus::*;
use prometheus::proto::{LabelPair, MetricFamily, MetricType};
use prometheus::{Encoder, TextEncoder};

pub fn dump_metrics() -> Result<String, String> {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|_| "Encode metrics failed".to_string())?;
    String::from_utf8(buffer).map_err(|e| e.to_string())
}

/// `MetricFilter` used in `report_metric_task`.
/// for metric user don't want collect, return a `false`, else return a `true`
#[derive(Clone)]
pub struct MetricFilter {
    inner: Arc<dyn Fn(&MetricFamily) -> bool + Send + Sync>,
}

impl MetricFilter {
    pub fn new(inner: Arc<dyn Fn(&MetricFamily) -> bool + Send + Sync>) -> Self {
        Self { inner }
    }
    pub fn filter(&self, mf: &MetricFamily) -> bool {
        (self.inner)(mf)
    }
}

pub fn convert_metric_to_write_request(
    metric_families: Vec<MetricFamily>,
    metric_filter: Option<&MetricFilter>,
    default_timestamp: i64,
) -> remote::WriteRequest {
    let mut timeseries: Vec<TimeSeries> = Vec::with_capacity(metric_families.len());
    for mf in metric_families {
        if !metric_filter.map(|f| f.filter(&mf)).unwrap_or(true) {
            continue;
        }
        let mf_type = mf.get_field_type();
        let mf_name = mf.get_name();
        for m in mf.get_metric() {
            let timestamp = if m.get_timestamp_ms() == 0 {
                default_timestamp
            } else {
                m.get_timestamp_ms()
            };
            match mf_type {
                MetricType::COUNTER => timeseries.push(TimeSeries {
                    labels: convert_label(m.get_label(), mf_name, None),
                    samples: vec![Sample {
                        value: m.get_counter().get_value(),
                        timestamp,
                    }],
                    exemplars: vec![],
                }),
                MetricType::GAUGE => timeseries.push(TimeSeries {
                    labels: convert_label(m.get_label(), mf_name, None),
                    samples: vec![Sample {
                        value: m.get_gauge().get_value(),
                        timestamp,
                    }],
                    exemplars: vec![],
                }),
                MetricType::HISTOGRAM => {
                    let h = m.get_histogram();
                    let mut inf_seen = false;
                    let metric_name = format!("{}_bucket", mf_name);
                    for b in h.get_bucket() {
                        let upper_bound = b.get_upper_bound();
                        timeseries.push(TimeSeries {
                            labels: convert_label(
                                m.get_label(),
                                metric_name.as_str(),
                                Some(("le", upper_bound.to_string())),
                            ),
                            samples: vec![Sample {
                                value: b.get_cumulative_count() as f64,
                                timestamp,
                            }],
                            exemplars: vec![],
                        });
                        if upper_bound.is_sign_positive() && upper_bound.is_infinite() {
                            inf_seen = true;
                        }
                    }
                    if !inf_seen {
                        timeseries.push(TimeSeries {
                            labels: convert_label(
                                m.get_label(),
                                metric_name.as_str(),
                                Some(("le", "+Inf".to_string())),
                            ),
                            samples: vec![Sample {
                                value: h.get_sample_count() as f64,
                                timestamp,
                            }],
                            exemplars: vec![],
                        });
                    }
                    timeseries.push(TimeSeries {
                        labels: convert_label(
                            m.get_label(),
                            format!("{}_sum", mf_name).as_str(),
                            None,
                        ),
                        samples: vec![Sample {
                            value: h.get_sample_sum(),
                            timestamp,
                        }],
                        exemplars: vec![],
                    });
                    timeseries.push(TimeSeries {
                        labels: convert_label(
                            m.get_label(),
                            format!("{}_count", mf_name).as_str(),
                            None,
                        ),
                        samples: vec![Sample {
                            value: h.get_sample_count() as f64,
                            timestamp,
                        }],
                        exemplars: vec![],
                    });
                }
                MetricType::SUMMARY => {
                    let s = m.get_summary();
                    for q in s.get_quantile() {
                        timeseries.push(TimeSeries {
                            labels: convert_label(
                                m.get_label(),
                                mf_name,
                                Some(("quantile", q.get_quantile().to_string())),
                            ),
                            samples: vec![Sample {
                                value: q.get_value(),
                                timestamp,
                            }],
                            exemplars: vec![],
                        });
                    }
                    timeseries.push(TimeSeries {
                        labels: convert_label(
                            m.get_label(),
                            format!("{}_sum", mf_name).as_str(),
                            None,
                        ),
                        samples: vec![Sample {
                            value: s.get_sample_sum(),
                            timestamp,
                        }],
                        exemplars: vec![],
                    });
                    timeseries.push(TimeSeries {
                        labels: convert_label(
                            m.get_label(),
                            format!("{}_count", mf_name).as_str(),
                            None,
                        ),
                        samples: vec![Sample {
                            value: s.get_sample_count() as f64,
                            timestamp,
                        }],
                        exemplars: vec![],
                    });
                }
                MetricType::UNTYPED => {
                    // `TextEncoder` `MetricType::UNTYPED` unimplemented
                    // To keep the implementation consistent and not cause unexpected panics, we do nothing here.
                }
            };
        }
    }
    remote::WriteRequest {
        timeseries,
        metadata: vec![],
    }
}

fn convert_label(
    pairs: &[LabelPair],
    name: &str,
    addon: Option<(&'static str, String)>,
) -> Vec<remote::Label> {
    let mut labels = Vec::with_capacity(pairs.len() + 1 + if addon.is_some() { 1 } else { 0 });
    for label in pairs {
        labels.push(remote::Label {
            name: label.get_name().to_string(),
            value: label.get_value().to_string(),
        });
    }
    labels.push(remote::Label {
        name: "__name__".to_string(),
        value: name.to_string(),
    });
    if let Some(addon) = addon {
        labels.push(remote::Label {
            name: addon.0.to_string(),
            value: addon.1,
        });
    }
    // Remote write protocol need label names sorted in lexicographical order.
    labels.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    labels
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use prometheus::core::Collector;
    use prometheus::proto::{LabelPair, MetricFamily, MetricType};
    use prometheus::{Counter, Gauge, Histogram, HistogramOpts, Opts};

    use super::convert_label;
    use crate::metric::{convert_metric_to_write_request, MetricFilter};

    #[test]
    fn test_convert_label() {
        let pairs = vec![
            {
                let mut pair = LabelPair::new();
                pair.set_name(String::from("a"));
                pair.set_value(String::from("b"));
                pair
            },
            {
                let mut pair = LabelPair::new();
                pair.set_name(String::from("e"));
                pair.set_value(String::from("g"));
                pair
            },
        ];
        let label1 = convert_label(&pairs, "label1", None);
        assert_eq!(
            format!("{:?}", label1),
            r#"[Label { name: "__name__", value: "label1" }, Label { name: "a", value: "b" }, Label { name: "e", value: "g" }]"#
        );
        let label2 = convert_label(&pairs, "label2", Some(("c", "c".to_string())));
        assert_eq!(
            format!("{:?}", label2),
            r#"[Label { name: "__name__", value: "label2" }, Label { name: "a", value: "b" }, Label { name: "c", value: "c" }, Label { name: "e", value: "g" }]"#
        );
    }

    #[test]
    fn test_write_request_encoder() {
        let counter_opts = Opts::new("test_counter", "test help")
            .const_label("a", "1")
            .const_label("b", "2");
        let counter = Counter::with_opts(counter_opts).unwrap();
        counter.inc();

        let mf = counter.collect();
        let write_quest = convert_metric_to_write_request(mf, None, 0);

        assert_eq!(
            format!("{:?}", write_quest.timeseries),
            r#"[TimeSeries { labels: [Label { name: "__name__", value: "test_counter" }, Label { name: "a", value: "1" }, Label { name: "b", value: "2" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }]"#
        );

        let gauge_opts = Opts::new("test_gauge", "test help")
            .const_label("a", "1")
            .const_label("b", "2");
        let gauge = Gauge::with_opts(gauge_opts).unwrap();
        gauge.inc();
        gauge.set(42.0);

        let mf = gauge.collect();
        let write_quest = convert_metric_to_write_request(mf, None, 0);
        assert_eq!(
            format!("{:?}", write_quest.timeseries),
            r#"[TimeSeries { labels: [Label { name: "__name__", value: "test_gauge" }, Label { name: "a", value: "1" }, Label { name: "b", value: "2" }], samples: [Sample { value: 42.0, timestamp: 0 }], exemplars: [] }]"#
        );
    }

    #[test]
    fn test_write_request_histogram() {
        let opts = HistogramOpts::new("test_histogram", "test help").const_label("a", "1");
        let histogram = Histogram::with_opts(opts).unwrap();
        histogram.observe(0.25);

        let mf = histogram.collect();
        let write_quest = convert_metric_to_write_request(mf, None, 0);
        let write_quest_str: Vec<_> = write_quest
            .timeseries
            .iter()
            .map(|x| format!("{:?}", x))
            .collect();
        let ans = r#"TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "0.005" }], samples: [Sample { value: 0.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "0.01" }], samples: [Sample { value: 0.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "0.025" }], samples: [Sample { value: 0.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "0.05" }], samples: [Sample { value: 0.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "0.1" }], samples: [Sample { value: 0.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "0.25" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "0.5" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "1" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "2.5" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "5" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "10" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_bucket" }, Label { name: "a", value: "1" }, Label { name: "le", value: "+Inf" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_sum" }, Label { name: "a", value: "1" }], samples: [Sample { value: 0.25, timestamp: 0 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_histogram_count" }, Label { name: "a", value: "1" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }"#;
        assert_eq!(write_quest_str.join("\n"), ans);
    }

    #[test]
    fn test_write_request_summary() {
        use prometheus::proto::{Metric, Quantile, Summary};

        let mut metric_family = MetricFamily::default();
        metric_family.set_name("test_summary".to_string());
        metric_family.set_help("This is a test summary statistic".to_string());
        metric_family.set_field_type(MetricType::SUMMARY);

        let mut summary = Summary::default();
        summary.set_sample_count(5.0 as u64);
        summary.set_sample_sum(15.0);

        let mut quantile1 = Quantile::default();
        quantile1.set_quantile(50.0);
        quantile1.set_value(3.0);

        let mut quantile2 = Quantile::default();
        quantile2.set_quantile(100.0);
        quantile2.set_value(5.0);

        summary.set_quantile(vec![quantile1, quantile2].into());

        let mut metric = Metric::default();
        metric.set_summary(summary);
        metric_family.set_metric(vec![metric].into());

        let write_quest = convert_metric_to_write_request(vec![metric_family], None, 20);
        let write_quest_str: Vec<_> = write_quest
            .timeseries
            .iter()
            .map(|x| format!("{:?}", x))
            .collect();
        let ans = r#"TimeSeries { labels: [Label { name: "__name__", value: "test_summary" }, Label { name: "quantile", value: "50" }], samples: [Sample { value: 3.0, timestamp: 20 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_summary" }, Label { name: "quantile", value: "100" }], samples: [Sample { value: 5.0, timestamp: 20 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_summary_sum" }], samples: [Sample { value: 15.0, timestamp: 20 }], exemplars: [] }
TimeSeries { labels: [Label { name: "__name__", value: "test_summary_count" }], samples: [Sample { value: 5.0, timestamp: 20 }], exemplars: [] }"#;
        assert_eq!(write_quest_str.join("\n"), ans);
    }

    #[test]
    fn test_metric_filter() {
        let counter_opts = Opts::new("filter_counter", "test help")
            .const_label("a", "1")
            .const_label("b", "2");
        let counter_1 = Counter::with_opts(counter_opts).unwrap();
        counter_1.inc_by(1.0);
        let counter_opts = Opts::new("test_counter", "test help")
            .const_label("a", "1")
            .const_label("b", "2");
        let counter_2 = Counter::with_opts(counter_opts).unwrap();
        counter_2.inc_by(2.0);

        let mut mf = counter_1.collect();
        mf.append(&mut counter_2.collect());

        let filter = MetricFilter::new(Arc::new(|mf: &MetricFamily| {
            !mf.get_name().starts_with("filter")
        }));
        let write_quest1 = convert_metric_to_write_request(mf.clone(), None, 0);
        let write_quest2 = convert_metric_to_write_request(mf, Some(&filter), 0);
        assert_eq!(
            format!("{:?}", write_quest1.timeseries),
            r#"[TimeSeries { labels: [Label { name: "__name__", value: "filter_counter" }, Label { name: "a", value: "1" }, Label { name: "b", value: "2" }], samples: [Sample { value: 1.0, timestamp: 0 }], exemplars: [] }, TimeSeries { labels: [Label { name: "__name__", value: "test_counter" }, Label { name: "a", value: "1" }, Label { name: "b", value: "2" }], samples: [Sample { value: 2.0, timestamp: 0 }], exemplars: [] }]"#
        );
        assert_eq!(
            format!("{:?}", write_quest2.timeseries),
            r#"[TimeSeries { labels: [Label { name: "__name__", value: "test_counter" }, Label { name: "a", value: "1" }, Label { name: "b", value: "2" }], samples: [Sample { value: 2.0, timestamp: 0 }], exemplars: [] }]"#
        );
    }
}
