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

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::buffer::Buffer;
use datatypes::error::InvalidSeriesSnafu;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{
    LABEL_DECODE_VERSION, LabelDecodeColumn, LabelDecodeEncoding, LabelDecodeSpec, LabelDecodeTag,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, MetricName};
use futures::{Stream, StreamExt};

use crate::codec::PrimaryKeyEncoding;
use crate::region_engine::{
    PreparedSeriesSource, RawLabelSource, RawSeriesStream, SendableRawSeriesStream, SeriesInfo,
};

#[derive(Clone)]
struct TestSeries {
    info: SeriesInfo,
    chunks: Vec<RawSeriesChunk>,
}

struct MemorySource {
    series: Arc<[TestSeries]>,
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl PreparedSeriesSource for MemorySource {
    fn num_series(&self) -> usize {
        self.series.len()
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    fn series_info(&self, ordinal: usize) -> Result<SeriesInfo, BoxedError> {
        self.series
            .get(ordinal)
            .map(|series| series.info.clone())
            .ok_or_else(|| {
                BoxedError::new(
                    InvalidSeriesSnafu {
                        reason: "invalid series ordinal".to_string(),
                    }
                    .build(),
                )
            })
    }

    fn read_series(
        &self,
        range: std::ops::Range<usize>,
    ) -> Result<SendableRawSeriesStream, BoxedError> {
        if range.start > range.end || range.end > self.series.len() {
            return Err(BoxedError::new(
                InvalidSeriesSnafu {
                    reason: "invalid series range".to_string(),
                }
                .build(),
            ));
        }
        let chunks = self.series[range]
            .iter()
            .flat_map(|series| series.chunks.iter().cloned())
            .collect();
        Ok(Box::pin(MemoryReader {
            chunks,
            schema: self.schema.clone(),
            label_decode_spec: declared_decode_spec(&self.label_source),
            label_source: self.label_source.clone(),
            polls: self.polls.clone(),
            drops: self.drops.clone(),
        }))
    }
}

struct MemoryReader {
    chunks: VecDeque<RawSeriesChunk>,
    schema: Arc<SeriesSchema>,
    /// The description this fixture declares, derived once from `label_source`, as a producer does.
    label_decode_spec: Option<LabelDecodeSpec>,
    label_source: RawLabelSource,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Stream for MemoryReader {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let chunk = self.chunks.pop_front();
        if chunk.is_some() {
            self.polls.fetch_add(1, Ordering::SeqCst);
        }
        Poll::Ready(chunk.map(Ok))
    }
}

impl Drop for MemoryReader {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl RawSeriesStream for MemoryReader {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        self.label_decode_spec.as_ref()
    }

    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::Contiguous
    }
}

/// Describes the key-encoded chunks a reader of `source` yields.
///
/// This crate cannot depend on `common-query` (which depends on it), so this fixture repeats the
/// derivation `common_query::series_frame::label_decode_spec` performs for a producer: the metric
/// name, the primary-key encoding, the ordered key columns with their physical types, and the
/// logical tags in canonical order. An explicit binding already carries complete labels and
/// declares nothing.
fn declared_decode_spec(source: &RawLabelSource) -> Option<LabelDecodeSpec> {
    let RawLabelSource::PrimaryKey {
        metric_name,
        physical_metadata,
        logical_tags,
    } = source
    else {
        return None;
    };
    let mut description = LabelDecodeSpec {
        version: LABEL_DECODE_VERSION,
        metric_name: metric_name.to_string(),
        encoding: match physical_metadata.primary_key_encoding {
            PrimaryKeyEncoding::Dense => LabelDecodeEncoding::Dense,
            PrimaryKeyEncoding::Sparse => LabelDecodeEncoding::Sparse,
        },
        primary_key: physical_metadata
            .primary_key_columns()
            .map(|column| LabelDecodeColumn {
                column_id: column.column_id,
                name: column.column_schema.name.clone(),
                physical_type: column.column_schema.data_type.clone(),
            })
            .collect(),
        logical_tags: logical_tags
            .iter()
            .map(|tag| LabelDecodeTag {
                column_id: tag.column_id,
                name: tag.name.to_string(),
            })
            .collect(),
    };
    description.canonicalize();
    Some(description)
}

fn metric(host: &str) -> MetricName {
    MetricName::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(host),
        }],
    )
    .unwrap()
}

fn assert_same_label_source(expected: &RawLabelSource, actual: &RawLabelSource) {
    match (expected, actual) {
        (RawLabelSource::Explicit, RawLabelSource::Explicit) => {}
        (
            RawLabelSource::PrimaryKey {
                metric_name: expected_name,
                physical_metadata: expected_metadata,
                logical_tags: expected_tags,
            },
            RawLabelSource::PrimaryKey {
                metric_name: actual_name,
                physical_metadata: actual_metadata,
                logical_tags: actual_tags,
            },
        ) => {
            assert_eq!(expected_name, actual_name);
            assert!(Arc::ptr_eq(expected_metadata, actual_metadata));
            assert_eq!(expected_tags.len(), actual_tags.len());
            for (expected, actual) in expected_tags.iter().zip(actual_tags.iter()) {
                assert_eq!(expected.name, actual.name);
                assert_eq!(expected.column_id, actual.column_id);
            }
        }
        _ => panic!("label source differs"),
    }
}

fn schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("usage"),
        }]
        .into(),
        eval_grid: None,
    })
}

fn chunk(
    key: &[u8],
    metric: Option<MetricName>,
    timestamp: i64,
    value: f64,
    last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        &schema(),
        Buffer::from(key),
        metric,
        RawTimestamps::try_new(vec![timestamp].into()).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(vec![value]))),
        last,
    )
    .unwrap()
}

fn primary_key_metadata() -> crate::metadata::RegionMetadata {
    let mut builder =
        crate::metadata::RegionMetadataBuilder::new(crate::storage::RegionId::new(1, 1));
    builder.push_column_metadata(crate::metadata::ColumnMetadata {
        column_schema: ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        semantic_type: SemanticType::Tag,
        column_id: 1,
    });
    builder.push_column_metadata(crate::metadata::ColumnMetadata {
        column_schema: ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
        column_id: 2,
    });
    builder.primary_key(vec![1]);
    builder.build().unwrap()
}

fn empty_source() -> Arc<MemorySource> {
    Arc::new(MemorySource {
        series: Arc::from([]),
        schema: schema(),
        label_source: RawLabelSource::Explicit,
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    })
}

fn source(explicit: bool) -> Arc<MemorySource> {
    source_from_keys(
        explicit,
        vec![
            Buffer::from(vec![0x10, 0x01]),
            Buffer::from(vec![0x10, 0x02]),
        ]
        .into(),
    )
}

fn source_from_keys(explicit: bool, keys: Arc<[Buffer]>) -> Arc<MemorySource> {
    assert_eq!(keys.len(), 2);
    let first_metric = explicit.then(|| metric("a"));
    let second_metric = explicit.then(|| metric("b"));
    let series = vec![
        TestSeries {
            info: SeriesInfo {
                primary_key: keys[0].clone(),
                metric: first_metric.clone(),
            },
            chunks: vec![chunk(keys[0].as_slice(), first_metric, 10, 1.0, true)],
        },
        TestSeries {
            info: SeriesInfo {
                primary_key: keys[1].clone(),
                metric: second_metric.clone(),
            },
            chunks: vec![
                chunk(keys[1].as_slice(), second_metric.clone(), 10, 2.0, false),
                chunk(keys[1].as_slice(), second_metric, 20, 3.0, true),
            ],
        },
    ];
    Arc::new(MemorySource {
        series: series.into(),
        schema: schema(),
        label_source: if explicit {
            RawLabelSource::Explicit
        } else {
            RawLabelSource::PrimaryKey {
                metric_name: Arc::from("cpu"),
                physical_metadata: Arc::new(primary_key_metadata()),
                logical_tags: vec![crate::region_engine::BoundTag {
                    column_id: 1,
                    name: Arc::from("host"),
                }]
                .into(),
            }
        },
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    })
}

#[tokio::test]
async fn prepared_source_label_binding_is_available_before_readers_and_matches_chunks() {
    for explicit in [false, true] {
        let source = source(explicit);
        assert_eq!(source.num_series(), 2);
        match (explicit, source.label_source()) {
            (true, RawLabelSource::Explicit) => {}
            (
                false,
                RawLabelSource::PrimaryKey {
                    metric_name,
                    physical_metadata,
                    logical_tags,
                },
            ) => {
                assert_eq!(metric_name.as_ref(), "cpu");
                assert_eq!(
                    physical_metadata
                        .column_by_id(1)
                        .unwrap()
                        .column_schema
                        .name,
                    "host"
                );
                assert_eq!(logical_tags.len(), 1);
                assert_eq!(logical_tags[0].column_id, 1);
                assert_eq!(logical_tags[0].name.as_ref(), "host");
            }
            _ => panic!("unexpected source label binding"),
        }
        assert_eq!(source.polls.load(Ordering::SeqCst), 0);
        let first = source.series_info(0).unwrap();
        let second = source.series_info(1).unwrap();
        assert_eq!(first.primary_key, Buffer::from(vec![0x10, 0x01]));
        assert_eq!(second.primary_key, Buffer::from(vec![0x10, 0x02]));
        assert_eq!(
            first.metric.as_ref().and_then(MetricName::name),
            if explicit { Some("cpu") } else { None }
        );
        assert_eq!(
            first.metric.as_ref().and_then(|m| m.get("host")),
            explicit.then_some("a")
        );
        assert_eq!(
            second.metric.as_ref().and_then(|m| m.get("host")),
            explicit.then_some("b")
        );

        let empty = source.read_series(1..1).unwrap();
        assert_eq!(empty.schema().fields.len(), 1);
        assert_eq!(empty.ordering(), SeriesOrdering::Contiguous);
        assert_same_label_source(source.label_source(), empty.label_source());
        assert_eq!(source.polls.load(Ordering::SeqCst), 0);
        assert!(
            futures::TryStreamExt::try_collect::<Vec<_>>(empty)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(source.polls.load(Ordering::SeqCst), 0);
        let end = source.read_series(2..2).unwrap();
        assert_eq!(end.schema().fields.len(), 1);
        assert_eq!(end.ordering(), SeriesOrdering::Contiguous);
        assert_same_label_source(source.label_source(), end.label_source());
        assert_eq!(source.polls.load(Ordering::SeqCst), 0);

        let reader = source.read_series(0..2).unwrap();
        assert_same_label_source(source.label_source(), reader.label_source());
        assert_eq!(source.polls.load(Ordering::SeqCst), 0);
        let chunks = futures::TryStreamExt::try_collect::<Vec<_>>(reader)
            .await
            .unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(source.polls.load(Ordering::SeqCst), 3);
        assert_eq!(chunks[1].time.timestamp(0), Some(10));
        assert_eq!(chunks[2].time.timestamp(0), Some(20));
        assert!(
            matches!(chunks[1].fields.get(0).unwrap().sample(0), datatypes::series::samples::SampleRef::Float(value) if value == 2.0)
        );
        assert!(
            matches!(chunks[2].fields.get(0).unwrap().sample(0), datatypes::series::samples::SampleRef::Float(value) if value == 3.0)
        );
        for chunk in &chunks {
            let ordinal = if chunk.primary_key == first.primary_key {
                0
            } else {
                1
            };
            let info = source.series_info(ordinal).unwrap();
            assert_eq!(chunk.primary_key, info.primary_key);
            assert_eq!(chunk.metric, info.metric);
        }
        assert_eq!(chunks[0].metric.is_some(), explicit);
        assert!(!chunks[1].is_last);
        assert!(chunks[2].is_last);
    }
}

#[tokio::test]
async fn empty_prepared_source_supports_its_empty_range() {
    let source = empty_source();
    assert!(matches!(source.label_source(), RawLabelSource::Explicit));
    assert_eq!(source.num_series(), 0);
    let stream = source.read_series(0..0).unwrap();
    assert_eq!(stream.schema().fields.len(), 1);
    assert!(
        futures::TryStreamExt::try_collect::<Vec<_>>(stream)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(source.polls.load(Ordering::SeqCst), 0);
}

#[test]
fn invalid_ordinals_and_ranges_fail_without_polling_samples() {
    let source = source(false);
    assert!(source.series_info(2).is_err());
    assert!(source.read_series(2..3).is_err());
    assert!(
        source
            .read_series(std::ops::Range { start: 2, end: 1 })
            .is_err()
    );
    assert_eq!(source.polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn overlapping_readers_are_independent_across_source_and_reader_drops() {
    let source = source(false);
    let drops = source.drops.clone();
    let mut left = source.read_series(0..2).unwrap();
    let mut right = source.read_series(1..2).unwrap();
    drop(source);
    assert!(left.next().await.unwrap().unwrap().is_last);
    assert_eq!(
        right.next().await.unwrap().unwrap().primary_key,
        Buffer::from(vec![0x10, 0x02])
    );
    drop(left);
    assert_eq!(
        futures::TryStreamExt::try_collect::<Vec<_>>(right)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn prepared_selection_is_independent_of_later_mutable_selection_changes() {
    let selected = Arc::new(Mutex::new(vec![
        Buffer::from(vec![1_u8]),
        Buffer::from(vec![2_u8]),
    ]));
    let prepared = source_from_keys(false, selected.lock().unwrap().clone().into());
    selected.lock().unwrap().push(Buffer::from(vec![3_u8]));

    assert_eq!(prepared.num_series(), 2);
    assert_eq!(
        prepared.series_info(0).unwrap().primary_key,
        Buffer::from(vec![1_u8])
    );
    assert_eq!(
        prepared.series_info(1).unwrap().primary_key,
        Buffer::from(vec![2_u8])
    );
    let chunks = futures::TryStreamExt::try_collect::<Vec<_>>(prepared.read_series(0..2).unwrap())
        .await
        .unwrap();
    assert_eq!(chunks.len(), 3);
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.primary_key != Buffer::from(vec![3_u8]))
    );
}
