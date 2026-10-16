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
use datatypes::series::raw::{RawSeriesChunk, RawSeriesOrdering, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, MetricName};
use futures::{Stream, StreamExt};

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
            label_source: self.label_source.clone(),
            polls: self.polls.clone(),
            drops: self.drops.clone(),
        }))
    }
}

struct MemoryReader {
    chunks: VecDeque<RawSeriesChunk>,
    schema: Arc<SeriesSchema>,
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

    fn ordering(&self) -> RawSeriesOrdering {
        RawSeriesOrdering::Contiguous
    }
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
                logical_tags: Arc::from([]),
            }
        },
        polls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    })
}

#[tokio::test]
async fn prepared_identity_matches_chunks_in_both_label_modes_including_empty_reader() {
    for explicit in [false, true] {
        let source = source(explicit);
        assert_eq!(source.num_series(), 2);
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
        assert_eq!(empty.ordering(), RawSeriesOrdering::Contiguous);
        assert_eq!(
            matches!(empty.label_source(), RawLabelSource::Explicit),
            explicit
        );
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
        assert_eq!(
            matches!(end.label_source(), RawLabelSource::Explicit),
            explicit
        );

        let chunks =
            futures::TryStreamExt::try_collect::<Vec<_>>(source.read_series(0..2).unwrap())
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
    assert!(source.read_series(2..1).is_err());
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
