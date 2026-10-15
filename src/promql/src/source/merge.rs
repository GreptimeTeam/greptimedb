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

#[cfg(test)]
mod tests;

use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use datatypes::error::InvalidSeriesSnafu;
use datatypes::series::raw::{RawSeriesChunk, RawSeriesOrdering};
use datatypes::series::samples::{SeriesFields, SeriesSchema};
use futures::Stream;
use mito_codec::labels::{LabelProjection, PrimaryKeyLabelDecoder};
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

/// Merge compatible, explicitly supplied raw streams by complete read key and time.
///
/// Callers provide authorized bindings and own snapshot/storage policy. This does not select an
/// MVCC view, resolve overlapping physical copies, or roll back emitted prefixes after errors.
/// `is_last` is global only over these inputs; there is no fallback, replay, or input sorting.
pub fn merge_ordered_raw_series(
    inputs: Vec<SendableRawSeriesStream>,
) -> Result<SendableRawSeriesStream, BoxedError> {
    let mut inputs = inputs.into_iter();
    let first = inputs
        .next()
        .ok_or_else(|| invalid("raw merge requires at least one input"))?;
    let schema = Arc::clone(first.schema());
    let binding = first.label_source().clone();
    let mut streams = vec![Input::new(first)];
    for stream in inputs {
        streams.push(Input::new(stream));
    }
    for input in &streams {
        let stream = input
            .stream
            .as_ref()
            .ok_or_else(|| invalid("raw merge input disappeared"))?;
        validate_binding(stream.label_source())?;
        validate_input(stream.as_ref().get_ref(), &schema, &binding)?;
    }
    Ok(Box::pin(OrderedRawMerge {
        streams,
        schema,
        binding,
        candidate: None,
    }))
}

fn invalid(reason: &'static str) -> BoxedError {
    BoxedError::new(InvalidSeriesSnafu { reason }.build())
}

fn validate_binding(binding: &RawLabelSource) -> Result<(), BoxedError> {
    PrimaryKeyLabelDecoder::new(binding, LabelProjection::All)
        .map(|_| ())
        .map_err(BoxedError::new)
}

fn validate_input(
    stream: &dyn RawSeriesStream,
    schema: &SeriesSchema,
    binding: &RawLabelSource,
) -> Result<(), BoxedError> {
    let other = stream.schema();
    if stream.ordering() != RawSeriesOrdering::ReadKeyThenTime
        || other.eval_grid.is_some()
        || other.fields.is_empty()
        || other.fields.len() != schema.fields.len()
        || other
            .fields
            .iter()
            .zip(schema.fields.iter())
            .any(|(a, b)| a.name != b.name)
        || !bindings_compatible(stream.label_source(), binding)
    {
        return Err(invalid(
            "raw merge requires equal raw schemas, primary-key bindings, and ReadKeyThenTime ordering",
        ));
    }
    Ok(())
}

fn bindings_compatible(left: &RawLabelSource, right: &RawLabelSource) -> bool {
    let (
        RawLabelSource::PrimaryKey {
            metric_name: lm,
            physical_metadata: lmeta,
            logical_tags: lt,
        },
        RawLabelSource::PrimaryKey {
            metric_name: rm,
            physical_metadata: rmeta,
            logical_tags: rt,
        },
    ) = (left, right)
    else {
        return false;
    };
    if lm != rm
        || lt.len() != rt.len()
        || lt.iter().any(|tag| {
            !rt.iter()
                .any(|other| tag.column_id == other.column_id && tag.name == other.name)
        })
    {
        return false;
    }
    let lm = lmeta.as_ref();
    let rm = rmeta.as_ref();
    lm.primary_key == rm.primary_key
        && lm.primary_key_encoding == rm.primary_key_encoding
        && lm
            .primary_key
            .iter()
            .all(|id| match (lm.column_by_id(*id), rm.column_by_id(*id)) {
                (Some(a), Some(b)) => {
                    a.column_id == b.column_id
                        && a.column_schema.name == b.column_schema.name
                        && a.column_schema.data_type == b.column_schema.data_type
                        && a.semantic_type == b.semantic_type
                }
                _ => false,
            })
}

struct Input {
    stream: Option<SendableRawSeriesStream>,
    head: Option<RawSeriesChunk>,
    previous_key: Option<Vec<u8>>,
    previous_time: Option<i64>,
    ended_series: bool,
}

impl Input {
    fn new(stream: SendableRawSeriesStream) -> Self {
        Self {
            stream: Some(stream),
            head: None,
            previous_key: None,
            previous_time: None,
            ended_series: false,
        }
    }

    fn validate(
        &mut self,
        chunk: &RawSeriesChunk,
        schema: &SeriesSchema,
    ) -> Result<(), BoxedError> {
        if chunk.metric.is_some() {
            return Err(invalid("raw merge requires undecoded primary-key chunks"));
        }
        RawSeriesChunk::try_new(
            schema,
            chunk.read_key.clone(),
            None,
            chunk.time.clone(),
            chunk.fields.clone(),
            chunk.is_last,
        )
        .map_err(BoxedError::new)?;
        let times = chunk
            .time
            .as_explicit()
            .ok_or_else(|| invalid("raw merge requires explicit timestamps"))?;
        if let Some(previous_key) = &self.previous_key {
            match previous_key.as_slice().cmp(chunk.read_key.as_slice()) {
                Ordering::Greater => {
                    return Err(invalid("raw merge input read keys are not ordered"));
                }
                Ordering::Equal
                    if self.ended_series
                        || self.previous_time.is_some_and(|time| times[0] <= time) =>
                {
                    return Err(invalid(
                        "raw merge input has repeated, non-increasing timestamps or invalid is_last",
                    ));
                }
                Ordering::Less if !self.ended_series => {
                    return Err(invalid("raw merge input changed read keys before is_last"));
                }
                _ => {}
            }
        }
        self.previous_key = Some(chunk.read_key.as_slice().to_vec());
        self.previous_time = times.last().copied();
        self.ended_series = chunk.is_last;
        Ok(())
    }
}

struct OrderedRawMerge {
    streams: Vec<Input>,
    schema: Arc<SeriesSchema>,
    binding: RawLabelSource,
    candidate: Option<RawSeriesChunk>,
}

impl OrderedRawMerge {
    fn fail(&mut self, error: BoxedError) -> Poll<Option<Result<RawSeriesChunk, BoxedError>>> {
        self.streams.clear();
        self.candidate = None;
        Poll::Ready(Some(Err(error)))
    }

    fn poll_inputs(&mut self, cx: &mut Context<'_>) -> Result<bool, BoxedError> {
        let mut pending = false;
        for input in &mut self.streams {
            if input.stream.is_none() || input.head.is_some() {
                continue;
            }
            match input
                .stream
                .as_mut()
                .map(|stream| stream.as_mut().poll_next(cx))
            {
                Some(Poll::Pending) => pending = true,
                Some(Poll::Ready(Some(Err(error)))) => return Err(error),
                Some(Poll::Ready(Some(Ok(chunk)))) => {
                    input.validate(&chunk, &self.schema)?;
                    input.head = Some(chunk);
                }
                Some(Poll::Ready(None)) => {
                    if input.previous_key.is_some() && !input.ended_series {
                        return Err(invalid("raw merge input ended before its final chunk"));
                    }
                    input.stream = None;
                    input.previous_key = None;
                    input.previous_time = None;
                }
                None => {}
            }
        }
        Ok(pending)
    }

    fn take_next(&mut self) -> Result<Option<RawSeriesChunk>, BoxedError> {
        let Some(index) = self
            .streams
            .iter()
            .enumerate()
            .filter_map(|(index, input)| input.head.as_ref().map(|head| (index, head)))
            .min_by(|(_, a), (_, b)| {
                a.read_key
                    .as_slice()
                    .cmp(b.read_key.as_slice())
                    .then_with(|| a.time.timestamp(0).cmp(&b.time.timestamp(0)))
            })
            .map(|(index, _)| index)
        else {
            return Ok(None);
        };
        let chunk = self.streams[index]
            .head
            .take()
            .ok_or_else(|| invalid("raw merge lost selected head"))?;
        let key = chunk.read_key.as_slice();
        let start = chunk
            .time
            .timestamp(0)
            .ok_or_else(|| invalid("raw merge chunk has no timestamp"))?;
        let mut end = chunk.time.len();
        for input in &self.streams {
            if let Some(head) = &input.head {
                if head.read_key.as_slice() == key {
                    let next = head
                        .time
                        .timestamp(0)
                        .ok_or_else(|| invalid("raw merge chunk has no timestamp"))?;
                    if next == start {
                        return Err(invalid(
                            "raw merge inputs contain duplicate timestamps for a complete read key",
                        ));
                    }
                    if next > start {
                        end = end.min(
                            chunk
                                .time
                                .as_explicit()
                                .ok_or_else(|| invalid("raw merge requires explicit timestamps"))?
                                .partition_point(|timestamp| *timestamp < next),
                        );
                    }
                }
            }
        }
        if end == chunk.time.len() {
            return Ok(Some(RawSeriesChunk {
                read_key: chunk.read_key,
                metric: None,
                time: chunk.time,
                fields: chunk.fields,
                is_last: false,
            }));
        }
        let fields = SeriesFields::try_from_vec(
            chunk
                .fields
                .iter()
                .map(|samples| samples.slice(0, end).map_err(BoxedError::new))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(BoxedError::new)?;
        let time = chunk.time.slice(0, end).map_err(BoxedError::new)?;
        if end < chunk.time.len() {
            let remainder = SeriesFields::try_from_vec(
                chunk
                    .fields
                    .iter()
                    .map(|samples| {
                        samples
                            .slice(end, chunk.time.len() - end)
                            .map_err(BoxedError::new)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(BoxedError::new)?;
            self.streams[index].head = Some(RawSeriesChunk {
                read_key: chunk.read_key.clone(),
                metric: None,
                time: chunk
                    .time
                    .slice(end, chunk.time.len() - end)
                    .map_err(BoxedError::new)?,
                fields: remainder,
                is_last: chunk.is_last,
            });
        }
        Ok(Some(RawSeriesChunk {
            read_key: chunk.read_key,
            metric: None,
            time,
            fields,
            is_last: false,
        }))
    }
}

impl Stream for OrderedRawMerge {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.streams.is_empty() {
            return Poll::Ready(None);
        }
        if this.candidate.is_none() {
            match this.poll_inputs(cx) {
                Err(error) => return this.fail(error),
                Ok(true) => return Poll::Pending,
                Ok(false) => {}
            }
            match this.take_next() {
                Ok(candidate) => this.candidate = candidate,
                Err(error) => return this.fail(error),
            }
        }
        match this.poll_inputs(cx) {
            Err(error) => return this.fail(error),
            Ok(true) => return Poll::Pending,
            Ok(false) => {}
        }
        let Some(candidate) = this.candidate.take() else {
            this.streams.clear();
            return Poll::Ready(None);
        };
        let is_last = this.streams.iter().all(|input| {
            input.stream.is_none() && input.head.is_none()
                || input
                    .head
                    .as_ref()
                    .is_some_and(|head| head.read_key.as_slice() > candidate.read_key.as_slice())
        });
        Poll::Ready(Some(Ok(RawSeriesChunk {
            is_last,
            ..candidate
        })))
    }
}

impl RawSeriesStream for OrderedRawMerge {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.binding
    }
    fn ordering(&self) -> RawSeriesOrdering {
        RawSeriesOrdering::ReadKeyThenTime
    }
}
