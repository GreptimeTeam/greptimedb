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
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::LabelDecodeSpec;
use datatypes::series::histogram::HistogramView;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SampleRef;
use datatypes::series::samples::{SeriesFields, SeriesSchema};
use futures::Stream;
use mito_codec::labels::PrimaryKeyLabelDecoder;
use store_api::region_engine::{
    NO_BOUND_LABEL_SOURCE, RawLabelSource, RawSeriesStream, SendableRawSeriesStream,
};

/// Merge compatible, explicitly supplied raw streams by complete read key and time.
///
/// Callers provide authorized bindings and own snapshot/storage policy. This does not select an
/// MVCC view, resolve overlapping physical copies, or roll back emitted prefixes after errors.
/// `is_last` is global only over these inputs; there is no fallback, replay, or input sorting.
///
/// The merge contract is the label layout, not a binding: every input must declare the same one —
/// from its frame if it arrived as a frame, otherwise from its own binding — and the merged stream
/// advertises that one declaration. A consumer therefore decodes the merged stream from the frame;
/// the identity it reports is none, see [`RawSeriesStream::label_source`].
pub fn merge_ordered_raw_series(
    inputs: Vec<SendableRawSeriesStream>,
) -> Result<SendableRawSeriesStream, BoxedError> {
    let mut inputs = inputs.into_iter();
    let first = inputs
        .next()
        .ok_or_else(|| invalid("raw merge requires at least one input"))?;
    let schema = Arc::clone(first.schema());
    let decode = merged_decode_spec(first.as_ref().get_ref())?;
    let mut streams = vec![Input::new(first)];
    for stream in inputs {
        streams.push(Input::new(stream));
    }
    for input in &streams {
        let stream = input
            .stream
            .as_ref()
            .ok_or_else(|| invalid("raw merge input disappeared"))?;
        validate_input(stream.as_ref().get_ref(), &schema)?;
        validate_decode(stream.as_ref().get_ref(), &decode)?;
    }
    Ok(Box::pin(OrderedRawMerge {
        streams,
        schema,
        decode,
        candidate: None,
    }))
}

fn invalid(reason: &'static str) -> BoxedError {
    BoxedError::new(InvalidSeriesSnafu { reason }.build())
}

/// Returns the label layout a stream declares, with no binding required to state it.
///
/// Every key-encoded stream declares its own description — a producer derives it from the binding it
/// read under and states it before the first poll, and a stream reassembled from a frame carries the
/// description its producer wrote — so the answer is exactly what decoding that stream's chunks
/// needs. A merge can therefore compare inputs that were framed and inputs that were not in the same
/// terms. `None` means the stream carries complete labels and declares no key layout at all.
fn declared_layout(stream: &dyn RawSeriesStream) -> Option<LabelDecodeSpec> {
    stream.label_decode_spec().cloned()
}

/// Returns the one label decode description the merged stream advertises for all inputs.
///
/// The first input's declaration is forwarded unchanged, so a merge advertises exactly what its
/// inputs advertise, whether they are frame-decoded or straight from a producer. The declaration is
/// validated as a decode layout before it is advertised, so a downstream consumer holding only the
/// merged frame schema can decode it.
fn merged_decode_spec(stream: &dyn RawSeriesStream) -> Result<LabelDecodeSpec, BoxedError> {
    let description = declared_layout(stream).ok_or_else(|| {
        invalid("raw merge requires inputs that describe a primary-key label layout")
    })?;
    PrimaryKeyLabelDecoder::from_decode_spec(&description)
        .map_err(BoxedError::new)?;
    Ok(description)
}

/// Requires every input to declare the advertised label layout.
///
/// The merged stream advertises one description, so an input declaring another would be decoded as
/// if it had the advertised encoding. Every input is compared through [`declared_layout`], which
/// reads the input's own declaration, so no input needs a binding to be present.
fn validate_decode(
    stream: &dyn RawSeriesStream,
    decode: &LabelDecodeSpec,
) -> Result<(), BoxedError> {
    if declared_layout(stream).as_ref() != Some(decode) {
        return Err(invalid(
            "raw merge inputs describe different label decode layouts",
        ));
    }
    Ok(())
}

fn validate_input(stream: &dyn RawSeriesStream, schema: &SeriesSchema) -> Result<(), BoxedError> {
    let other = stream.schema();
    if stream.ordering() != SeriesOrdering::KeyThenTime
        || other.eval_grid.is_some()
        || other.fields.is_empty()
        || other.fields.len() != schema.fields.len()
        || other
            .fields
            .iter()
            .zip(schema.fields.iter())
            .any(|(a, b)| a.name != b.name)
    {
        return Err(invalid(
            "raw merge requires equal raw schemas and ReadKeyThenTime ordering",
        ));
    }
    Ok(())
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
            chunk.primary_key.clone(),
            None,
            chunk.time.clone(),
            chunk.fields.clone(),
            chunk.is_last,
        )
        .map_err(BoxedError::new)?;
        let times = chunk.time.as_buffer();
        if let Some(previous_key) = &self.previous_key {
            match previous_key.as_slice().cmp(chunk.primary_key.as_slice()) {
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
        self.previous_key = Some(chunk.primary_key.as_slice().to_vec());
        self.previous_time = times.last().copied();
        self.ended_series = chunk.is_last;
        Ok(())
    }

    /// Returns whether this input can no longer contribute chunks for `key`.
    ///
    /// A held head beyond `key` proves completion. Without a head, only an ended input or `key`'s
    /// own final chunk proves completion: an earlier completed key never rules out unknown later
    /// or intervening keys.
    fn finished_at(&self, key: &[u8]) -> bool {
        match self.head.as_ref() {
            Some(head) => head.primary_key.as_slice() > key,
            None => {
                self.stream.is_none()
                    || self.previous_key.as_deref().is_some_and(|previous| {
                        previous > key || (previous == key && self.ended_series)
                    })
            }
        }
    }
}

struct OrderedRawMerge {
    streams: Vec<Input>,
    schema: Arc<SeriesSchema>,
    /// The description the merged stream advertises for all inputs; see [`merged_decode_spec`].
    decode: LabelDecodeSpec,
    candidate: Option<RawSeriesChunk>,
}

impl OrderedRawMerge {
    fn fail(&mut self, error: BoxedError) -> Poll<Option<Result<RawSeriesChunk, BoxedError>>> {
        self.streams.clear();
        self.candidate = None;
        Poll::Ready(Some(Err(error)))
    }

    /// Polls every input that has no head once, returning upstream errors before progress.
    fn poll_inputs(&mut self, cx: &mut Context<'_>) -> Result<(), BoxedError> {
        for input in &mut self.streams {
            if input.stream.is_none() || input.head.is_some() {
                continue;
            }
            match input
                .stream
                .as_mut()
                .map(|stream| stream.as_mut().poll_next(cx))
            {
                Some(Poll::Pending) => {}
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
        Ok(())
    }

    /// Returns the index of the smallest available head, by read key then first timestamp.
    fn smallest_head_index(&self) -> Option<usize> {
        self.streams
            .iter()
            .enumerate()
            .filter_map(|(index, input)| input.head.as_ref().map(|head| (index, head)))
            .min_by(|(_, a), (_, b)| {
                a.primary_key
                    .as_slice()
                    .cmp(b.primary_key.as_slice())
                    .then_with(|| a.time.timestamp(0).cmp(&b.time.timestamp(0)))
            })
            .map(|(index, _)| index)
    }

    /// Returns whether every input accounts for `key`: it holds a head, or completion is proven.
    ///
    /// A headless input that cannot prove completion may still add chunks to `key`, so the merge
    /// must wait instead of deciding `key`.
    fn all_inputs_ready_at(&self, key: &[u8]) -> bool {
        self.streams
            .iter()
            .all(|input| input.head.is_some() || input.finished_at(key))
    }

    fn take_next(&mut self) -> Result<Option<RawSeriesChunk>, BoxedError> {
        let Some(index) = self.smallest_head_index() else {
            return Ok(None);
        };
        let chunk = self.streams[index]
            .head
            .take()
            .ok_or_else(|| invalid("raw merge lost selected head"))?;
        let key = chunk.primary_key.as_slice();
        let start = chunk
            .time
            .timestamp(0)
            .ok_or_else(|| invalid("raw merge chunk has no timestamp"))?;
        let next_same_key = self
            .streams
            .iter()
            .enumerate()
            .filter(|(other_index, input)| {
                *other_index != index
                    && input
                        .head
                        .as_ref()
                        .is_some_and(|head| head.primary_key.as_slice() == key)
            })
            .filter_map(|(_, input)| input.head.as_ref()?.time.timestamp(0))
            .min();
        if next_same_key == Some(start) {
            for other_index in 0..self.streams.len() {
                if other_index == index {
                    continue;
                }
                let Some(head) = self.streams[other_index].head.as_ref() else {
                    continue;
                };
                if head.primary_key.as_slice() == key && head.time.timestamp(0) == Some(start) {
                    if !chunks_equal_at(&chunk, 0, head, 0, &self.schema)? {
                        return Err(invalid(
                            "raw merge inputs contain conflicting samples for a complete read key and timestamp",
                        ));
                    }
                    if head.time.len() == 1 {
                        self.streams[other_index].head = None;
                    } else {
                        self.streams[other_index].head =
                            Some(slice_chunk(head, 1, head.time.len() - 1)?);
                    }
                }
            }
            if chunk.time.len() > 1 {
                self.streams[index].head = Some(slice_chunk(&chunk, 1, chunk.time.len() - 1)?);
            }
            let fields = SeriesFields::try_from_vec(
                chunk
                    .fields
                    .iter()
                    .map(|samples| samples.slice(0, 1).map_err(BoxedError::new))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(BoxedError::new)?;
            return Ok(Some(RawSeriesChunk {
                primary_key: chunk.primary_key,
                metric: None,
                time: chunk.time.slice(0, 1).map_err(BoxedError::new)?,
                fields,
                is_last: false,
            }));
        }
        let end = next_same_key.map_or(chunk.time.len(), |next| {
            chunk
                .time
                .as_buffer()
                .partition_point(|timestamp| *timestamp < next)
        });
        if end == chunk.time.len() {
            return Ok(Some(RawSeriesChunk {
                primary_key: chunk.primary_key,
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
                primary_key: chunk.primary_key.clone(),
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
            primary_key: chunk.primary_key,
            metric: None,
            time,
            fields,
            is_last: false,
        }))
    }
}

fn slice_chunk(
    chunk: &RawSeriesChunk,
    offset: usize,
    len: usize,
) -> Result<RawSeriesChunk, BoxedError> {
    Ok(RawSeriesChunk {
        primary_key: chunk.primary_key.clone(),
        metric: None,
        time: chunk.time.slice(offset, len).map_err(BoxedError::new)?,
        fields: SeriesFields::try_from_vec(
            chunk
                .fields
                .iter()
                .map(|samples| samples.slice(offset, len).map_err(BoxedError::new))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(BoxedError::new)?,
        is_last: chunk.is_last,
    })
}

fn chunks_equal_at(
    a: &RawSeriesChunk,
    ai: usize,
    b: &RawSeriesChunk,
    bi: usize,
    schema: &SeriesSchema,
) -> Result<bool, BoxedError> {
    if a.primary_key != b.primary_key
        || a.time.as_buffer()[ai] != b.time.as_buffer()[bi]
        || a.fields.len() != b.fields.len()
    {
        return Ok(false);
    }
    for field in 0..schema.fields.len() {
        if !samples_equal(
            a.fields
                .get(field)
                .ok_or_else(|| invalid("missing raw field"))?
                .sample(ai),
            b.fields
                .get(field)
                .ok_or_else(|| invalid("missing raw field"))?
                .sample(bi),
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn samples_equal(a: SampleRef<'_>, b: SampleRef<'_>) -> bool {
    match (a, b) {
        (SampleRef::Missing, SampleRef::Missing) => true,
        (SampleRef::Float(a), SampleRef::Float(b)) => a.to_bits() == b.to_bits(),
        (SampleRef::Histogram(a), SampleRef::Histogram(b)) => histograms_equal(a, b),
        _ => false,
    }
}

fn histograms_equal(a: HistogramView<'_>, b: HistogramView<'_>) -> bool {
    a.schema == b.schema
        && a.zero_threshold.to_bits() == b.zero_threshold.to_bits()
        && a.sum.to_bits() == b.sum.to_bits()
        && a.count.to_bits() == b.count.to_bits()
        && a.zero_count.to_bits() == b.zero_count.to_bits()
        && a.reset_hint == b.reset_hint
        && a.start_timestamp == b.start_timestamp
        && float_slices_equal(a.custom_values, b.custom_values)
        && a.positive.span_offsets == b.positive.span_offsets
        && a.positive.span_lengths == b.positive.span_lengths
        && float_slices_equal(a.positive.buckets, b.positive.buckets)
        && a.negative.span_offsets == b.negative.span_offsets
        && a.negative.span_lengths == b.negative.span_lengths
        && float_slices_equal(a.negative.buckets, b.negative.buckets)
}

fn float_slices_equal(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.to_bits() == b.to_bits())
}

impl Stream for OrderedRawMerge {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.streams.is_empty() {
            return Poll::Ready(None);
        }
        if this.candidate.is_none() {
            if let Err(error) = this.poll_inputs(cx) {
                return this.fail(error);
            }
            let Some(head) = this
                .smallest_head_index()
                .and_then(|index| this.streams[index].head.as_ref())
            else {
                // Only a fully drained input set is EOF: a live input may still reveal future keys
                // or a late error, so its state stays owned instead of being cleared.
                if this.streams.iter().all(|input| input.stream.is_none()) {
                    this.streams.clear();
                    return Poll::Ready(None);
                }
                return Poll::Pending;
            };
            // A headless input that cannot prove completion at the smallest available key may
            // still add chunks to it, so the merge waits before consuming that head.
            if !this.all_inputs_ready_at(head.primary_key.as_slice()) {
                return Poll::Pending;
            }
            match this.take_next() {
                Ok(candidate) => this.candidate = candidate,
                Err(error) => return this.fail(error),
            }
        }
        if let Err(error) = this.poll_inputs(cx) {
            return this.fail(error);
        }
        let Some(candidate) = this.candidate.take() else {
            this.streams.clear();
            return Poll::Ready(None);
        };
        if !this.all_inputs_ready_at(candidate.primary_key.as_slice()) {
            // The candidate's key may still be extended or interleaved by a headless input: an
            // already-decided prefix is held back instead of being emitted as if it were final.
            this.candidate = Some(candidate);
            return Poll::Pending;
        }
        let is_last = this
            .streams
            .iter()
            .all(|input| input.finished_at(candidate.primary_key.as_slice()));
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
    /// Returns [`NO_BOUND_LABEL_SOURCE`] unconditionally.
    ///
    /// A merge is not a region scan, so it has no scan-bound identity to report, and it does not
    /// invent one from its inputs. Its decode contract is [`Self::label_decode_spec`]; identity is
    /// not something a merged stream can report.
    fn label_source(&self) -> &RawLabelSource {
        &NO_BOUND_LABEL_SOURCE
    }

    /// Returns the description this merge advertises for every input, so a consumer of the merged
    /// stream decodes labels from the frame instead of from the binding.
    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        Some(&self.decode)
    }
    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::KeyThenTime
    }
}
