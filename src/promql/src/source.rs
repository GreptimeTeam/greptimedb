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

mod merge;
#[cfg(test)]
mod tests;

pub(crate) use merge::merge_raw_identity_charging;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use datatypes::error::InvalidSeriesSnafu;
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::LabelDecodeSpec;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SeriesSchema;
use futures::Stream;
use mito_codec::labels::PrimaryKeyLabelDecoder;
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

/// Materializes complete metric labels from a raw-series frame stream.
///
/// The labels are decoded from the stream's own label decode description, so a consumer that holds
/// only the frame needs no scan-bound binding; a stream that carries complete labels already, and
/// so declares no description, needs no decoder and is forwarded as supplied. Adjacent-chunk
/// pack/unpack validation remains the caller's responsibility, and nonadjacent key recurrence is
/// governed by the producer contract. Explicit labels are retained as supplied; their completeness
/// cannot be inferred from the schema. No authorization or cross-stream completion is performed
/// here.
pub fn materialize_raw_labels(
    source: SendableRawSeriesStream,
) -> Result<SendableRawSeriesStream, BoxedError> {
    let schema = Arc::clone(source.schema());
    let decode = source.label_decode_spec().cloned();
    let ordering = source.ordering();
    if schema.eval_grid.is_some() {
        return Err(BoxedError::new(
            InvalidSeriesSnafu {
                reason: "raw series schema cannot have an evaluation grid",
            }
            .build(),
        ));
    }
    if schema.fields.is_empty() {
        return Err(BoxedError::new(
            InvalidSeriesSnafu {
                reason: "raw series schema must have fields",
            }
            .build(),
        ));
    }
    let decoder = match &decode {
        Some(description) => {
            Some(PrimaryKeyLabelDecoder::from_decode_spec(description).map_err(BoxedError::new)?)
        }
        None => None,
    };

    Ok(Box::pin(MaterializedRawSeriesStream {
        source: Some(source),
        schema,
        ordering,
        decoder,
        label_source: RawLabelSource::Explicit,
    }))
}

struct MaterializedRawSeriesStream {
    source: Option<SendableRawSeriesStream>,
    schema: Arc<SeriesSchema>,
    ordering: SeriesOrdering,
    decoder: Option<PrimaryKeyLabelDecoder>,
    label_source: RawLabelSource,
}

impl Stream for MaterializedRawSeriesStream {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(source) = this.source.as_mut() else {
            return Poll::Ready(None);
        };
        match source.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.source = None;
                this.decoder = None;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.source = None;
                this.decoder = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(chunk))) => {
                let result = this.materialize(chunk);
                if result.is_err() {
                    this.source = None;
                    this.decoder = None;
                }
                Poll::Ready(Some(result))
            }
        }
    }
}

impl RawSeriesStream for MaterializedRawSeriesStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    /// Returns `None`: materialized labels are already complete, so this stream declares no label
    /// decode description and a consumer needs no decoder.
    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        None
    }

    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}

impl MaterializedRawSeriesStream {
    fn materialize(&mut self, chunk: RawSeriesChunk) -> Result<RawSeriesChunk, BoxedError> {
        let metric = match (&mut self.decoder, chunk.metric) {
            (Some(decoder), None) => Some(
                decoder
                    .decode(&chunk.primary_key)
                    .map_err(BoxedError::new)?,
            ),
            (None, Some(metric)) => Some(metric),
            (Some(_), Some(_)) => {
                return Err(BoxedError::new(
                    InvalidSeriesSnafu {
                        reason: "raw chunk already carries labels but the source describes a key",
                    }
                    .build(),
                ));
            }
            (None, None) => {
                return Err(BoxedError::new(
                    InvalidSeriesSnafu {
                        reason: "raw chunk carries no labels and the source describes no key",
                    }
                    .build(),
                ));
            }
        };
        RawSeriesChunk::try_new(
            &self.schema,
            chunk.primary_key,
            metric,
            chunk.time,
            chunk.fields,
            chunk.is_last,
        )
        .map_err(BoxedError::new)
    }
}
