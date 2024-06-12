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

use common_telemetry::warn;
use rskafka::client::producer::aggregator::{self, Aggregator, StatusDeaggregator, TryPush};
use rskafka::record::Record;
use snafu::ResultExt;
use tokio::sync::watch;

use crate::error::{self, Error, Result};

/// The state of Producing batch records.
#[derive(Clone)]
pub(crate) enum BatchFlushState<A: Aggregator> {
    Init,
    Result(Arc<AggregatedStatus<A>>),
    Err(Arc<Error>),
}

/// The result of a batch Kafka write, and the deaggregator implementation to
/// demux the batch of responses to individual results produce() call.
#[derive(Debug)]
pub(crate) struct AggregatedStatus<A>
where
    A: Aggregator,
{
    pub(crate) aggregated_status: Vec<i64>,
    pub(crate) status_deagg: <A as Aggregator>::StatusDeaggregator,
}

/// A result handle obtained by pushing an input to the aggregator.
///
/// Holders of this handle can use it to obtain the produce result once the
/// aggregated batch is wrote to Kafka.
pub(crate) struct ResultHandle<A>
where
    A: Aggregator,
{
    receiver: watch::Receiver<BatchFlushState<A>>,
    tag: A::Tag,
}

impl<A: aggregator::Aggregator> ResultHandle<A> {
    /// Waits for [`BatchFlushState`] changed(exclude [`BatchFlushState::Init`]) and returns the  [`AggregatedStatus`].
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. If you use it as the event in a
    /// [`tokio::select!`] statement and some other branch
    /// completes first, then it is guaranteed that no values have been marked
    /// seen by this call to `changed`.
    pub(crate) async fn wait(&mut self) -> Result<Arc<AggregatedStatus<A>>> {
        self.receiver
            .changed()
            .await
            .context(error::StateRecvSnafu)?;

        let status = &*self.receiver.borrow();
        match status {
            BatchFlushState::Init => unreachable!(),
            BatchFlushState::Result(status) => Ok(status.clone()),
            BatchFlushState::Err(err) => Err(err.clone()).context(error::FlushErrSnafu),
        }
    }

    /// Return the demuxed result of the produce() call.
    pub(crate) fn result(
        self,
        status: Arc<AggregatedStatus<A>>,
    ) -> Result<<A as aggregator::AggregatorStatus>::Status> {
        status
            .status_deagg
            .deaggregate(&status.aggregated_status, self.tag)
            .context(error::DeaggregateStatusSnafu)
    }
}

/// A [`BatchBuilder`] uses an [`Aggregator`] to construct maximally large batch
/// of writes, and returning a [`ResultHandle`] for callers to demux the result.
pub(crate) struct BatchBuilder<A>
where
    A: aggregator::Aggregator,
{
    aggregator: A,
    /// Sends the flush `results` to [ResultHandle]s.
    sender: watch::Sender<BatchFlushState<A>>,
    /// The receiver of `results`.
    receiver: watch::Receiver<BatchFlushState<A>>,
}

pub(crate) struct FlushRequest<A: Aggregator> {
    pub(crate) batch: Vec<Record>,
    pub(crate) status_deagg: A::StatusDeaggregator,
    /// Sends the flush `results` to [ResultHandle]s.
    pub(crate) sender: watch::Sender<BatchFlushState<A>>,
}

impl<A: aggregator::Aggregator> BatchBuilder<A> {
    pub(crate) fn new(aggregator: A) -> Self {
        let (sender, receiver) = watch::channel(BatchFlushState::Init);

        Self {
            aggregator,
            sender,
            receiver,
        }
    }

    pub(crate) fn try_push(
        &mut self,
        data: A::Input,
    ) -> Result<TryPush<A::Input, ResultHandle<A>>> {
        match self
            .aggregator
            .try_push(data)
            .context(error::AggregateInputSnafu)?
        {
            TryPush::NoCapacity(data) => Ok(TryPush::NoCapacity(data)),
            TryPush::Aggregated(tag) => Ok(TryPush::Aggregated(ResultHandle {
                receiver: self.receiver.clone(),
                tag,
            })),
        }
    }

    pub(crate) fn next_batch(self) -> (Self, Result<Option<FlushRequest<A>>>) {
        let BatchBuilder {
            mut aggregator,
            sender,
            ..
        } = self;
        let (batch, status_deagg) = match aggregator.flush().context(error::FlushAggregatorSnafu) {
            Ok(v) => v,
            Err(e) => return (Self::new(aggregator), Err(e)),
        };

        if batch.is_empty() {
            // The aggregator might have produced no records,
            // but the `produce()` callers are still waiting for their responses.
            //
            // Sends an empty result set to all waiters.
            if let Err(err) = sender.send(BatchFlushState::Result(Arc::new(AggregatedStatus {
                aggregated_status: vec![],
                status_deagg,
            }))) {
                warn!(err; "Failed to send batch write state");
            }

            return (Self::new(aggregator), Ok(None));
        }

        (
            Self::new(aggregator),
            Ok(Some(FlushRequest {
                batch,
                status_deagg,
                sender,
            })),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use rskafka::client::producer::aggregator::RecordAggregator;
    use rskafka::record::Record;

    use crate::kafka::producer::batch::{
        AggregatedStatus, BatchBuilder, BatchFlushState, FlushRequest,
    };

    fn record() -> Record {
        Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: Utc.timestamp_millis_opt(320).unwrap(),
        }
    }

    #[tokio::test]
    async fn test_result_handle() {
        let record = record();
        let aggregator = RecordAggregator::new(record.approximate_size() * 2);
        let mut batch = BatchBuilder::new(aggregator);

        let mut handle = batch.try_push(record).unwrap().unwrap_tag();

        let (_, flush_req) = batch.next_batch();
        tokio::spawn(async move {
            let FlushRequest {
                batch: _batch,
                status_deagg,
                sender,
            } = flush_req.unwrap().unwrap();

            sender.send(BatchFlushState::Result(Arc::new(AggregatedStatus {
                aggregated_status: vec![1, 2],
                status_deagg,
            })))
        });

        let status = handle.wait().await.unwrap();
        assert_eq!(status.aggregated_status, vec![1, 2]);
    }
}
