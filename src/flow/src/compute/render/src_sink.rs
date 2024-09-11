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

//! Source and Sink for the dataflow

use std::collections::{BTreeMap, VecDeque};

use common_telemetry::{debug, trace};
use hydroflow::scheduled::graph_ext::GraphExt;
use itertools::Itertools;
use snafu::OptionExt;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::{broadcast, mpsc};

use crate::compute::render::Context;
use crate::compute::types::{Arranged, Collection, CollectionBundle, Toff};
use crate::error::{Error, PlanSnafu};
use crate::expr::error::InternalSnafu;
use crate::expr::{Batch, EvalError};
use crate::repr::{DiffRow, Row, BROADCAST_CAP};

#[allow(clippy::mutable_key_type)]
impl<'referred, 'df> Context<'referred, 'df> {
    /// simply send the batch to downstream, without fancy features like buffering
    pub fn render_source_batch(
        &mut self,
        mut src_recv: broadcast::Receiver<Batch>,
    ) -> Result<CollectionBundle<Batch>, Error> {
        debug!("Rendering Source Batch");
        let (send_port, recv_port) = self.df.make_edge::<_, Toff<Batch>>("source_batch");

        let schd = self.compute_state.get_scheduler();
        let inner_schd = schd.clone();
        let now = self.compute_state.current_time_ref();
        let err_collector = self.err_collector.clone();

        let sub = self
            .df
            .add_subgraph_source("source_batch", send_port, move |_ctx, send| {
                let mut total_batches = vec![];
                let mut total_row_count = 0;
                loop {
                    match src_recv.try_recv() {
                        Ok(batch) => {
                            total_row_count += batch.row_count();
                            total_batches.push(batch);
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Lagged(lag_offset)) => {
                            // use `err_collector` instead of `error!` to locate which operator caused the error
                            err_collector.run(|| -> Result<(), EvalError> {
                                InternalSnafu {
                                    reason: format!("Flow missing {} rows behind", lag_offset),
                                }
                                .fail()
                            });
                            break;
                        }
                        Err(TryRecvError::Closed) => {
                            err_collector.run(|| -> Result<(), EvalError> {
                                InternalSnafu {
                                    reason: "Source Batch Channel is closed".to_string(),
                                }
                                .fail()
                            });
                            break;
                        }
                    }
                }

                trace!(
                    "Send {} rows in {} batches",
                    total_row_count,
                    total_batches.len()
                );
                send.give(total_batches);

                let now = *now.borrow();
                // always schedule source to run at now so we can
                // repeatedly run source if needed
                inner_schd.schedule_at(now);
            });
        schd.set_cur_subgraph(sub);
        let bundle = CollectionBundle::from_collection(Collection::<Batch>::from_port(recv_port));
        Ok(bundle)
    }

    /// Render a source which comes from brocast channel into the dataflow
    /// will immediately send updates not greater than `now` and buffer the rest in arrangement
    pub fn render_source(
        &mut self,
        mut src_recv: broadcast::Receiver<DiffRow>,
    ) -> Result<CollectionBundle, Error> {
        debug!("Rendering Source");
        let (send_port, recv_port) = self.df.make_edge::<_, Toff>("source");
        let arrange_handler = self.compute_state.new_arrange(None);
        let arrange_handler_inner =
            arrange_handler
                .clone_future_only()
                .with_context(|| PlanSnafu {
                    reason: "No write is expected at this point",
                })?;

        let schd = self.compute_state.get_scheduler();
        let inner_schd = schd.clone();
        let now = self.compute_state.current_time_ref();
        let err_collector = self.err_collector.clone();

        let sub = self
            .df
            .add_subgraph_source("source", send_port, move |_ctx, send| {
                let now = *now.borrow();
                // write lock to prevent unexpected mutation
                let mut arranged = arrange_handler_inner.write();
                let arr = arranged.get_updates_in_range(..=now);
                err_collector.run(|| arranged.compact_to(now));

                let prev_avail = arr.into_iter().map(|((k, _), t, d)| (k, t, d));
                let mut to_send = Vec::new();
                let mut to_arrange = Vec::new();
                // TODO(discord9): handling tokio broadcast error
                loop {
                    match src_recv.try_recv() {
                        Ok((r, t, d)) => {
                            if t <= now {
                                to_send.push((r, t, d));
                            } else {
                                to_arrange.push(((r, Row::empty()), t, d));
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Lagged(lag_offset)) => {
                            common_telemetry::error!("Flow missing {} rows behind", lag_offset);
                            break;
                        }
                        Err(err) => {
                            err_collector.run(|| -> Result<(), EvalError> {
                                InternalSnafu {
                                    reason: format!(
                                        "Error receiving from broadcast channel: {}",
                                        err
                                    ),
                                }
                                .fail()
                            });
                        }
                    }
                }
                let all = prev_avail.chain(to_send).collect_vec();
                if !to_arrange.is_empty() {
                    debug!("Source Operator buffered {} rows", to_arrange.len());
                }
                err_collector.run(|| arranged.apply_updates(now, to_arrange));
                send.give(all);
                // always schedule source to run at now so we can repeatedly run source if needed
                inner_schd.schedule_at(now);
            });
        schd.set_cur_subgraph(sub);
        let arranged = Arranged::new(arrange_handler);
        arranged.writer.borrow_mut().replace(sub);
        let arranged = BTreeMap::from([(vec![], arranged)]);
        Ok(CollectionBundle {
            collection: Collection::from_port(recv_port),
            arranged,
        })
    }

    pub fn render_unbounded_sink_batch(
        &mut self,
        bundle: CollectionBundle<Batch>,
        sender: mpsc::UnboundedSender<Batch>,
    ) {
        let CollectionBundle {
            collection,
            arranged: _,
        } = bundle;

        let _sink = self.df.add_subgraph_sink(
            "UnboundedSinkBatch",
            collection.into_inner(),
            move |_ctx, recv| {
                let data = recv.take_inner();
                let mut row_count = 0;
                let mut batch_count = 0;
                for batch in data.into_iter().flat_map(|i| i.into_iter()) {
                    row_count += batch.row_count();
                    batch_count += 1;
                    // if the sender is closed unexpectedly, stop sending
                    if sender.is_closed() || sender.send(batch).is_err() {
                        common_telemetry::error!("UnboundedSinkBatch is closed");
                        break;
                    }
                }
                trace!("sink send {} rows in {} batches", row_count, batch_count);
            },
        );
    }

    pub fn render_unbounded_sink(
        &mut self,
        bundle: CollectionBundle,
        sender: mpsc::UnboundedSender<DiffRow>,
    ) {
        let CollectionBundle {
            collection,
            arranged: _,
        } = bundle;

        let _sink = self.df.add_subgraph_sink(
            "UnboundedSink",
            collection.into_inner(),
            move |_ctx, recv| {
                let data = recv.take_inner();
                debug!(
                    "render_unbounded_sink: send {} rows",
                    data.iter().map(|i| i.len()).sum::<usize>()
                );
                for row in data.into_iter().flat_map(|i| i.into_iter()) {
                    // if the sender is closed, stop sending
                    if sender.is_closed() {
                        common_telemetry::error!("UnboundedSink is closed");
                        break;
                    }
                    // TODO(discord9): handling tokio error
                    let _ = sender.send(row);
                }
            },
        );
    }

    /// Render a sink which send updates to broadcast channel, have internal buffer in case broadcast channel is full
    pub fn render_sink(&mut self, bundle: CollectionBundle, sender: broadcast::Sender<DiffRow>) {
        let CollectionBundle {
            collection,
            arranged: _,
        } = bundle;
        let mut buf = VecDeque::with_capacity(1000);

        let schd = self.compute_state.get_scheduler();
        let inner_schd = schd.clone();
        let now = self.compute_state.current_time_ref();

        let sink = self
            .df
            .add_subgraph_sink("Sink", collection.into_inner(), move |_ctx, recv| {
                let data = recv.take_inner();
                buf.extend(data.into_iter().flat_map(|i| i.into_iter()));
                if sender.len() >= BROADCAST_CAP {
                    return;
                } else {
                    while let Some(row) = buf.pop_front() {
                        // if the sender is full, stop sending
                        if sender.len() >= BROADCAST_CAP {
                            break;
                        }
                        // TODO(discord9): handling tokio broadcast error
                        let _ = sender.send(row);
                    }
                }

                // if buffer is not empty, schedule the next run at next tick
                // so the buffer can be drained as soon as possible
                if !buf.is_empty() {
                    inner_schd.schedule_at(*now.borrow() + 1);
                }
            });

        schd.set_cur_subgraph(sink);
    }
}
