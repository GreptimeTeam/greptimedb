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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::region::RegionRequest;
use backon::{BackoffBuilder, ExponentialBuilder};
use common_error::ext::{BoxedError, ErrorExt, RetryHint};
use common_meta::error::{Error, UnexpectedSnafu};
use common_query::WriteProgress;
use futures::future;
use tokio_util::sync::CancellationToken;

use crate::write_dispatcher::{
    RoutedWrite, WriteAttempt, WriteDispatcher, get_peer, write_region_id,
};

/// A region payload and its original-input indices, in payload row order.
/// Ownership stays local; it is never added to the schema or sent to storage.
pub struct TrackedWrite {
    pub request: RegionRequest,
    pub owners: Vec<usize>,
}

impl WriteDispatcher {
    /// Collects acknowledgements per input across shared flushes and repartition retries.
    pub async fn dispatch_tracked(
        &self,
        requests: Vec<TrackedWrite>,
        inputs: usize,
    ) -> Vec<WriteProgress> {
        let (writes, progress) = self.prepare_tracked(requests, inputs).await;
        self.execute_tracked(writes, progress, None).await
    }

    /// Resolves routes before tracked writes are detached from the caller.
    pub(crate) async fn prepare_tracked(
        &self,
        requests: Vec<TrackedWrite>,
        inputs: usize,
    ) -> (Vec<RoutedWrite>, Vec<WriteProgress>) {
        let mut progress = vec![WriteProgress::default(); inputs];
        if requests
            .iter()
            .any(|request| request.owners.iter().any(|owner| *owner >= inputs))
        {
            let error = UnexpectedSnafu {
                err_msg: "Write owner exceeds original input count",
            }
            .build();
            return (
                Vec::new(),
                vec![WriteProgress::failed(BoxedError::new(error)); inputs],
            );
        }
        let regions = match requests
            .iter()
            .map(|write| write_region_id(&write.request))
            .collect::<common_meta::error::Result<Vec<_>>>()
        {
            Ok(regions) => regions,
            Err(error) => {
                return (
                    Vec::new(),
                    vec![WriteProgress::failed(BoxedError::new(error)); inputs],
                );
            }
        };
        let peers = match self.resolve_peers(&regions).await {
            Ok(peers) => peers,
            Err(error) => {
                return (
                    Vec::new(),
                    vec![WriteProgress::failed(BoxedError::new(error)); inputs],
                );
            }
        };
        let mut pending = Vec::with_capacity(requests.len());
        for (write, region) in requests.into_iter().zip(regions) {
            match get_peer(&peers, region) {
                Ok(peer) => pending.push(RoutedWrite {
                    peer,
                    request: Arc::new(write.request),
                    attribution: super::WriteAttribution::default(),
                    owners: Some(write.owners),
                }),
                Err(error) => record_failure(&mut progress, &write.owners, error),
            }
        }
        (pending, progress)
    }

    /// Drains in-flight writes while cancellation stops subsequent retries.
    pub(crate) async fn execute_tracked(
        &self,
        mut pending: Vec<RoutedWrite>,
        mut progress: Vec<WriteProgress>,
        cancellation: Option<&CancellationToken>,
    ) -> Vec<WriteProgress> {
        let mut backoff = self.retry.as_ref().map(|retry| {
            ExponentialBuilder::default()
                .with_min_delay(retry.options.initial_backoff)
                .with_max_delay(retry.options.max_backoff)
                .with_factor(retry.options.backoff_multiplier)
                .with_max_times(retry.options.max_retries)
                .with_jitter()
                .build()
        });
        let mut failures = Vec::new();
        loop {
            failures.clear();
            let attempts = future::join_all(pending.into_iter().map(|write| async {
                let owners = write.owners.clone().unwrap_or_default();
                let attempt = if let Some(retry) = &self.retry {
                    retry.send(write, None).await
                } else {
                    let datanode = self.node_manager.datanode(&write.peer).await;
                    match datanode.handle(write.request).await {
                        Ok(response) => WriteAttempt::Success(response),
                        Err(error) => WriteAttempt::Failed {
                            error,
                            request: None,
                        },
                    }
                };
                (owners, attempt)
            }))
            .await;
            pending = Vec::new();
            let mut refresh = Vec::new();
            let mut terminal = false;
            for (owners, attempt) in attempts {
                match attempt {
                    WriteAttempt::Success(response) if response.affected_rows == owners.len() => {
                        for run in owners.chunk_by(|a, b| a == b) {
                            progress[run[0]].affected_rows += run.len();
                        }
                    }
                    WriteAttempt::Success(response) => {
                        terminal = true;
                        record_failure(&mut progress, &owners, UnexpectedSnafu {
                            err_msg: format!("Write acknowledged {} rows, expected {}; cannot attribute this acknowledgement", response.affected_rows, owners.len()),
                        }.build());
                    }
                    WriteAttempt::Failed {
                        error,
                        request: Some(write),
                    } => {
                        if error.retry_hint() == RetryHint::RetryAfterRouteRefresh {
                            refresh.push(write);
                        } else {
                            pending.push(write);
                        }
                        failures.push((owners, error));
                    }
                    WriteAttempt::Failed {
                        error,
                        request: None,
                    } => {
                        terminal = true;
                        record_failure(&mut progress, &owners, error);
                    }
                }
            }
            if failures.is_empty() {
                break;
            }
            let delay = (!terminal)
                .then(|| backoff.as_mut().and_then(Iterator::next))
                .flatten();
            let (Some(delay), Some(retry)) = (delay, &self.retry) else {
                break;
            };
            if let Some(cancellation) = cancellation {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                }
            } else {
                tokio::time::sleep(delay).await;
            }
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                break;
            }
            if !refresh.is_empty() {
                let owners = refresh
                    .iter()
                    .flat_map(|write| write.owners.iter().flatten().copied())
                    .collect::<Vec<_>>();
                match retry.reroute_failed_writes(refresh).await {
                    Ok(writes) => pending.extend(writes),
                    Err(error) => record_failure(&mut progress, &owners, error),
                }
            }
            if pending.is_empty() || cancellation.is_some_and(CancellationToken::is_cancelled) {
                break;
            }
        }
        for (owners, error) in failures {
            record_failure(&mut progress, &owners, error);
        }
        progress
    }
}

fn record_failure(progress: &mut [WriteProgress], owners: &[usize], error: Error) {
    let error = Arc::new(BoxedError::new(error));
    for owner in owners.iter().copied().collect::<HashSet<_>>() {
        progress[owner].error.get_or_insert_with(|| error.clone());
    }
}
