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

use std::ops::DerefMut;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_telemetry::{error, warn};
use common_time::util::current_time_millis;
use dashmap::mapref::multiple::RefMulti;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

use crate::failure_detector::PhiAccrualFailureDetector;
use crate::handler::failure_handler::{DatanodeHeartbeat, RegionIdent};
use crate::metasrv::ElectionRef;

pub(crate) enum FailureDetectControl {
    Purge,

    #[cfg(test)]
    Dump(tokio::sync::oneshot::Sender<FailureDetectorContainer>),
}

pub(crate) struct FailureDetectRunner {
    election: Option<ElectionRef>,

    heartbeat_tx: Sender<DatanodeHeartbeat>,
    heartbeat_rx: Option<Receiver<DatanodeHeartbeat>>,

    control_tx: Sender<FailureDetectControl>,
    control_rx: Option<Receiver<FailureDetectControl>>,

    receiver_handle: Option<JoinHandle<()>>,
    runner_handle: Option<JoinHandle<()>>,
}

impl FailureDetectRunner {
    pub(crate) fn new(election: Option<ElectionRef>) -> Self {
        let (heartbeat_tx, heartbeat_rx) = mpsc::channel::<DatanodeHeartbeat>(1024);
        let (control_tx, control_rx) = mpsc::channel::<FailureDetectControl>(1024);
        Self {
            election,
            heartbeat_tx,
            heartbeat_rx: Some(heartbeat_rx),
            control_tx,
            control_rx: Some(control_rx),
            receiver_handle: None,
            runner_handle: None,
        }
    }

    pub(crate) async fn send_heartbeat(&self, heartbeat: DatanodeHeartbeat) {
        if let Err(e) = self.heartbeat_tx.send(heartbeat).await {
            error!("FailureDetectRunner is stop receiving heartbeats: {}", e)
        }
    }

    pub(crate) async fn send_control(&self, control: FailureDetectControl) {
        if let Err(e) = self.control_tx.send(control).await {
            error!("FailureDetectRunner is stop receiving controls: {}", e)
        }
    }

    pub(crate) async fn start(&mut self) {
        let failure_detectors = Arc::new(FailureDetectorContainer(DashMap::new()));
        self.start_with(failure_detectors).await
    }

    async fn start_with(&mut self, failure_detectors: Arc<FailureDetectorContainer>) {
        let Some(mut heartbeat_rx) = self.heartbeat_rx.take() else { return };
        let Some(mut control_rx) = self.control_rx.take() else { return };

        let container = failure_detectors.clone();
        let receiver_handle = common_runtime::spawn_bg(async move {
            loop {
                tokio::select! {
                    Some(control) = control_rx.recv() => {
                        match control {
                            FailureDetectControl::Purge => container.clear(),

                            #[cfg(test)]
                            FailureDetectControl::Dump(tx) => {
                                // Drain any heartbeats that are not handled before dump.
                                while let Ok(heartbeat) = heartbeat_rx.try_recv() {
                                    for ident in heartbeat.region_idents {
                                        let mut detector = container.get_failure_detector(ident);
                                        detector.heartbeat(heartbeat.heartbeat_time);
                                    }
                                }
                                let _ = tx.send(container.dump());
                            }
                        }
                    }
                    Some(heartbeat) = heartbeat_rx.recv() => {
                        for ident in heartbeat.region_idents {
                            let mut detector = container.get_failure_detector(ident);
                            detector.heartbeat(heartbeat.heartbeat_time);
                        }
                    }
                    else => {
                        warn!("Both control and heartbeat senders are closed, quit receiving.");
                        break;
                    }
                }
            }
        });
        self.receiver_handle = Some(receiver_handle);

        let election = self.election.clone();
        let runner_handle = common_runtime::spawn_bg(async move {
            loop {
                let start = Instant::now();

                let is_leader = election.as_ref().map(|x| x.is_leader()).unwrap_or(true);
                if is_leader {
                    for e in failure_detectors.iter() {
                        if e.failure_detector().is_available(current_time_millis()) {
                            // TODO(LFC): TBC
                        }
                    }
                }

                let elapsed = Instant::now().duration_since(start);
                if let Some(sleep) = Duration::from_secs(1).checked_sub(elapsed) {
                    tokio::time::sleep(sleep).await;
                } // else the elapsed time is exceeding one second, we should continue working immediately
            }
        });
        self.runner_handle = Some(runner_handle);
    }

    #[cfg(test)]
    fn abort(&mut self) {
        let Some(handle) = self.receiver_handle.take() else { return };
        handle.abort();

        let Some(handle) = self.runner_handle.take() else { return };
        handle.abort();
    }

    #[cfg(test)]
    pub(crate) async fn dump(&self) -> FailureDetectorContainer {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send_control(FailureDetectControl::Dump(tx)).await;
        rx.await.unwrap()
    }
}

pub(crate) struct FailureDetectorEntry<'a> {
    e: RefMulti<'a, RegionIdent, PhiAccrualFailureDetector>,
}

impl FailureDetectorEntry<'_> {
    fn failure_detector(&self) -> &PhiAccrualFailureDetector {
        self.e.value()
    }
}

pub(crate) struct FailureDetectorContainer(DashMap<RegionIdent, PhiAccrualFailureDetector>);

impl FailureDetectorContainer {
    fn get_failure_detector(
        &self,
        ident: RegionIdent,
    ) -> impl DerefMut<Target = PhiAccrualFailureDetector> + '_ {
        self.0
            .entry(ident)
            .or_insert_with(PhiAccrualFailureDetector::default)
    }

    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = FailureDetectorEntry> + '_> {
        Box::new(self.0.iter().map(move |e| FailureDetectorEntry { e })) as _
    }

    fn clear(&self) {
        self.0.clear()
    }

    #[cfg(test)]
    fn dump(&self) -> FailureDetectorContainer {
        let mut m = DashMap::with_capacity(self.0.len());
        m.extend(self.0.iter().map(|x| (x.key().clone(), x.value().clone())));
        Self(m)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_default_failure_detector_container() {
        let container = FailureDetectorContainer(DashMap::new());
        let ident = RegionIdent {
            catalog: "a".to_string(),
            schema: "b".to_string(),
            table: "c".to_string(),
            region_id: 1,
        };
        let _ = container.get_failure_detector(ident.clone());
        assert!(container.0.contains_key(&ident));

        {
            let mut iter = container.iter();
            assert!(iter.next().is_some());
            assert!(iter.next().is_none());
        }

        container.clear();
        assert!(container.0.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_control() {
        let container = FailureDetectorContainer(DashMap::new());

        let ident = RegionIdent {
            catalog: "a".to_string(),
            schema: "b".to_string(),
            table: "c".to_string(),
            region_id: 1,
        };
        container.get_failure_detector(ident.clone());

        let mut runner = FailureDetectRunner::new(None);
        runner.start_with(Arc::new(container)).await;

        let dump = runner.dump().await;
        assert_eq!(dump.iter().collect::<Vec<_>>().len(), 1);

        runner.send_control(FailureDetectControl::Purge).await;

        let dump = runner.dump().await;
        assert_eq!(dump.iter().collect::<Vec<_>>().len(), 0);

        runner.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_heartbeat() {
        let mut runner = FailureDetectRunner::new(None);
        runner.start().await;

        // Generate 2000 heartbeats start from now. Heartbeat interval is one second, plus some random millis.
        fn generate_heartbeats(node_id: u64, region_ids: Vec<u64>) -> Vec<DatanodeHeartbeat> {
            let mut rng = rand::thread_rng();
            let start = current_time_millis();
            (0..2000)
                .map(|i| DatanodeHeartbeat {
                    cluster_id: 1,
                    node_id,
                    region_idents: region_ids
                        .iter()
                        .map(|&region_id| RegionIdent {
                            catalog: "a".to_string(),
                            schema: "b".to_string(),
                            table: "c".to_string(),
                            region_id,
                        })
                        .collect(),
                    heartbeat_time: start + i * 1000 + rng.gen_range(0..100),
                })
                .collect::<Vec<_>>()
        }

        let heartbeats = generate_heartbeats(100, vec![1, 2, 3]);
        let last_heartbeat_time = heartbeats.last().unwrap().heartbeat_time;
        for heartbeat in heartbeats {
            runner.send_heartbeat(heartbeat).await;
        }

        let dump = runner.dump().await;
        let failure_detectors = dump.iter().collect::<Vec<_>>();
        assert_eq!(failure_detectors.len(), 3);

        failure_detectors.iter().for_each(|e| {
            let fd = e.failure_detector();
            let acceptable_heartbeat_pause_millis = fd.acceptable_heartbeat_pause_millis() as i64;
            let start = last_heartbeat_time;

            // Within the "acceptable_heartbeat_pause_millis" period, phi is zero ...
            for i in 1..=acceptable_heartbeat_pause_millis / 1000 {
                let now = start + i * 1000;
                assert_eq!(fd.phi(now), 0.0);
            }

            // ... then in less than two seconds, phi is above the threshold.
            // The same effect can be seen in the diagrams in Akka's document.
            let now = start + acceptable_heartbeat_pause_millis + 1000;
            assert!(fd.phi(now) < fd.threshold() as _);
            let now = start + acceptable_heartbeat_pause_millis + 2000;
            assert!(fd.phi(now) > fd.threshold() as _);
        });

        runner.abort();
    }
}
