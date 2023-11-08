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

use common_meta::RegionIdent;
use common_telemetry::{error, info, warn};
use common_time::util::current_time_millis;
use dashmap::mapref::multiple::RefMulti;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

use crate::failure_detector::{PhiAccrualFailureDetector, PhiAccrualFailureDetectorOptions};
use crate::handler::failure_handler::DatanodeHeartbeat;
use crate::metasrv::ElectionRef;
use crate::procedure::region_failover::RegionFailoverManager;

pub(crate) enum FailureDetectControl {
    Purge,

    #[cfg(test)]
    Dump(tokio::sync::oneshot::Sender<FailureDetectorContainer>),
}

pub(crate) struct FailureDetectRunner {
    election: Option<ElectionRef>,
    region_failover_manager: Arc<RegionFailoverManager>,
    failure_detector_options: PhiAccrualFailureDetectorOptions,

    heartbeat_tx: Sender<DatanodeHeartbeat>,
    heartbeat_rx: Option<Receiver<DatanodeHeartbeat>>,

    control_tx: Sender<FailureDetectControl>,
    control_rx: Option<Receiver<FailureDetectControl>>,

    receiver_handle: Option<JoinHandle<()>>,
    runner_handle: Option<JoinHandle<()>>,
}

impl FailureDetectRunner {
    pub(super) fn new(
        election: Option<ElectionRef>,
        region_failover_manager: Arc<RegionFailoverManager>,
        failure_detector_options: PhiAccrualFailureDetectorOptions,
    ) -> Self {
        let (heartbeat_tx, heartbeat_rx) = mpsc::channel::<DatanodeHeartbeat>(1024);
        let (control_tx, control_rx) = mpsc::channel::<FailureDetectControl>(1024);
        Self {
            election,
            region_failover_manager,
            failure_detector_options,
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
        let failure_detectors = Arc::new(FailureDetectorContainer {
            detectors: DashMap::new(),
            options: self.failure_detector_options.clone(),
        });
        self.start_with(failure_detectors).await
    }

    async fn start_with(&mut self, failure_detectors: Arc<FailureDetectorContainer>) {
        let Some(mut heartbeat_rx) = self.heartbeat_rx.take() else {
            return;
        };
        let Some(mut control_rx) = self.control_rx.take() else {
            return;
        };

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
        let region_failover_manager = self.region_failover_manager.clone();
        let runner_handle = common_runtime::spawn_bg(async move {
            loop {
                let start = Instant::now();

                let is_leader = election.as_ref().map(|x| x.is_leader()).unwrap_or(true);
                if is_leader {
                    let failed_regions = failure_detectors
                        .iter()
                        .filter_map(|e| {
                            // Intentionally not place `current_time_millis()` out of the iteration.
                            // The failure detection determination should be happened "just in time",
                            // i.e., failed or not has to be compared with the most recent "now".
                            // Besides, it might reduce the false positive of failure detection,
                            // because during the iteration, heartbeats are coming in as usual,
                            // and the `phi`s are still updating.
                            if !e.failure_detector().is_available(current_time_millis()) {
                                Some(e.region_ident().clone())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<RegionIdent>>();

                    for r in failed_regions {
                        if let Err(e) = region_failover_manager.do_region_failover(&r).await {
                            error!(e; "Failed to do region failover for {r}");
                        } else {
                            // Now that we know the region is starting to do failover, remove it
                            // from the failure detectors, avoiding the failover procedure to be
                            // triggered again.
                            // If the region is back alive (the failover procedure runs successfully),
                            // it will be added back to the failure detectors again.
                            failure_detectors.remove(&r);
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
    pub(crate) async fn dump(&self) -> FailureDetectorContainer {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send_control(FailureDetectControl::Dump(tx)).await;
        rx.await.unwrap()
    }
}

impl Drop for FailureDetectRunner {
    fn drop(&mut self) {
        if let Some(handle) = self.receiver_handle.take() {
            handle.abort();
            info!("Heartbeat receiver in FailureDetectRunner is stopped.");
        }

        if let Some(handle) = self.runner_handle.take() {
            handle.abort();
            info!("Failure detector in FailureDetectRunner is stopped.");
        }
    }
}

pub(crate) struct FailureDetectorEntry<'a> {
    e: RefMulti<'a, RegionIdent, PhiAccrualFailureDetector>,
}

impl FailureDetectorEntry<'_> {
    fn region_ident(&self) -> &RegionIdent {
        self.e.key()
    }

    fn failure_detector(&self) -> &PhiAccrualFailureDetector {
        self.e.value()
    }
}

pub(crate) struct FailureDetectorContainer {
    options: PhiAccrualFailureDetectorOptions,
    detectors: DashMap<RegionIdent, PhiAccrualFailureDetector>,
}

impl FailureDetectorContainer {
    fn get_failure_detector(
        &self,
        ident: RegionIdent,
    ) -> impl DerefMut<Target = PhiAccrualFailureDetector> + '_ {
        self.detectors
            .entry(ident)
            .or_insert_with(|| PhiAccrualFailureDetector::from_options(self.options.clone()))
    }

    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = FailureDetectorEntry> + '_> {
        Box::new(
            self.detectors
                .iter()
                .map(move |e| FailureDetectorEntry { e }),
        ) as _
    }

    fn remove(&self, ident: &RegionIdent) {
        let _ = self.detectors.remove(ident);
    }

    fn clear(&self) {
        self.detectors.clear()
    }

    #[cfg(test)]
    fn dump(&self) -> FailureDetectorContainer {
        let mut m = DashMap::with_capacity(self.detectors.len());
        m.extend(
            self.detectors
                .iter()
                .map(|x| (x.key().clone(), x.value().clone())),
        );
        Self {
            detectors: m,
            options: self.options.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;
    use crate::test_util::create_region_failover_manager;

    #[test]
    fn test_default_failure_detector_container() {
        let container = FailureDetectorContainer {
            detectors: DashMap::new(),
            options: PhiAccrualFailureDetectorOptions::default(),
        };
        let ident = RegionIdent {
            table_id: 1,
            cluster_id: 3,
            datanode_id: 2,
            region_number: 1,
            engine: "mito2".to_string(),
        };
        let _ = container.get_failure_detector(ident.clone());
        assert!(container.detectors.contains_key(&ident));

        {
            let mut iter = container.iter();
            let _ = iter.next().unwrap();
            assert!(iter.next().is_none());
        }

        container.clear();
        assert!(container.detectors.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_control() {
        let container = FailureDetectorContainer {
            detectors: DashMap::new(),
            options: PhiAccrualFailureDetectorOptions::default(),
        };

        let ident = RegionIdent {
            table_id: 1,
            cluster_id: 3,
            datanode_id: 2,
            region_number: 1,
            engine: "mito2".to_string(),
        };
        let _ = container.get_failure_detector(ident.clone());

        let region_failover_manager = create_region_failover_manager();
        let failure_detector_options = PhiAccrualFailureDetectorOptions::default();
        let mut runner =
            FailureDetectRunner::new(None, region_failover_manager, failure_detector_options);
        runner.start_with(Arc::new(container)).await;

        let dump = runner.dump().await;
        assert_eq!(dump.iter().collect::<Vec<_>>().len(), 1);

        runner.send_control(FailureDetectControl::Purge).await;

        let dump = runner.dump().await;
        assert_eq!(dump.iter().collect::<Vec<_>>().len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_heartbeat() {
        let region_failover_manager = create_region_failover_manager();
        let failure_detector_options = PhiAccrualFailureDetectorOptions::default();
        let mut runner =
            FailureDetectRunner::new(None, region_failover_manager, failure_detector_options);
        runner.start().await;

        // Generate 2000 heartbeats start from now. Heartbeat interval is one second, plus some random millis.
        fn generate_heartbeats(datanode_id: u64, region_ids: Vec<u32>) -> Vec<DatanodeHeartbeat> {
            let mut rng = rand::thread_rng();
            let start = current_time_millis();
            (0..2000)
                .map(|i| DatanodeHeartbeat {
                    region_idents: region_ids
                        .iter()
                        .map(|&region_number| RegionIdent {
                            table_id: 0,
                            cluster_id: 1,
                            datanode_id,
                            region_number,
                            engine: "mito2".to_string(),
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
    }
}
