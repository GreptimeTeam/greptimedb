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

use std::collections::HashMap;
use std::time::Duration;

use common_telemetry::error;
use common_time::util::current_time_millis;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

use crate::failure_detector::PhiAccrualFailureDetector;
use crate::handler::failure_handler::{DatanodeHeartbeat, RegionIdent};
use crate::metasrv::ElectionRef;

pub(crate) enum FailureDetectControl {
    Purge,

    #[cfg(test)]
    Dump(tokio::sync::oneshot::Sender<Box<dyn FailureDetectorContainer>>),
}

pub(crate) struct FailureDetectRunner {
    election: Option<ElectionRef>,

    heartbeat_tx: Sender<DatanodeHeartbeat>,
    heartbeat_rx: Option<Receiver<DatanodeHeartbeat>>,

    control_tx: Sender<FailureDetectControl>,
    control_rx: Option<Receiver<FailureDetectControl>>,

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
        let failure_detectors = Box::new(DefaultFailureDetectorContainer(HashMap::new()));
        self.start_with(failure_detectors).await
    }

    async fn start_with(&mut self, failure_detectors: Box<dyn FailureDetectorContainer>) {
        let Some(mut heartbeat_rx) = self.heartbeat_rx.take() else { return };
        let Some(mut control_rx) = self.control_rx.take() else { return };

        let election = self.election.clone();
        let mut failure_detectors = failure_detectors;
        let handle = common_runtime::spawn_bg(async move {
            loop {
                loop {
                    let maybe_control = control_rx.try_recv();
                    match maybe_control {
                        Ok(control) => match control {
                            FailureDetectControl::Purge => failure_detectors.clear(),

                            #[cfg(test)]
                            FailureDetectControl::Dump(tx) => {
                                let _ = tx.send(failure_detectors.dump());
                            }
                        },
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => return,
                    }
                }

                loop {
                    let maybe_heartbeat = heartbeat_rx.try_recv();
                    match maybe_heartbeat {
                        Ok(heartbeat) => {
                            for ident in heartbeat.region_idents {
                                let detector = failure_detectors.get_failure_detector(ident);
                                detector.heartbeat(heartbeat.heartbeat_time);
                            }
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => return,
                    }
                }

                let is_leader = election.as_ref().map(|x| x.is_leader()).unwrap_or(true);
                if is_leader {
                    let now = current_time_millis();
                    for detector in failure_detectors.iter() {
                        if !detector.is_available(now) {
                            // TODO(LFC): TBC
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        self.runner_handle = Some(handle);
    }

    #[cfg(test)]
    fn abort(&mut self) {
        let Some(handle) = self.runner_handle.take() else { return };
        handle.abort();
    }

    #[cfg(test)]
    pub(crate) async fn dump(&self) -> Box<dyn FailureDetectorContainer> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send_control(FailureDetectControl::Dump(tx)).await;
        rx.await.unwrap()
    }
}

pub(crate) trait FailureDetectorContainer: Send {
    fn get_failure_detector(&mut self, ident: RegionIdent) -> &mut PhiAccrualFailureDetector;
    fn iter(&self) -> Box<dyn Iterator<Item = &PhiAccrualFailureDetector> + '_>;
    fn clear(&mut self);

    #[cfg(test)]
    fn dump(&self) -> Box<dyn FailureDetectorContainer>;
}

struct DefaultFailureDetectorContainer(HashMap<RegionIdent, PhiAccrualFailureDetector>);

impl FailureDetectorContainer for DefaultFailureDetectorContainer {
    fn get_failure_detector(&mut self, ident: RegionIdent) -> &mut PhiAccrualFailureDetector {
        self.0
            .entry(ident)
            .or_insert_with(PhiAccrualFailureDetector::default)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &PhiAccrualFailureDetector> + '_> {
        Box::new(self.0.values()) as _
    }

    fn clear(&mut self) {
        self.0.clear()
    }

    #[cfg(test)]
    fn dump(&self) -> Box<dyn FailureDetectorContainer> {
        let mut m = HashMap::with_capacity(self.0.len());
        m.extend(self.0.iter().map(|(k, v)| (k.clone(), v.clone())));
        Box::new(Self(m))
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_default_failure_detector_container() {
        let mut container = DefaultFailureDetectorContainer(HashMap::new());
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
        let mut container = DefaultFailureDetectorContainer(HashMap::new());

        let ident = RegionIdent {
            catalog: "a".to_string(),
            schema: "b".to_string(),
            table: "c".to_string(),
            region_id: 1,
        };
        container.get_failure_detector(ident.clone());

        let mut runner = FailureDetectRunner::new(None);
        runner.start_with(Box::new(container)).await;

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

        failure_detectors.iter().for_each(|fd| {
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
