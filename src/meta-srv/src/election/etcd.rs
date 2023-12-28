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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common_meta::distributed_time_constants::{META_KEEP_ALIVE_INTERVAL_SECS, META_LEASE_SECS};
use common_telemetry::{error, info, warn};
use etcd_client::Client;
use snafu::{OptionExt, ResultExt};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;

use crate::election::{Election, LeaderChangeMessage, ELECTION_KEY};
use crate::error;
use crate::error::Result;
use crate::metasrv::{ElectionRef, LeaderValue};

pub struct EtcdElection {
    leader_value: String,
    client: Client,
    is_leader: AtomicBool,
    infancy: AtomicBool,
    leader_watcher: broadcast::Sender<LeaderChangeMessage>,
    store_key_prefix: String,
}

impl EtcdElection {
    pub async fn with_endpoints<E, S>(
        leader_value: E,
        endpoints: S,
        store_key_prefix: String,
    ) -> Result<ElectionRef>
    where
        E: AsRef<str>,
        S: AsRef<[E]>,
    {
        let client = Client::connect(endpoints, None)
            .await
            .context(error::ConnectEtcdSnafu)?;

        Self::with_etcd_client(leader_value, client, store_key_prefix).await
    }

    pub async fn with_etcd_client<E>(
        leader_value: E,
        client: Client,
        store_key_prefix: String,
    ) -> Result<ElectionRef>
    where
        E: AsRef<str>,
    {
        let leader_value: String = leader_value.as_ref().into();

        let leader_ident = leader_value.clone();
        let (tx, mut rx) = broadcast::channel(100);
        let _handle = common_runtime::spawn_bg(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => match msg {
                        LeaderChangeMessage::Elected(key) => {
                            info!(
                                "[{leader_ident}] is elected as leader: {:?}, lease: {}",
                                key.name_str(),
                                key.lease()
                            );
                        }
                        LeaderChangeMessage::StepDown(key) => {
                            warn!(
                                "[{leader_ident}] is stepping down: {:?}, lease: {}",
                                key.name_str(),
                                key.lease()
                            );
                        }
                    },
                    Err(RecvError::Lagged(_)) => {
                        warn!("Log printing is too slow or leader changed too fast!");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });

        Ok(Arc::new(Self {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            infancy: AtomicBool::new(false),
            leader_watcher: tx,
            store_key_prefix,
        }))
    }

    fn election_key(&self) -> String {
        if self.store_key_prefix.is_empty() {
            ELECTION_KEY.to_string()
        } else {
            format!("{}{}", self.store_key_prefix, ELECTION_KEY)
        }
    }
}

#[async_trait::async_trait]
impl Election for EtcdElection {
    type Leader = LeaderValue;

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    fn in_infancy(&self) -> bool {
        self.infancy
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    async fn campaign(&self) -> Result<()> {
        let mut lease_client = self.client.lease_client();
        let mut election_client = self.client.election_client();
        let res = lease_client
            .grant(META_LEASE_SECS as i64, None)
            .await
            .context(error::EtcdFailedSnafu)?;
        let lease_id = res.id();

        info!("Election grant ttl: {:?}, lease: {:?}", res.ttl(), lease_id);

        // Campaign, waits to acquire leadership in an election, returning
        // a LeaderKey representing the leadership if successful.
        //
        // The method will be blocked until the election is won, and after
        // passing the method, it is necessary to execute `keep_alive` immediately
        // to confirm that it is a valid leader, because it is possible that the
        // election's lease expires.
        let res = election_client
            .campaign(self.election_key(), self.leader_value.clone(), lease_id)
            .await
            .context(error::EtcdFailedSnafu)?;

        if let Some(leader) = res.leader() {
            let (mut keeper, mut receiver) = lease_client
                .keep_alive(lease_id)
                .await
                .context(error::EtcdFailedSnafu)?;

            let mut keep_alive_interval =
                tokio::time::interval(Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS));
            loop {
                let _ = keep_alive_interval.tick().await;
                keeper.keep_alive().await.context(error::EtcdFailedSnafu)?;

                if let Some(res) = receiver.message().await.context(error::EtcdFailedSnafu)? {
                    if res.ttl() > 0 {
                        // Only after a successful `keep_alive` is the leader considered official.
                        if self
                            .is_leader
                            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                            .is_ok()
                        {
                            self.infancy.store(true, Ordering::Relaxed);

                            if let Err(e) = self
                                .leader_watcher
                                .send(LeaderChangeMessage::Elected(Arc::new(leader.clone())))
                            {
                                error!("Failed to send leader change message, error: {e}");
                            }
                        }
                    } else {
                        if self.is_leader.load(Ordering::Relaxed) {
                            if let Err(e) = self
                                .leader_watcher
                                .send(LeaderChangeMessage::StepDown(Arc::new(leader.clone())))
                            {
                                error!("Failed to send leader change message, error: {e}");
                            }
                        }
                        break;
                    }
                }
            }

            self.is_leader.store(false, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn leader(&self) -> Result<LeaderValue> {
        if self.is_leader.load(Ordering::Relaxed) {
            Ok(LeaderValue(self.leader_value.clone()))
        } else {
            let res = self
                .client
                .election_client()
                .leader(self.election_key())
                .await
                .context(error::EtcdFailedSnafu)?;
            let leader_value = res.kv().context(error::NoLeaderSnafu)?.value();
            let leader_value = String::from_utf8_lossy(leader_value).to_string();
            Ok(LeaderValue(leader_value))
        }
    }

    async fn resign(&self) -> Result<()> {
        todo!()
    }

    fn subscribe_leader_change(&self) -> Receiver<LeaderChangeMessage> {
        self.leader_watcher.subscribe()
    }
}
