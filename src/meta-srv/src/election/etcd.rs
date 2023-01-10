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

use common_telemetry::{info, warn};
use etcd_client::Client;
use snafu::{OptionExt, ResultExt};

use crate::election::{Election, ELECTION_KEY, KEEP_ALIVE_PERIOD_SECS, LEASE_SECS};
use crate::error;
use crate::error::Result;
use crate::metasrv::{ElectionRef, LeaderValue};

pub struct EtcdElection {
    leader_value: String,
    client: Client,
    is_leader: AtomicBool,
    infancy: AtomicBool,
}

impl EtcdElection {
    pub async fn with_endpoints<E, S>(leader_value: E, endpoints: S) -> Result<ElectionRef>
    where
        E: AsRef<str>,
        S: AsRef<[E]>,
    {
        let leader_value = leader_value.as_ref().into();
        let client = Client::connect(endpoints, None)
            .await
            .context(error::ConnectEtcdSnafu)?;

        Ok(Arc::new(Self {
            leader_value,
            client,
            is_leader: AtomicBool::new(false),
            infancy: AtomicBool::new(false),
        }))
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
            .grant(LEASE_SECS, None)
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
            .campaign(ELECTION_KEY, self.leader_value.clone(), lease_id)
            .await
            .context(error::EtcdFailedSnafu)?;

        if let Some(leader) = res.leader() {
            let (mut keeper, mut receiver) = self
                .client
                .lease_client()
                .keep_alive(lease_id)
                .await
                .context(error::EtcdFailedSnafu)?;

            let mut keep_alive_interval =
                tokio::time::interval(Duration::from_secs(KEEP_ALIVE_PERIOD_SECS));
            loop {
                keep_alive_interval.tick().await;
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
                            info!(
                                "[{}] becoming leader: {:?}, lease: {}",
                                &self.leader_value,
                                leader.name_str(),
                                leader.lease()
                            );
                        }
                    } else {
                        warn!(
                            "Failed to keep-alive, lease: {}, will re-initiate election",
                            leader.lease()
                        );
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
                .leader(ELECTION_KEY)
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
}
