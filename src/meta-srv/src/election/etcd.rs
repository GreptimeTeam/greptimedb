use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common_telemetry::{info, warn};
use etcd_client::Client;
use snafu::{OptionExt, ResultExt};

use crate::election::{Election, ELECTION_KEY, LEASE_SECS, PROCLAIM_PERIOD_SECS};
use crate::error;
use crate::error::Result;
use crate::metasrv::{ElectionRef, LeaderValue};

pub struct EtcdElection {
    leader_value: String,
    client: Client,
    is_leader: AtomicBool,
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
        }))
    }
}

#[async_trait::async_trait]
impl Election for EtcdElection {
    type Leader = LeaderValue;

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    async fn campaign(&self) -> Result<()> {
        let mut lease_client = self.client.lease_client();
        let mut election_client = self.client.election_client();
        let res = lease_client
            .grant(LEASE_SECS, None)
            .await
            .context(error::EtcdFailedSnafu)?;
        let lease_id = res.id();

        info!("Election grant ttl: {:?}, id: {:?}", res.ttl(), lease_id);

        // campaign
        let res = election_client
            .campaign(ELECTION_KEY, self.leader_value.clone(), lease_id)
            .await
            .context(error::EtcdFailedSnafu)?;

        if let Some(leader) = res.leader() {
            info!(
                "[{}] becoming leader: {:?}, lease: {}",
                &self.leader_value,
                leader.name_str(),
                leader.lease()
            );

            let (mut keeper, mut receiver) = self
                .client
                .lease_client()
                .keep_alive(lease_id)
                .await
                .context(error::EtcdFailedSnafu)?;

            let mut interval = tokio::time::interval(Duration::from_secs(PROCLAIM_PERIOD_SECS));
            loop {
                interval.tick().await;
                keeper.keep_alive().await.context(error::EtcdFailedSnafu)?;

                if let Some(res) = receiver.message().await.context(error::EtcdFailedSnafu)? {
                    if res.ttl() > 0 {
                        self.is_leader.store(true, Ordering::Relaxed);
                    } else {
                        warn!(
                            "Already lost leader status, lease: {}, will re-initiate election",
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
