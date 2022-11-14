use std::sync::Arc;

use etcd_client::Client;
use snafu::ResultExt;

use super::Election;
use crate::error;
use crate::error::Result;
use crate::metasrv::ElectionRef;
use crate::metasrv::LeaderValue;

#[derive(Clone)]
pub struct EtcdElection {
    leader_value: String,
    client: Client,
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
        }))
    }
}

#[async_trait::async_trait]
impl Election for EtcdElection {
    type Leader = LeaderValue;

    fn is_leader(&self) -> bool {
        todo!()
    }

    async fn campaign(&self) -> Result<()> {
        todo!()
    }

    async fn leader(&self) -> Result<LeaderValue> {
        todo!()
    }

    async fn resign(&self) -> Result<()> {
        todo!()
    }
}
