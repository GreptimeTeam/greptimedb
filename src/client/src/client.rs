use api::v1::{greptime_client::GreptimeClient, *};
use snafu::{OptionExt, ResultExt};
use tonic::transport::Channel;

use crate::{ConnectFailedSnafu, MissingResultSnafu, Result};

#[derive(Clone, Debug)]
pub struct Client {
    client: GreptimeClient<Channel>,
}

impl Client {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        let client = GreptimeClient::connect(url.clone())
            .await
            .context(ConnectFailedSnafu { url })?;
        Ok(Self { client })
    }

    pub async fn admin(&self, req: AdminRequest) -> Result<AdminResponse> {
        let req = BatchRequest {
            admins: vec![req],
            ..Default::default()
        };

        let mut res = self.batch(req).await?;
        res.admins.pop().context(MissingResultSnafu {
            name: "admins",
            expected: 1_usize,
            actual: 0_usize,
        })
    }

    pub async fn database(&self, req: DatabaseRequest) -> Result<DatabaseResponse> {
        let req = BatchRequest {
            databases: vec![req],
            ..Default::default()
        };

        let mut res = self.batch(req).await?;
        res.databases.pop().context(MissingResultSnafu {
            name: "database",
            expected: 1_usize,
            actual: 0_usize,
        })
    }

    pub async fn batch(&self, req: BatchRequest) -> Result<BatchResponse> {
        let res = self.client.clone().batch(req).await?;
        Ok(res.into_inner())
    }
}
