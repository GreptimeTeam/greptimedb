use api::v1::{greptime_client::GreptimeClient, *};
use snafu::{OptionExt, ResultExt};
use tonic::transport::Channel;

use crate::error;
use crate::Result;

#[derive(Clone, Debug, Default)]
pub struct Client {
    client: Option<GreptimeClient<Channel>>,
}

impl Client {
    pub async fn start(&mut self, url: impl Into<String>) -> Result<()> {
        match self.client.as_ref() {
            None => {
                let url = url.into();
                let client = GreptimeClient::connect(url.clone())
                    .await
                    .context(error::ConnectFailedSnafu { url })?;
                self.client = Some(client);
                Ok(())
            }
            Some(_) => error::IllegalGrpcClientStateSnafu {
                err_msg: "already started",
            }
            .fail(),
        }
    }

    pub fn with_client(client: GreptimeClient<Channel>) -> Self {
        Self {
            client: Some(client),
        }
    }

    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        let client = GreptimeClient::connect(url.clone())
            .await
            .context(error::ConnectFailedSnafu { url })?;
        Ok(Self {
            client: Some(client),
        })
    }

    pub async fn admin(&self, req: AdminRequest) -> Result<AdminResponse> {
        let req = BatchRequest {
            admins: vec![req],
            ..Default::default()
        };

        let mut res = self.batch(req).await?;
        res.admins.pop().context(error::MissingResultSnafu {
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
        res.databases.pop().context(error::MissingResultSnafu {
            name: "database",
            expected: 1_usize,
            actual: 0_usize,
        })
    }

    pub async fn batch(&self, req: BatchRequest) -> Result<BatchResponse> {
        if let Some(client) = self.client.as_ref() {
            let res = client
                .clone()
                .batch(req)
                .await
                .context(error::TonicStatusSnafu)?;
            Ok(res.into_inner())
        } else {
            error::IllegalGrpcClientStateSnafu {
                err_msg: "not started",
            }
            .fail()
        }
    }
}
