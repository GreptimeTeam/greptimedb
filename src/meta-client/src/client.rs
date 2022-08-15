use api::v1::meta::meta_service_client::MetaServiceClient;
use snafu::ResultExt;
use tonic::transport::Channel;

use crate::error::{self, Result};

type InnerClient = MetaServiceClient<Channel>;

#[derive(Clone, Debug)]
pub struct Client {
    // client to leader endpoint
    leader: InnerClient,
    endpoints: Vec<String>,
}

impl Client {
    async fn connect(url: impl Into<String>) -> Result<InnerClient> {
        let url = url.into();
        let client = MetaServiceClient::connect(url.clone())
            .await
            .context(error::ConnectFailedSnafu { url })?;

        Ok(client)
    }
}
