use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use super::HttpHandler;
use crate::cluster::MetaPeerClient;
use crate::error::{self, Result};
use crate::keys::StatValue;

pub struct HeartBeatHandler {
    pub meta_peer_client: Option<MetaPeerClient>,
}

#[async_trait::async_trait]
impl HttpHandler for HeartBeatHandler {
    async fn handle(&self, _: &str, _: &HashMap<String, String>) -> Result<http::Response<String>> {
        let meta_peer_client = self
            .meta_peer_client
            .as_ref()
            .context(error::NoMetaPeerClientSnafu)?;
        let stat_kvs = meta_peer_client.get_all_dn_stat_kvs().await?;
        let stat_vals: Vec<StatValue> = stat_kvs.into_values().collect();
        let result = StatValues { stat_vals }.try_into()?;

        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(result)
            .unwrap())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StatValues {
    pub stat_vals: Vec<StatValue>,
}

impl TryFrom<StatValues> for String {
    type Error = error::Error;

    fn try_from(vals: StatValues) -> Result<Self> {
        serde_json::to_string(&vals).context(error::SerializeToJsonSnafu {
            input: format!("{vals:?}"),
        })
    }
}
