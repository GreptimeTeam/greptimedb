#![allow(unused)] // TODO(jiachun) rmove this
use std::collections::HashMap;
use std::sync::Arc;

use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::AskLeaderRequest;
use common_telemetry::debug;
use snafu::ResultExt;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Heartbeat {
    inner: Arc<Mutex<Inner>>,
}

impl Heartbeat {
    pub async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let mut inner = self.inner.lock().await;
        inner.start(urls).await
    }

    pub async fn ask_leader(&mut self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.ask_leader().await
    }

    // TODO(jiachun) send heartbeat
}

type HeartbeatChannel = HeartbeatClient<Channel>;

// Sending a request on a tonic channel requires a `&mut self`
// and thus can only send one request in flight.
// We implement a `Clone` for `Inner` since tonic channel provides
// a `Clone` implementation that is cheap.
#[derive(Clone, Debug, Default)]
struct Inner {
    clients: HashMap<String, HeartbeatChannel>,
    leader: Option<HeartbeatChannel>,
}

impl Inner {
    fn new() -> Self {
        Self {
            clients: HashMap::new(),
            leader: None,
        }
    }

    async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        if self.is_start() {
            return error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client already started",
            }
            .fail();
        }

        for url in urls.as_ref() {
            self.add_client_if_absent(url.as_ref());
        }

        Ok(())
    }

    async fn add_client_if_absent(&mut self, url: impl AsRef<str>) -> Result<HeartbeatChannel> {
        let url = url.as_ref();
        match self.clients.get(url) {
            Some(client) => Ok(client.clone()),
            None => {
                let client = HeartbeatClient::connect(url.to_string())
                    .await
                    .context(error::ConnectFailedSnafu { url })?;
                self.clients.insert(url.to_string(), client.clone());
                Ok(client)
            }
        }
    }

    async fn ask_leader(&mut self) -> Result<()> {
        if !self.is_start() {
            return error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client not start",
            }
            .fail();
        }

        let mut addr = None;
        for (url, client) in &self.clients {
            let req = AskLeaderRequest::default();
            let mut client = client.clone();
            match client.ask_leader(req).await {
                Ok(res) => {
                    if let Some(endpoint) = res.into_inner().leader {
                        addr = Some(endpoint.addr);
                        break;
                    }
                }
                Err(status) => {
                    debug!("Fail to ask leader from: {}, {}", url, status);
                }
            }
        }

        match addr {
            Some(addr) => {
                self.leader = Some(self.add_client_if_absent(&addr).await?);
                Ok(())
            }
            None => error::AskLeaderSnafu {}.fail(),
        }
    }

    #[inline]
    fn has_leader(&self) -> bool {
        self.leader.is_some()
    }

    #[inline]
    fn is_start(&self) -> bool {
        !self.clients.is_empty()
    }
}
