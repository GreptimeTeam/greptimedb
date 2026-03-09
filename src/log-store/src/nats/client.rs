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

//! NATS client and JetStream stream initialisation.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::stream::{Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType, Stream};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::{Client, ConnectOptions};
use common_wal::config::nats::{DatanodeNatsConfig, NatsTlsConfig};
use snafu::ResultExt;

use crate::error::{ConnectNatsSnafu, CreateNatsStreamSnafu, GetNatsStreamSnafu, IoSnafu, Result};

pub(crate) type NatsClientRef = Arc<NatsClient>;

/// Wraps the NATS connection together with the JetStream context and the WAL
/// stream handle.  All WAL operations go through this struct.
#[derive(Debug)]
pub(crate) struct NatsClient {
    pub(crate) inner: Client,
    pub(crate) jetstream: JetStreamContext,
    pub(crate) stream_name: String,
}

impl NatsClient {
    /// Connect to NATS, build the JetStream context, and create (or assert) the
    /// WAL stream.
    pub(crate) async fn try_new(config: &DatanodeNatsConfig) -> Result<NatsClientRef> {
        let mut opts = ConnectOptions::new();

        // Authentication
        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            opts = opts.user_and_password(user.clone(), pass.clone());
        }
        if let Some(nkey) = &config.nkey_seed {
            opts = opts.nkey(nkey.clone());
        }
        if let Some(creds) = &config.credentials_file {
            opts = opts
                .credentials_file(creds)
                .await
                .context(IoSnafu { path: creds.clone() })?;
        }

        // TLS
        if let Some(tls) = &config.tls {
            if let Some(ca_cert) = &tls.ca_cert_path {
                opts = opts
                    .add_root_certificates(ca_cert)
                    .await
                    .context(IoSnafu { path: ca_cert.clone() })?;
            }
            if let (Some(cert), Some(key)) = (&tls.client_cert_path, &tls.client_key_path) {
                opts = opts
                    .add_client_certificate(cert, key)
                    .await
                    .context(IoSnafu { path: cert.clone() })?;
            }
            opts = opts.require_tls(true);
        }

        let client = async_nats::connect_with_options(config.servers.join(","), opts)
            .await
            .context(ConnectNatsSnafu {
                servers: config.servers.clone(),
            })?;

        let jetstream = async_nats::jetstream::new(client.clone());

        let stream_name = format!("{}-{}", config.stream_name_prefix, config.cluster_name);
        let subject_prefix = &config.subject_prefix;

        let stream_config = StreamConfig {
            name: stream_name.clone(),
            // Capture all subjects of the form "<prefix>.*"
            subjects: vec![format!("{}.*", subject_prefix)],
            storage: StorageType::File,
            num_replicas: config.num_replicas,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
            // allow_direct enables get_last_raw_message_by_subject
            allow_direct: true,
            ..Default::default()
        };

        jetstream
            .get_or_create_stream(stream_config)
            .await
            .context(CreateNatsStreamSnafu {
                stream_name: stream_name.clone(),
            })?;

        Ok(Arc::new(Self {
            inner: client,
            jetstream,
            stream_name,
        }))
    }

    /// Returns the JetStream stream handle.
    pub(crate) async fn stream(&self) -> Result<Stream> {
        self.jetstream
            .get_stream(&self.stream_name)
            .await
            .context(GetNatsStreamSnafu {
                stream_name: self.stream_name.clone(),
            })
    }
}
