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

use std::default::Default;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::metric::try_handle;
use common_telemetry::{info, metric};
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, StatusCode};
use snafu::{ensure, ResultExt};
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::Mutex;

use crate::error::{AlreadyStartedSnafu, HyperSnafu, Result};
use crate::server::Server as ServerTrait;

pub const METRIC_SERVER: &str = "METRIC_SERVER";

/// a server that serves metrics
/// only start when datanode starts in distributed mode
#[derive(Clone)]
pub struct MetricsHandler;

impl MetricsHandler {
    pub fn render(&self) -> String {
        if let Some(handle) = metric::try_handle() {
            handle.render()
        } else {
            "Prometheus handle not initialized.".to_owned()
        }
    }
}
