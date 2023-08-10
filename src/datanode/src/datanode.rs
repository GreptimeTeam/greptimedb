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

//! Datanode configurations

use std::sync::Arc;

use common_base::Plugins;
use common_error::ext::BoxedError;
pub use common_procedure::options::ProcedureConfig;
use common_telemetry::info;
use datanode_options::DatanodeOptions;
use servers::Mode;
use snafu::ResultExt;

use crate::error::{Result, ShutdownInstanceSnafu};
use crate::heartbeat::HeartbeatTask;
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

/// Datanode service.
pub struct Datanode {
    opts: DatanodeOptions,
    services: Option<Services>,
    instance: InstanceRef,
    heartbeat_task: Option<HeartbeatTask>,
}

impl Datanode {
    pub async fn new(opts: DatanodeOptions, plugins: Arc<Plugins>) -> Result<Datanode> {
        let (instance, heartbeat_task) = Instance::with_opts(&opts, plugins).await?;
        let services = match opts.mode {
            Mode::Distributed => Some(Services::try_new(instance.clone(), &opts).await?),
            Mode::Standalone => None,
        };
        Ok(Self {
            opts,
            services,
            instance,
            heartbeat_task,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting datanode instance...");
        self.start_instance().await?;
        self.start_services().await
    }

    /// Start only the internal component of datanode.
    pub async fn start_instance(&mut self) -> Result<()> {
        let _ = self.instance.start().await;
        if let Some(task) = &self.heartbeat_task {
            task.start().await?;
        }
        Ok(())
    }

    /// Start services of datanode. This method call will block until services are shutdown.
    pub async fn start_services(&mut self) -> Result<()> {
        if let Some(service) = self.services.as_mut() {
            service.start(&self.opts).await
        } else {
            Ok(())
        }
    }

    pub fn get_instance(&self) -> InstanceRef {
        self.instance.clone()
    }

    pub async fn shutdown_instance(&self) -> Result<()> {
        if let Some(heartbeat_task) = &self.heartbeat_task {
            heartbeat_task
                .close()
                .await
                .map_err(BoxedError::new)
                .context(ShutdownInstanceSnafu)?;
        }
        let _ = self.instance.shutdown().await;
        Ok(())
    }

    async fn shutdown_services(&self) -> Result<()> {
        if let Some(service) = self.services.as_ref() {
            service.shutdown().await
        } else {
            Ok(())
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        // We must shutdown services first
        self.shutdown_services().await?;
        self.shutdown_instance().await
    }
}
