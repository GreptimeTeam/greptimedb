pub mod grpc;

use std::net::SocketAddr;
use std::sync::Arc;

use common_runtime::Builder as RuntimeBuilder;
use common_telemetry::info;
use frontend::frontend::{Frontend, FrontendOptions, Mode};
use frontend::instance::Instance as FrontendInstanceImpl;
use servers::grpc::GrpcServer;
use servers::server::Server;
use snafu::ResultExt;
use tokio::try_join;

use crate::datanode::DatanodeOptions;
use crate::error::{
    BuildFrontendSnafu, ParseAddrSnafu, Result, RuntimeResourceSnafu, StartServerSnafu,
};
use crate::instance::InstanceRef;

/// All rpc services.
pub struct Services {
    grpc_server: GrpcServer,
    frontend: Option<Frontend<FrontendInstanceImpl>>,
}

impl Services {
    pub async fn try_new(instance: InstanceRef, opts: &DatanodeOptions) -> Result<Self> {
        let grpc_runtime = Arc::new(
            RuntimeBuilder::default()
                .worker_threads(opts.rpc_runtime_size as usize)
                .thread_name("grpc-io-handlers")
                .build()
                .context(RuntimeResourceSnafu)?,
        );

        let frontend = match opts.mode {
            Mode::Standalone => Some(Self::build_frontend(opts).await?),
            Mode::Distributed => {
                info!("Starting datanode in distributed mode, only gRPC server will be started.");
                None
            }
        };
        Ok(Self {
            grpc_server: GrpcServer::new(instance.clone(), instance.clone(), grpc_runtime),
            frontend,
        })
    }

    /// Build frontend instance in standalone mode
    async fn build_frontend(opts: &DatanodeOptions) -> Result<Frontend<FrontendInstanceImpl>> {
        let grpc_server_addr = &opts.rpc_addr;
        info!(
            "Build frontend with datanode gRPC addr: {}",
            grpc_server_addr
        );
        let options = FrontendOptions {
            mode: Mode::Standalone,
            datanode_rpc_addr: grpc_server_addr.clone(),
            ..Default::default()
        };
        let frontend_instance = FrontendInstanceImpl::try_new(&options)
            .await
            .context(BuildFrontendSnafu)?;
        Ok(Frontend::new(options, frontend_instance))
    }

    // TODO(LFC): make servers started on demand (not starting mysql if no needed, for example)
    pub async fn start(&mut self, opts: &DatanodeOptions) -> Result<()> {
        let grpc_addr: SocketAddr = opts.rpc_addr.parse().context(ParseAddrSnafu {
            addr: &opts.rpc_addr,
        })?;

        try_join!(self.grpc_server.start(grpc_addr), async {
            if let Some(ref mut frontend_instance) = self.frontend {
                info!("Starting frontend instance");
                let _ = frontend_instance.start().await;
            }
            Ok(())
        })
        .context(StartServerSnafu)?;
        Ok(())
    }
}
