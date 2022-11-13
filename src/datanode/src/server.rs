use std::default::Default;
use std::net::SocketAddr;
use std::sync::Arc;

use common_runtime::Builder as RuntimeBuilder;
use servers::grpc::GrpcServer;
use servers::server::Server;
use snafu::ResultExt;

use crate::datanode::DatanodeOptions;
use crate::error::{ParseAddrSnafu, Result, RuntimeResourceSnafu, StartServerSnafu};
use crate::instance::InstanceRef;

pub mod grpc;

/// All rpc services.
pub struct Services {
    grpc_server: GrpcServer,
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

        Ok(Self {
            grpc_server: GrpcServer::new(instance.clone(), instance, grpc_runtime),
        })
    }

    pub async fn start(&mut self, opts: &DatanodeOptions) -> Result<()> {
        let grpc_addr: SocketAddr = opts.rpc_addr.parse().context(ParseAddrSnafu {
            addr: &opts.rpc_addr,
        })?;
        self.grpc_server
            .start(grpc_addr)
            .await
            .context(StartServerSnafu)?;
        Ok(())
    }
}
