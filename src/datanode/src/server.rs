pub mod grpc;

use std::default::Default;
use std::net::SocketAddr;
use std::sync::Arc;

use common_error::prelude::BoxedError;
use common_runtime::Builder as RuntimeBuilder;
use common_telemetry::info;
use frontend::frontend::{Frontend, FrontendOptions, Mode};
use frontend::instance::Instance as FrontendInstanceImpl;
use frontend::mysql::MysqlOptions;
use frontend::postgres::PostgresOptions;
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
            Mode::Standalone => Some(Self::build_frontend(opts, instance.clone()).await?),
            Mode::Distributed => {
                info!("Starting datanode in distributed mode, only gRPC server will be started.");
                None
            }
        };
        Ok(Self {
            grpc_server: GrpcServer::new(instance.clone(), instance, grpc_runtime),
            frontend,
        })
    }

    /// Build frontend instance in standalone mode
    async fn build_frontend(
        dn_opts: &DatanodeOptions,
        datanode_instance: InstanceRef,
    ) -> Result<Frontend<FrontendInstanceImpl>> {
        let grpc_server_addr = &dn_opts.rpc_addr;
        info!(
            "Build frontend with datanode gRPC addr: {}",
            grpc_server_addr
        );
        let mut fe_opts = FrontendOptions {
            mode: Mode::Standalone,
            datanode_rpc_addr: grpc_server_addr.clone(),
            ..Default::default()
        };

        Self::datanode_opts_to_frontend_opts(dn_opts, &mut fe_opts);
        let mut frontend_instance = FrontendInstanceImpl::try_new(&fe_opts)
            .await
            .context(BuildFrontendSnafu)?;
        frontend_instance.set_catalog_manager(datanode_instance.catalog_manager().clone());
        Ok(Frontend::new(fe_opts, frontend_instance))
    }

    /// Convert datanode options to frontend options in standalone mode.
    fn datanode_opts_to_frontend_opts(dn_opts: &DatanodeOptions, fe_opts: &mut FrontendOptions) {
        fe_opts.http_addr = Some(dn_opts.http_addr.clone());
        fe_opts.mysql_options = Some(MysqlOptions {
            addr: dn_opts.mysql_addr.clone(),
            runtime_size: dn_opts.mysql_runtime_size,
        });

        fe_opts.postgres_options = Some(PostgresOptions {
            addr: dn_opts.postgres_addr.clone(),
            runtime_size: dn_opts.postgres_runtime_size,
        });
    }

    pub async fn start(&mut self, opts: &DatanodeOptions) -> Result<()> {
        let grpc_addr: SocketAddr = opts.rpc_addr.parse().context(ParseAddrSnafu {
            addr: &opts.rpc_addr,
        })?;

        try_join!(self.grpc_server.start(grpc_addr), async {
            if let Some(ref mut frontend_instance) = self.frontend {
                info!("Starting frontend instance");
                frontend_instance
                    .start()
                    .await
                    .map_err(BoxedError::new)
                    .context(servers::error::StartFrontendSnafu)?;
            }
            Ok(())
        })
        .context(StartServerSnafu)?;
        Ok(())
    }
}
