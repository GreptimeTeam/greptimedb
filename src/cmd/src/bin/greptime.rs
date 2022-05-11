use clap::Parser;
use cmd::opts::{GrepTimeOpts, NodeType};
use common_telemetry::{self, logging::error};
use datanode::DataNode;

async fn datanode_main(_opts: &GrepTimeOpts) {
    let data_node = DataNode::new().expect("error new data node");

    if let Err(e) = data_node.start().await {
        error!("Fail to start data node, error: {:?}", e);
    }
}

#[tokio::main]
async fn main() {
    let opts = GrepTimeOpts::parse();
    let node_type = opts.node_type;
    // TODO(dennis): 1. adds ip/port to app
    //                          2. config log dir
    let app = format!("{node_type:?}-node").to_lowercase();

    common_telemetry::set_panic_hook();
    common_telemetry::init_default_metrics_recorder();
    let _guard = common_telemetry::init_global_logging(&app, "logs", "info", false);

    match node_type {
        NodeType::Data => datanode_main(&opts).await,
    }
}
