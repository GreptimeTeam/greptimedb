use clap::Parser;
use cmd::opts::{GrepTimeOpts, NodeType};
use common_telemetry::{logging, panic_hook};
use datanode::DataNode;

async fn datanode_main(_opts: &GrepTimeOpts) {
    let data_node = DataNode::new().unwrap();

    if let Err(e) = data_node.start().await {
        logging::error!("Fail to start data node, error: {:?}", e);
    }
}

#[tokio::main]
async fn main() {
    let opts = GrepTimeOpts::parse();

    let node_type = opts.node_type;

    // TODO(dennis): 1. adds ip/port to app
    //                          2. config log dir
    let app = format!("{node_type:?}-node").to_lowercase();

    panic_hook::set_panic_hook();
    let _guard = logging::init_global_tracing(&app, "logs", "info", false);

    match node_type {
        NodeType::Data => datanode_main(&opts).await,
    }
}
