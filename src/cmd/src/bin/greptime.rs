use clap::Parser;
use datanode::DataNode;
use greptime_cmd::opts::{GrepTimeOpts, NodeType};

async fn datanode_main(_opts: &GrepTimeOpts) {
    let data_node = DataNode::new().unwrap();

    if let Err(e) = data_node.start().await {
        println!("Fail to start data node, error: {:?}", e);
    }
}

#[tokio::main]
async fn main() {
    let opts = GrepTimeOpts::parse();

    match opts.node_type {
        NodeType::Data => datanode_main(&opts).await,
    }
}
