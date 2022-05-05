/// greptime commandline options
use clap::{ArgEnum, Parser};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
pub enum NodeType {
    /// Data node
    Data,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct GrepTimeOpts {
    #[clap(name = "type", short, long, arg_enum)]
    pub node_type: NodeType,
}
