use serde::{Deserialize, Serialize};

use crate::expr::GlobalId;
mod dataflow;
mod sinks;
mod sources;

pub(crate) use dataflow::{BuildDesc, DataflowDescription, IndexDesc};
