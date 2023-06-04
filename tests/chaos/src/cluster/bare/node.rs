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

use datanode::datanode::DatanodeOptions;
use frontend::frontend::FrontendOptions;
use meta_srv::metasrv::MetaSrvOptions;

use crate::cluster::bare::process::ProcessInfo;

#[derive(Debug, Clone)]
pub enum NodeInfo {
    Datanode { options: Box<DatanodeOptions> },
    Frontend { options: Box<FrontendOptions> },
    Metasrv { options: Box<MetaSrvOptions> },
}

#[derive(Debug, Clone)]
/// A fundamental (logical) unit in a local cluster.
pub struct Node {
    pub info: NodeInfo,
    // The physical process may be killed by chaosd
    pub process: Option<ProcessInfo>,
}
