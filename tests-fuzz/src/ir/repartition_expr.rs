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

use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};

use crate::ir::Ident;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitPartitionExpr {
    pub table_name: Ident,
    pub target: PartitionExpr,
    pub into: Vec<PartitionExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergePartitionExpr {
    pub table_name: Ident,
    pub targets: Vec<PartitionExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RepartitionExpr {
    Split(SplitPartitionExpr),
    Merge(MergePartitionExpr),
}
