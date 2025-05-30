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

use std::collections::hash_map::IntoIter;
use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
use itertools::Itertools;
use session::context::{QueryContext, QueryContextRef};

use crate::tablesuffix::TableSuffixTemplate;
use crate::PipelineMap;

const DEFAULT_OPT: &str = "";

pub const PIPELINE_HINT_KEYS: [&str; 7] = [
    "greptime_auto_create_table",
    "greptime_ttl",
    "greptime_append_mode",
    "greptime_merge_mode",
    "greptime_physical_table",
    "greptime_skip_wal",
    "greptime_table_suffix",
];

const PIPELINE_HINT_PREFIX: &str = "greptime_";
pub const TABLE_SUFFIX_KEY: &str = "table_suffix";

#[derive(Debug)]
pub struct ContextOptMap(BTreeMap<String, String>);

impl ContextOptMap {
    // Remove hints from the pipeline context and form a option string
    // e.g: skip_wal=true,ttl=1d
    pub fn from_pipeline_map_to_opt(pipeline_map: &mut PipelineMap) -> Self {
        let mut btreemap = BTreeMap::new();
        for k in PIPELINE_HINT_KEYS {
            if let Some(v) = pipeline_map.remove(k) {
                btreemap.insert(k.replace(PIPELINE_HINT_PREFIX, ""), v.to_str_value());
            }
        }

        Self(btreemap)
    }

    pub(crate) fn resolve_table_suffix(
        &mut self,
        table_suffix: Option<&TableSuffixTemplate>,
        pipeline_map: &PipelineMap,
    ) -> Option<String> {
        self.0
            .remove(TABLE_SUFFIX_KEY)
            .or_else(|| table_suffix.and_then(|s| s.apply(pipeline_map)))
    }

    pub fn to_opt_string(self) -> String {
        self.0
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .join(",")
    }
}

// split the option string back to a map
fn from_opt_to_map(opt: &str) -> HashMap<&str, &str> {
    opt.split(',')
        .filter_map(|s| {
            s.split_once("=")
                .filter(|(k, v)| !k.is_empty() && !v.is_empty())
        })
        .collect()
}

// ContextReq is a collection of row insert requests with different options.
// The default option is empty string.
// Because options are set in query context, we have to split them into sequential calls
// e.g:
// {
//     "skip_wal=true,ttl=1d": [RowInsertRequest],
//     "ttl=1d": [RowInsertRequest],
// }
#[derive(Debug, Default)]
pub struct ContextReq {
    req: HashMap<String, Vec<RowInsertRequest>>,
}

impl ContextReq {
    pub fn from_opt_map(opt_map: HashMap<String, Rows>, table_name: String) -> Self {
        Self {
            req: opt_map
                .into_iter()
                .map(|(opt, rows)| {
                    (
                        opt,
                        vec![RowInsertRequest {
                            table_name: table_name.clone(),
                            rows: Some(rows),
                        }],
                    )
                })
                .collect::<HashMap<String, Vec<RowInsertRequest>>>(),
        }
    }

    pub fn default_opt_with_reqs(reqs: Vec<RowInsertRequest>) -> Self {
        let mut req_map = HashMap::new();
        req_map.insert(DEFAULT_OPT.to_string(), reqs);
        Self { req: req_map }
    }

    pub fn add_rows(&mut self, opt: String, req: RowInsertRequest) {
        self.req.entry(opt).or_default().push(req);
    }

    pub fn merge(&mut self, other: Self) {
        for (opt, req) in other.req {
            self.req.entry(opt).or_default().extend(req);
        }
    }

    pub fn as_req_iter(self, ctx: QueryContextRef) -> ContextReqIter {
        let ctx = (*ctx).clone();

        ContextReqIter {
            opt_req: self.req.into_iter(),
            ctx_template: ctx,
        }
    }

    pub fn all_req(self) -> impl Iterator<Item = RowInsertRequest> {
        self.req.into_iter().flat_map(|(_, req)| req)
    }

    pub fn ref_all_req(&self) -> impl Iterator<Item = &RowInsertRequest> {
        self.req.values().flatten()
    }
}

// ContextReqIter is an iterator that iterates over the ContextReq.
// The context template is cloned from the original query context.
// It will clone the query context for each option and set the options to the context.
// Then it will return the context and the row insert requests for actual insert.
pub struct ContextReqIter {
    opt_req: IntoIter<String, Vec<RowInsertRequest>>,
    ctx_template: QueryContext,
}

impl Iterator for ContextReqIter {
    type Item = (QueryContextRef, RowInsertRequests);

    fn next(&mut self) -> Option<Self::Item> {
        let (opt, req_vec) = self.opt_req.next()?;

        let opt_map = from_opt_to_map(&opt);

        let mut ctx = self.ctx_template.clone();
        for (k, v) in opt_map {
            ctx.set_extension(k, v);
        }

        Some((Arc::new(ctx), RowInsertRequests { inserts: req_vec }))
    }
}
