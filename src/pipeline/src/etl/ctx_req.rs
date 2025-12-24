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
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
use session::context::{QueryContext, QueryContextRef};
use snafu::OptionExt;
use vrl::value::Value as VrlValue;

use crate::error::{Result, ValueMustBeMapSnafu};
use crate::tablesuffix::TableSuffixTemplate;

const GREPTIME_AUTO_CREATE_TABLE: &str = "greptime_auto_create_table";
const GREPTIME_TTL: &str = "greptime_ttl";
const GREPTIME_APPEND_MODE: &str = "greptime_append_mode";
const GREPTIME_MERGE_MODE: &str = "greptime_merge_mode";
const GREPTIME_PHYSICAL_TABLE: &str = "greptime_physical_table";
const GREPTIME_SKIP_WAL: &str = "greptime_skip_wal";
const GREPTIME_TABLE_SUFFIX: &str = "greptime_table_suffix";

pub(crate) const AUTO_CREATE_TABLE_KEY: &str = "auto_create_table";
pub(crate) const TTL_KEY: &str = "ttl";
pub(crate) const APPEND_MODE_KEY: &str = "append_mode";
pub(crate) const MERGE_MODE_KEY: &str = "merge_mode";
pub(crate) const PHYSICAL_TABLE_KEY: &str = "physical_table";
pub(crate) const SKIP_WAL_KEY: &str = "skip_wal";
pub(crate) const TABLE_SUFFIX_KEY: &str = "table_suffix";

pub const PIPELINE_HINT_KEYS: [&str; 7] = [
    GREPTIME_AUTO_CREATE_TABLE,
    GREPTIME_TTL,
    GREPTIME_APPEND_MODE,
    GREPTIME_MERGE_MODE,
    GREPTIME_PHYSICAL_TABLE,
    GREPTIME_SKIP_WAL,
    GREPTIME_TABLE_SUFFIX,
];

const PIPELINE_HINT_PREFIX: &str = "greptime_";

/// ContextOpt is a collection of options(including table options and pipeline options)
/// that should be extracted during the pipeline execution.
///
/// The options are set in the format of hint keys. See [`PIPELINE_HINT_KEYS`].
/// It's is used as the key in [`ContextReq`] for grouping the row insert requests.
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct ContextOpt {
    // table options, that need to be set in the query context before making row insert requests
    auto_create_table: Option<String>,
    ttl: Option<String>,
    append_mode: Option<String>,
    merge_mode: Option<String>,
    physical_table: Option<String>,
    skip_wal: Option<String>,

    // reset the schema in query context
    schema: Option<String>,

    // pipeline options, not set in query context
    // can be removed before the end of the pipeline execution
    table_suffix: Option<String>,
}

impl ContextOpt {
    pub fn set_physical_table(&mut self, physical_table: String) {
        self.physical_table = Some(physical_table);
    }

    pub fn set_schema(&mut self, schema: String) {
        self.schema = Some(schema);
    }
}

impl ContextOpt {
    pub fn from_pipeline_map_to_opt(value: &mut VrlValue) -> Result<Self> {
        let map = value.as_object_mut().context(ValueMustBeMapSnafu)?;

        let mut opt = Self::default();
        for k in PIPELINE_HINT_KEYS {
            if let Some(v) = map.remove(k) {
                let v = v.to_string_lossy().to_string();
                match k {
                    GREPTIME_AUTO_CREATE_TABLE => {
                        opt.auto_create_table = Some(v);
                    }
                    GREPTIME_TTL => {
                        opt.ttl = Some(v);
                    }
                    GREPTIME_APPEND_MODE => {
                        opt.append_mode = Some(v);
                    }
                    GREPTIME_MERGE_MODE => {
                        opt.merge_mode = Some(v);
                    }
                    GREPTIME_PHYSICAL_TABLE => {
                        opt.physical_table = Some(v);
                    }
                    GREPTIME_SKIP_WAL => {
                        opt.skip_wal = Some(v);
                    }
                    GREPTIME_TABLE_SUFFIX => {
                        opt.table_suffix = Some(v);
                    }
                    _ => {}
                }
            }
        }
        Ok(opt)
    }

    pub(crate) fn resolve_table_suffix(
        &mut self,
        table_suffix: Option<&TableSuffixTemplate>,
        pipeline_map: &VrlValue,
    ) -> Option<String> {
        self.table_suffix
            .take()
            .or_else(|| table_suffix.and_then(|s| s.apply(pipeline_map)))
    }

    pub fn set_query_context(self, ctx: &mut QueryContext) {
        if let Some(auto_create_table) = &self.auto_create_table {
            ctx.set_extension(AUTO_CREATE_TABLE_KEY, auto_create_table);
        }
        if let Some(ttl) = &self.ttl {
            ctx.set_extension(TTL_KEY, ttl);
        }
        if let Some(append_mode) = &self.append_mode {
            ctx.set_extension(APPEND_MODE_KEY, append_mode);
        }
        if let Some(merge_mode) = &self.merge_mode {
            ctx.set_extension(MERGE_MODE_KEY, merge_mode);
        }
        if let Some(physical_table) = &self.physical_table {
            ctx.set_extension(PHYSICAL_TABLE_KEY, physical_table);
        }
        if let Some(skip_wal) = &self.skip_wal {
            ctx.set_extension(SKIP_WAL_KEY, skip_wal);
        }
    }
}

/// ContextReq is a collection of row insert requests with different options.
/// The default option is all empty.
/// Because options are set in query context, we have to split them into sequential calls
/// The key is a [`ContextOpt`] struct for strong type.
/// e.g:
/// {
///     "skip_wal=true,ttl=1d": [RowInsertRequest],
///     "ttl=1d": [RowInsertRequest],
/// }
#[derive(Debug, Default)]
pub struct ContextReq {
    req: HashMap<ContextOpt, Vec<RowInsertRequest>>,
}

impl ContextReq {
    pub fn from_opt_map(opt_map: HashMap<ContextOpt, Rows>, table_name: String) -> Self {
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
                .collect::<HashMap<ContextOpt, Vec<RowInsertRequest>>>(),
        }
    }

    pub fn default_opt_with_reqs(reqs: Vec<RowInsertRequest>) -> Self {
        let mut req_map = HashMap::new();
        req_map.insert(ContextOpt::default(), reqs);
        Self { req: req_map }
    }

    pub fn add_row(&mut self, opt: &ContextOpt, req: RowInsertRequest) {
        match self.req.get_mut(opt) {
            None => {
                self.req.insert(opt.clone(), vec![req]);
            }
            Some(e) => {
                e.push(req);
            }
        }
    }

    pub fn add_rows(&mut self, opt: ContextOpt, reqs: impl IntoIterator<Item = RowInsertRequest>) {
        self.req.entry(opt).or_default().extend(reqs);
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

    pub fn map_len(&self) -> usize {
        self.req.len()
    }
}

// ContextReqIter is an iterator that iterates over the ContextReq.
// The context template is cloned from the original query context.
// It will clone the query context for each option and set the options to the context.
// Then it will return the context and the row insert requests for actual insert.
pub struct ContextReqIter {
    opt_req: IntoIter<ContextOpt, Vec<RowInsertRequest>>,
    ctx_template: QueryContext,
}

impl Iterator for ContextReqIter {
    type Item = (QueryContextRef, RowInsertRequests);

    fn next(&mut self) -> Option<Self::Item> {
        let (mut opt, req_vec) = self.opt_req.next()?;
        let mut ctx = self.ctx_template.clone();
        if let Some(schema) = opt.schema.take() {
            ctx.set_current_schema(&schema);
        }
        opt.set_query_context(&mut ctx);

        Some((Arc::new(ctx), RowInsertRequests { inserts: req_vec }))
    }
}
