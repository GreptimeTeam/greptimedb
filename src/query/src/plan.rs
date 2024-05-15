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

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::table_name::TableName;
use common_query::prelude::ScalarValue;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{ParamValues, TableReference};
use datafusion_expr::LogicalPlan as DfLogicalPlan;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::Schema;
use snafu::ResultExt;
pub use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{ConvertDatafusionSchemaSnafu, DataFusionSnafu, Result};

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone, Debug)]
pub enum LogicalPlan {
    DfPlan(DfLogicalPlan),
}

impl LogicalPlan {
    /// Get the schema for this logical plan
    pub fn schema(&self) -> Result<Schema> {
        match self {
            Self::DfPlan(plan) => {
                let df_schema = plan.schema();
                df_schema
                    .clone()
                    .try_into()
                    .context(ConvertDatafusionSchemaSnafu)
            }
        }
    }

    /// Return a `format`able structure that produces a single line
    /// per node. For example:
    ///
    /// ```text
    /// Projection: employee.id
    ///    Filter: employee.state Eq Utf8(\"CO\")\
    ///       CsvScan: employee projection=Some([0, 3])
    /// ```
    pub fn display_indent(&self) -> impl Display + '_ {
        let LogicalPlan::DfPlan(plan) = self;
        plan.display_indent()
    }

    /// Walk the logical plan, find any `PlaceHolder` tokens,
    /// and return a map of their IDs and ConcreteDataTypes
    pub fn get_param_types(&self) -> Result<HashMap<String, Option<ConcreteDataType>>> {
        let LogicalPlan::DfPlan(plan) = self;
        let types = plan.get_parameter_types().context(DataFusionSnafu)?;

        Ok(types
            .into_iter()
            .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
            .collect())
    }

    /// Return a logical plan with all placeholders/params (e.g $1 $2,
    /// ...) replaced with corresponding values provided in the
    /// params_values
    pub fn replace_params_with_values(&self, values: &[ScalarValue]) -> Result<LogicalPlan> {
        let LogicalPlan::DfPlan(plan) = self;

        plan.clone()
            .replace_params_with_values(&ParamValues::List(values.to_vec()))
            .context(DataFusionSnafu)
            .map(LogicalPlan::DfPlan)
    }

    /// Unwrap the logical plan into a DataFusion logical plan
    pub fn unwrap_df_plan(self) -> DfLogicalPlan {
        match self {
            LogicalPlan::DfPlan(plan) => plan,
        }
    }

    /// Returns the DataFusion logical plan reference
    pub fn df_plan(&self) -> &DfLogicalPlan {
        match self {
            LogicalPlan::DfPlan(plan) => &plan,
        }
    }
}

impl From<DfLogicalPlan> for LogicalPlan {
    fn from(plan: DfLogicalPlan) -> Self {
        Self::DfPlan(plan)
    }
}

/// Visitor to extract table names from logical plan (TableScan node)
#[derive(Default)]
pub struct TableNamesExtractor {
    pub table_names: HashSet<TableName>,
}

impl TreeNodeVisitor for TableNamesExtractor {
    type Node = DfLogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> datafusion::error::Result<TreeNodeRecursion> {
        match node {
            DfLogicalPlan::TableScan(scan) => {
                if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>() {
                    if let Some(provider) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DfTableProviderAdapter>()
                    {
                        if provider.table().table_type() == TableType::Base {
                            let info = provider.table().table_info();
                            self.table_names.insert(TableName::new(
                                info.catalog_name.clone(),
                                info.schema_name.clone(),
                                info.name.clone(),
                            ));
                        }
                    }
                }
                match &scan.table_name {
                    TableReference::Full {
                        catalog,
                        schema,
                        table,
                    } => {
                        self.table_names.insert(TableName::new(
                            catalog.to_string(),
                            schema.to_string(),
                            table.to_string(),
                        ));
                    }
                    // TODO(ruihang): Maybe the following two cases should not be valid
                    TableReference::Partial { schema, table } => {
                        self.table_names.insert(TableName::new(
                            DEFAULT_CATALOG_NAME.to_string(),
                            schema.to_string(),
                            table.to_string(),
                        ));
                    }
                    TableReference::Bare { table } => {
                        self.table_names.insert(TableName::new(
                            DEFAULT_CATALOG_NAME.to_string(),
                            DEFAULT_SCHEMA_NAME.to_string(),
                            table.to_string(),
                        ));
                    }
                }

                Ok(TreeNodeRecursion::Continue)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }
}

/// Extract fully resolved table names from logical plan
pub fn extract_full_table_names(plan: &DfLogicalPlan) -> Result<HashSet<TableName>> {
    let mut extractor = TableNamesExtractor::default();
    let _ = plan.visit(&mut extractor).context(DataFusionSnafu)?;
    Ok(extractor.table_names)
}
