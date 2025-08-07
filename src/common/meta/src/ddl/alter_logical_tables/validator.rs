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

use std::collections::HashSet;

use api::v1::alter_table_expr::Kind;
use api::v1::AlterTableExpr;
use snafu::{ensure, OptionExt};
use store_api::storage::TableId;
use table::table_reference::TableReference;

use crate::ddl::utils::table_id::get_all_table_ids_by_names;
use crate::ddl::utils::table_info::{
    all_logical_table_routes_have_same_physical_id, get_all_table_info_values_by_table_ids,
};
use crate::error::{
    AlterLogicalTablesInvalidArgumentsSnafu, Result, TableInfoNotFoundSnafu,
    TableRouteNotFoundSnafu,
};
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::{PhysicalTableRouteValue, TableRouteManager, TableRouteValue};
use crate::key::{DeserializedValueWithBytes, TableMetadataManagerRef};

/// [AlterLogicalTableValidator] validates the alter logical expressions.
pub struct AlterLogicalTableValidator<'a> {
    physical_table_id: TableId,
    alters: Vec<&'a AlterTableExpr>,
}

impl<'a> AlterLogicalTableValidator<'a> {
    pub fn new(physical_table_id: TableId, alters: Vec<&'a AlterTableExpr>) -> Self {
        Self {
            physical_table_id,
            alters,
        }
    }

    /// Validates all alter table expressions have the same schema and catalog.
    fn validate_schema(&self) -> Result<()> {
        let is_same_schema = self.alters.windows(2).all(|pair| {
            pair[0].catalog_name == pair[1].catalog_name
                && pair[0].schema_name == pair[1].schema_name
        });

        ensure!(
            is_same_schema,
            AlterLogicalTablesInvalidArgumentsSnafu {
                err_msg: "Schemas of the alter table expressions are not the same"
            }
        );

        Ok(())
    }

    /// Validates that all alter table expressions are of the supported kind.
    /// Currently only supports `AddColumns` operations.
    fn validate_alter_kind(&self) -> Result<()> {
        for alter in &self.alters {
            let kind = alter
                .kind
                .as_ref()
                .context(AlterLogicalTablesInvalidArgumentsSnafu {
                    err_msg: "Alter kind is missing",
                })?;

            let Kind::AddColumns(_) = kind else {
                return AlterLogicalTablesInvalidArgumentsSnafu {
                    err_msg: "Only support add columns operation",
                }
                .fail();
            };
        }

        Ok(())
    }

    fn table_names(&self) -> Vec<TableReference> {
        self.alters
            .iter()
            .map(|alter| {
                TableReference::full(&alter.catalog_name, &alter.schema_name, &alter.table_name)
            })
            .collect()
    }

    /// Validates that the physical table info and route exist.
    ///
    /// This method performs the following validations:
    /// 1. Retrieves the full table info and route for the given physical table id
    /// 2. Ensures the table info and table route exists
    /// 3. Verifies that the table route is actually a physical table route, not a logical one
    ///
    /// Returns a tuple containing the validated table info and physical table route.
    async fn validate_physical_table(
        &self,
        table_metadata_manager: &TableMetadataManagerRef,
    ) -> Result<(
        DeserializedValueWithBytes<TableInfoValue>,
        PhysicalTableRouteValue,
    )> {
        let (table_info, table_route) = table_metadata_manager
            .get_full_table_info(self.physical_table_id)
            .await?;

        let table_info = table_info.with_context(|| TableInfoNotFoundSnafu {
            table: format!("table id - {}", self.physical_table_id),
        })?;

        let physical_table_route = table_route
            .context(TableRouteNotFoundSnafu {
                table_id: self.physical_table_id,
            })?
            .into_inner();

        let TableRouteValue::Physical(table_route) = physical_table_route else {
            return AlterLogicalTablesInvalidArgumentsSnafu {
                err_msg: format!(
                    "expected a physical table but got a logical table: {:?}",
                    self.physical_table_id
                ),
            }
            .fail();
        };

        Ok((table_info, table_route))
    }

    /// Validates that all logical table routes have the same physical table id.
    ///
    /// This method performs the following validations:
    /// 1. Retrieves table routes for all the given table ids.
    /// 2. Ensures that all retrieved routes are logical table routes (not physical)
    /// 3. Verifies that all logical table routes reference the same physical table id.
    /// 4. Returns an error if any route is not logical or references a different physical table.
    async fn validate_logical_table_routes(
        &self,
        table_route_manager: &TableRouteManager,
        table_ids: &[TableId],
    ) -> Result<()> {
        let all_logical_table_routes_have_same_physical_id =
            all_logical_table_routes_have_same_physical_id(
                table_route_manager,
                table_ids,
                self.physical_table_id,
            )
            .await?;

        ensure!(
            all_logical_table_routes_have_same_physical_id,
            AlterLogicalTablesInvalidArgumentsSnafu {
                err_msg: "All the tasks should have the same physical table id"
            }
        );

        Ok(())
    }

    /// Validates the alter logical expressions.
    ///
    /// This method performs the following validations:
    /// 1. Validates that all alter table expressions have the same schema and catalog.
    /// 2. Validates that all alter table expressions are of the supported kind.
    /// 3. Validates that the physical table info and route exist.
    /// 4. Validates that all logical table routes have the same physical table id.
    ///
    /// Returns a [ValidatorResult] containing the validation results.
    pub async fn validate(
        &self,
        table_metadata_manager: &TableMetadataManagerRef,
    ) -> Result<ValidatorResult> {
        self.validate_schema()?;
        self.validate_alter_kind()?;
        let (physical_table_info, physical_table_route) =
            self.validate_physical_table(table_metadata_manager).await?;
        let table_names = self.table_names();
        let table_ids =
            get_all_table_ids_by_names(table_metadata_manager.table_name_manager(), &table_names)
                .await?;
        let mut table_info_values = get_all_table_info_values_by_table_ids(
            table_metadata_manager.table_info_manager(),
            &table_ids,
            &table_names,
        )
        .await?;
        self.validate_logical_table_routes(
            table_metadata_manager.table_route_manager(),
            &table_ids,
        )
        .await?;
        let skip_alter = self
            .alters
            .iter()
            .zip(table_info_values.iter())
            .map(|(task, table)| skip_alter_logical_region(task, table))
            .collect::<Vec<_>>();
        retain_unskipped(&mut table_info_values, &skip_alter);
        let num_skipped = skip_alter.iter().filter(|&&x| x).count();

        Ok(ValidatorResult {
            num_skipped,
            skip_alter,
            table_info_values,
            physical_table_info,
            physical_table_route,
        })
    }
}

/// The result of the validator.
pub(crate) struct ValidatorResult {
    pub(crate) num_skipped: usize,
    pub(crate) skip_alter: Vec<bool>,
    pub(crate) table_info_values: Vec<DeserializedValueWithBytes<TableInfoValue>>,
    pub(crate) physical_table_info: DeserializedValueWithBytes<TableInfoValue>,
    pub(crate) physical_table_route: PhysicalTableRouteValue,
}

/// Retains the elements that are not skipped.
pub(crate) fn retain_unskipped<T>(target: &mut Vec<T>, skipped: &[bool]) {
    debug_assert_eq!(target.len(), skipped.len());
    let mut iter = skipped.iter();
    target.retain(|_| !iter.next().unwrap());
}

/// Returns true if does not required to alter the logical region.
fn skip_alter_logical_region(alter: &AlterTableExpr, table: &TableInfoValue) -> bool {
    let existing_columns = table
        .table_info
        .meta
        .schema
        .column_schemas
        .iter()
        .map(|c| &c.name)
        .collect::<HashSet<_>>();

    let Some(kind) = alter.kind.as_ref() else {
        return true; // Never get here since we have checked it in `validate_alter_kind`
    };
    let Kind::AddColumns(add_columns) = kind else {
        return true; // Never get here since we have checked it in `validate_alter_kind`
    };

    // We only check that all columns have been finished. That is to say,
    // if one part is finished but another part is not, it will be considered
    // unfinished.
    add_columns
        .add_columns
        .iter()
        .map(|add_column| add_column.column_def.as_ref().map(|c| &c.name))
        .all(|column| {
            column
                .map(|c| existing_columns.contains(c))
                .unwrap_or(false)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retain_unskipped() {
        let mut target = vec![1, 2, 3, 4, 5];
        let skipped = vec![false, true, false, true, false];
        retain_unskipped(&mut target, &skipped);
        assert_eq!(target, vec![1, 3, 5]);
    }
}
