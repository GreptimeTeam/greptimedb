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

#[cfg(any(test, feature = "testing", debug_assertions))]
use common_catalog::consts::NUMBERS_TABLE_ID;
#[cfg(any(test, feature = "testing", debug_assertions))]
use table::table::numbers::NumbersTable;
#[cfg(any(test, feature = "testing", debug_assertions))]
use table::table::numbers::NUMBERS_TABLE_NAME;
use table::TableRef;

// NumbersTableProvider is a dedicated provider for feature-gating the numbers table.
#[derive(Clone)]
pub struct NumbersTableProvider;

#[cfg(any(test, feature = "testing", debug_assertions))]
impl NumbersTableProvider {
    pub(crate) fn table_exists(&self, name: &str) -> bool {
        name == NUMBERS_TABLE_NAME
    }

    pub(crate) fn table_names(&self) -> Vec<String> {
        vec![NUMBERS_TABLE_NAME.to_string()]
    }

    pub(crate) fn table(&self, name: &str) -> Option<TableRef> {
        if name == NUMBERS_TABLE_NAME {
            Some(NumbersTable::table(NUMBERS_TABLE_ID))
        } else {
            None
        }
    }
}

#[cfg(not(any(test, feature = "testing", debug_assertions)))]
impl NumbersTableProvider {
    pub(crate) fn table_exists(&self, _name: &str) -> bool {
        false
    }

    pub(crate) fn table_names(&self) -> Vec<String> {
        vec![]
    }

    pub(crate) fn table(&self, _name: &str) -> Option<TableRef> {
        None
    }
}
