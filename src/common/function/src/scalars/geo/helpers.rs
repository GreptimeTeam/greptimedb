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
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error::{self, Result};
use geo_types::geometry::Geometry;
use snafu::ResultExt;
use wkt::TryFromWkt;

macro_rules! ensure_columns_len {
    ($columns:ident) => {
        snafu::ensure!(
            $columns.windows(2).all(|c| c[0].len() == c[1].len()),
            common_query::error::InvalidFuncArgsSnafu {
                err_msg: "The length of input columns are in different size"
            }
        )
    };
    ($column_a:ident, $column_b:ident, $($column_n:ident),*) => {
        snafu::ensure!(
            {
                let mut result = $column_a.len() == $column_b.len();
                $(
                result = result && ($column_a.len() == $column_n.len());
                )*
                result
            }
            common_query::error::InvalidFuncArgsSnafu {
                err_msg: "The length of input columns are in different size"
            }
        )
    };
}

pub(super) use ensure_columns_len;

macro_rules! ensure_columns_n {
    ($columns:ident, $n:literal) => {
        snafu::ensure!(
            $columns.len() == $n,
            common_query::error::InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of arguments is not correct, expect {}, provided : {}",
                    stringify!($n),
                    $columns.len()
                ),
            }
        );

        if $n > 1 {
            ensure_columns_len!($columns);
        }
    };
}

pub(super) use ensure_columns_n;

macro_rules! ensure_and_coerce {
    ($compare:expr, $coerce:expr) => {{
        snafu::ensure!(
            $compare,
            common_query::error::InvalidFuncArgsSnafu {
                err_msg: "Argument was outside of acceptable range "
            }
        );
        Ok($coerce)
    }};
}

pub(super) use ensure_and_coerce;

pub(super) fn parse_wkt(s: &str) -> Result<Geometry> {
    Geometry::try_from_wkt_str(s)
        .map_err(|e| {
            BoxedError::new(PlainError::new(
                format!("Fail to parse WKT: {}", e),
                StatusCode::EngineExecuteQuery,
            ))
        })
        .context(error::ExecuteSnafu)
}
