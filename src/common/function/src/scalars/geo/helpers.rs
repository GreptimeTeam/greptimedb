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

macro_rules! ensure_columns_len {
    ($columns:ident) => {
        ensure!(
            $columns.windows(2).all(|c| c[0].len() == c[1].len()),
            InvalidFuncArgsSnafu {
                err_msg: "The length of input columns are in different size"
            }
        )
    };
    ($column_a:ident, $column_b:ident, $($column_n:ident),*) => {
        ensure!(
            {
                let mut result = $column_a.len() == $column_b.len();
                $(
                result = result && ($column_a.len() == $column_n.len());
                )*
                result
            }
            InvalidFuncArgsSnafu {
                err_msg: "The length of input columns are in different size"
            }
        )
    };
}

pub(super) use ensure_columns_len;

macro_rules! ensure_columns_n {
    ($columns:ident, $n:literal) => {
        ensure!(
            $columns.len() == $n,
            InvalidFuncArgsSnafu {
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
