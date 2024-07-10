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

#[macro_export]
macro_rules! memory_table_cols{
    ([$($colname:ident),*], $t:expr) => {
        let t = &$t;
        $(
            let mut $colname = Vec::with_capacity(t.len());
        )*
        paste::paste!{
            for &($([<r_ $colname>]),*) in t {
                    $(
                        $colname.push([<r_ $colname>]);
                    )*
            }
        }
    };
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_memory_table_columns() {
        memory_table_cols!(
            [oid, typname, typlen],
            [
                (1, "String", -1),
                (2, "Binary", -1),
                (3, "Time", 8),
                (4, "Datetime", 8)
            ]
        );
        assert_eq!(&oid[..], &[1, 2, 3, 4]);
        assert_eq!(&typname[..], &["String", "Binary", "Time", "Datetime"]);
        assert_eq!(&typlen[..], &[-1, -1, 8, 8]);
    }
}
