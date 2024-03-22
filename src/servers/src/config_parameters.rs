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

// Refer to: https://www.postgresql.org/docs/current/config-setting.html#CONFIG-SETTING-NAMES-VALUES
#[macro_export]
macro_rules! define_config_enum_option {
    ($mod_name:ident, $name:ident, $($item:ident),+) => {
        pub mod $mod_name{
            pub const NAME: &str = stringify!($name);
            $(pub const $item: &str = stringify!($item);)+
        }
    };
}
