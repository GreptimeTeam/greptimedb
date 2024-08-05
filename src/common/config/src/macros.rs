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
macro_rules! define_mem_size_enum {
    ($(#[$meta:meta])*
     $name:ident, $factor: expr, $default: expr, $max: expr) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            /// Automatically determine the threshold based on default value or sys mem
            #[default]
            Auto,
            /// Unlimited size,
            Unlimited,
            /// Fixed size,
            #[serde(untagged)]
            Size(common_base::readable_size::ReadableSize),
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match self {
                    $name::Auto => write!(f, "auto"),
                    $name::Unlimited => write!(f, "unlimited"),
                    $name::Size(size) => write!(f, "{}", size),
                }
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S:  serde::Serializer,
            {
                match self.as_bytes_opt() {
                    Some(size) => ReadableSize(size).to_string().serialize(serializer),
                    None => serializer.serialize_str("unlimited"),
                }
            }
        }

        #[allow(dead_code)]
        impl $name {
            fn default_size() -> u64 {
                let size = if let Some(sys_memory) = $crate::utils::get_sys_total_memory() {
                    (sys_memory / $factor).as_bytes()
                } else {
                    $default.as_bytes()
                };

                if let Some(max) = $max {
                    std::cmp::min(size, max.as_bytes())
                } else {
                    size
                }
            }

            pub fn as_bytes(&self) -> u64 {
                match self {
                    $name::Auto => {
                        $name::default_size()
                    }
                    $name::Unlimited => $max
                        .map(|s: ReadableSize| s.as_bytes())
                        .unwrap_or(u64::MAX),
                    $name::Size(size) => size.as_bytes(),
                }
            }

            pub fn as_bytes_opt(&self) -> Option<u64> {
                match self {
                    $name::Auto => {
                        Some($name::default_size())
                    }
                    $name::Unlimited => $max.map(|s: ReadableSize| s.as_bytes()),
                    $name::Size(size) => Some(size.as_bytes()),
                }
            }
        }

        impl From<$name> for u64 {
            fn from(src: $name) -> u64 {
                src.as_bytes()
            }
        }

        impl From<$name> for Option<u64> {
            fn from(src: $name) -> Option<u64> {
                src.as_bytes_opt()
            }
        }
        impl From<$name> for usize {
            fn from(src: $name) -> usize {
                src.as_bytes() as _
            }
        }

        impl From<ReadableSize> for $name {
            fn from(size: ReadableSize) -> $name {
                $name::Size(size)
            }
        }

        impl From<$name> for Option<usize> {
            fn from(src: $name) -> Option<usize> {
                src.as_bytes_opt().map(|s| s as _)
            }
        }

    };
}

#[cfg(test)]
mod tests {
    use common_base::readable_size::ReadableSize;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_define_size_enum() {
        define_mem_size_enum!(TestSize, 16, ReadableSize::mb(32), None::<ReadableSize>);

        let size = TestSize::default();
        assert_eq!(size, TestSize::Auto);
        assert_eq!("auto", size.to_string());

        assert!(size.as_bytes() > 0);
        assert!(size.as_bytes_opt().is_some());

        let size = TestSize::Unlimited;
        assert!(size.as_bytes() > 0);
        assert!(size.as_bytes_opt().is_none());
        assert_eq!("unlimited", size.to_string());

        let size = TestSize::Size(ReadableSize::mb(16));
        assert_eq!(size.as_bytes(), ReadableSize::mb(16).as_bytes());
        assert_eq!(
            size.as_bytes_opt().unwrap(),
            ReadableSize::mb(16).as_bytes()
        );
        assert_eq!("16.0MiB", size.to_string());

        let size: u64 = size.into();
        assert_eq!(size, ReadableSize::mb(16).as_bytes());
    }
}
