// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

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

// This file is copied from https://github.com/tikv/raft-engine/blob/8dd2a39f359ff16f5295f35343f626e0c10132fa/src/util.rs

use std::fmt::{self, Debug, Display, Write};
use std::ops::{Div, Mul};
use std::str::FromStr;

use serde::de::{Unexpected, Visitor};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

const UNIT: u64 = 1;

const BINARY_DATA_MAGNITUDE: u64 = 1024;
pub const B: u64 = UNIT;
pub const KIB: u64 = B * BINARY_DATA_MAGNITUDE;
pub const MIB: u64 = KIB * BINARY_DATA_MAGNITUDE;
pub const GIB: u64 = MIB * BINARY_DATA_MAGNITUDE;
pub const TIB: u64 = GIB * BINARY_DATA_MAGNITUDE;
pub const PIB: u64 = TIB * BINARY_DATA_MAGNITUDE;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct ReadableSize(pub u64);

impl ReadableSize {
    pub const fn kb(count: u64) -> ReadableSize {
        ReadableSize(count * KIB)
    }

    pub const fn mb(count: u64) -> ReadableSize {
        ReadableSize(count * MIB)
    }

    pub const fn gb(count: u64) -> ReadableSize {
        ReadableSize(count * GIB)
    }

    pub const fn as_mb(self) -> u64 {
        self.0 / MIB
    }

    pub const fn as_bytes(self) -> u64 {
        self.0
    }
}

impl Div<u64> for ReadableSize {
    type Output = ReadableSize;

    fn div(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 / rhs)
    }
}

impl Div<ReadableSize> for ReadableSize {
    type Output = u64;

    fn div(self, rhs: ReadableSize) -> u64 {
        self.0 / rhs.0
    }
}

impl Mul<u64> for ReadableSize {
    type Output = ReadableSize;

    fn mul(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 * rhs)
    }
}

impl Serialize for ReadableSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let size = self.0;
        let mut buffer = String::new();
        if size == 0 {
            write!(buffer, "{}KiB", size).unwrap();
        } else if size % PIB == 0 {
            write!(buffer, "{}PiB", size / PIB).unwrap();
        } else if size % TIB == 0 {
            write!(buffer, "{}TiB", size / TIB).unwrap();
        } else if size % GIB as u64 == 0 {
            write!(buffer, "{}GiB", size / GIB).unwrap();
        } else if size % MIB as u64 == 0 {
            write!(buffer, "{}MiB", size / MIB).unwrap();
        } else if size % KIB as u64 == 0 {
            write!(buffer, "{}KiB", size / KIB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl FromStr for ReadableSize {
    type Err = String;

    // This method parses value in binary unit.
    fn from_str(s: &str) -> Result<ReadableSize, String> {
        let size_str = s.trim();
        if size_str.is_empty() {
            return Err(format!("{:?} is not a valid size.", s));
        }

        if !size_str.is_ascii() {
            return Err(format!("ASCII string is expected, but got {:?}", s));
        }

        // size: digits and '.' as decimal separator
        let size_len = size_str
            .to_string()
            .chars()
            .take_while(|c| char::is_ascii_digit(c) || ['.', 'e', 'E', '-', '+'].contains(c))
            .count();

        // unit: alphabetic characters
        let (size, unit) = size_str.split_at(size_len);

        let unit = match unit.trim() {
            "K" | "KB" | "KiB" => KIB,
            "M" | "MB" | "MiB" => MIB,
            "G" | "GB" | "GiB" => GIB,
            "T" | "TB" | "TiB" => TIB,
            "P" | "PB" | "PiB" => PIB,
            "B" | "" => B,
            _ => {
                return Err(format!(
                    "only B, KB, KiB, MB, MiB, GB, GiB, TB, TiB, PB, and PiB are supported: {:?}",
                    s
                ));
            }
        };

        match size.parse::<f64>() {
            Ok(n) => Ok(ReadableSize((n * unit as f64) as u64)),
            Err(_) => Err(format!("invalid size string: {:?}", s)),
        }
    }
}

impl Debug for ReadableSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for ReadableSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 >= PIB {
            write!(f, "{:.1}PiB", self.0 as f64 / PIB as f64)
        } else if self.0 >= TIB {
            write!(f, "{:.1}TiB", self.0 as f64 / TIB as f64)
        } else if self.0 >= GIB {
            write!(f, "{:.1}GiB", self.0 as f64 / GIB as f64)
        } else if self.0 >= MIB {
            write!(f, "{:.1}MiB", self.0 as f64 / MIB as f64)
        } else if self.0 >= KIB {
            write!(f, "{:.1}KiB", self.0 as f64 / KIB as f64)
        } else {
            write!(f, "{}B", self.0)
        }
    }
}

impl<'de> Deserialize<'de> for ReadableSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SizeVisitor;

        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = ReadableSize;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid size")
            }

            fn visit_i64<E>(self, size: i64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                if size >= 0 {
                    self.visit_u64(size as u64)
                } else {
                    Err(E::invalid_value(Unexpected::Signed(size), &self))
                }
            }

            fn visit_u64<E>(self, size: u64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                Ok(ReadableSize(size))
            }

            fn visit_str<E>(self, size_str: &str) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                size_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_readable_size() {
        let s = ReadableSize::kb(2);
        assert_eq!(s.0, 2048);
        assert_eq!(s.as_mb(), 0);
        let s = ReadableSize::mb(2);
        assert_eq!(s.0, 2 * 1024 * 1024);
        assert_eq!(s.as_mb(), 2);
        let s = ReadableSize::gb(2);
        assert_eq!(s.0, 2 * 1024 * 1024 * 1024);
        assert_eq!(s.as_mb(), 2048);

        assert_eq!((ReadableSize::mb(2) / 2).0, MIB);
        assert_eq!((ReadableSize::mb(1) / 2).0, 512 * KIB);
        assert_eq!(ReadableSize::mb(2) / ReadableSize::kb(1), 2048);
    }

    #[test]
    fn test_parse_readable_size() {
        #[derive(Serialize, Deserialize)]
        struct SizeHolder {
            s: ReadableSize,
        }

        let legal_cases = vec![
            (0, "0KiB"),
            (2 * KIB, "2KiB"),
            (4 * MIB, "4MiB"),
            (5 * GIB, "5GiB"),
            (7 * TIB, "7TiB"),
            (11 * PIB, "11PiB"),
        ];
        for (size, exp) in legal_cases {
            let c = SizeHolder {
                s: ReadableSize(size),
            };
            let res_str = toml::to_string(&c).unwrap();
            let exp_str = format!("s = {:?}\n", exp);
            assert_eq!(res_str, exp_str);
            let res_size: SizeHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(res_size.s.0, size);
        }

        let c = SizeHolder {
            s: ReadableSize(512),
        };
        let res_str = toml::to_string(&c).unwrap();
        assert_eq!(res_str, "s = 512\n");
        let res_size: SizeHolder = toml::from_str(&res_str).unwrap();
        assert_eq!(res_size.s.0, c.s.0);

        let decode_cases = vec![
            (" 0.5 PB", PIB / 2),
            ("0.5 TB", TIB / 2),
            ("0.5GB ", GIB / 2),
            ("0.5MB", MIB / 2),
            ("0.5KB", KIB / 2),
            ("0.5P", PIB / 2),
            ("0.5T", TIB / 2),
            ("0.5G", GIB / 2),
            ("0.5M", MIB / 2),
            ("0.5K", KIB / 2),
            ("23", 23),
            ("1", 1),
            ("1024B", KIB),
            // units with binary prefixes
            (" 0.5 PiB", PIB / 2),
            ("1PiB", PIB),
            ("0.5 TiB", TIB / 2),
            ("2 TiB", TIB * 2),
            ("0.5GiB ", GIB / 2),
            ("787GiB ", GIB * 787),
            ("0.5MiB", MIB / 2),
            ("3MiB", MIB * 3),
            ("0.5KiB", KIB / 2),
            ("1 KiB", KIB),
            // scientific notation
            ("0.5e6 B", B * 500000),
            ("0.5E6 B", B * 500000),
            ("1e6B", B * 1000000),
            ("8E6B", B * 8000000),
            ("8e7", B * 80000000),
            ("1e-1MB", MIB / 10),
            ("1e+1MB", MIB * 10),
            ("0e+10MB", 0),
        ];
        for (src, exp) in decode_cases {
            let src = format!("s = {:?}", src);
            let res: SizeHolder = toml::from_str(&src).unwrap();
            assert_eq!(res.s.0, exp);
        }

        let illegal_cases = vec![
            "0.5kb", "0.5kB", "0.5Kb", "0.5k", "0.5g", "b", "gb", "1b", "B", "1K24B", " 5_KB",
            "4B7", "5M_",
        ];
        for src in illegal_cases {
            let src_str = format!("s = {:?}", src);
            assert!(toml::from_str::<SizeHolder>(&src_str).is_err(), "{}", src);
        }
    }
}
