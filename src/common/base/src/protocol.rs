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

use std::fmt::{Display, Formatter};

/// The protocol through which a query is received.
#[derive(Debug, PartialEq, Default, Clone, Copy, strum::FromRepr)]
#[repr(u8)]
pub enum Channel {
    #[default]
    Unknown = 0,

    Mysql = 1,
    Postgres = 2,
    HttpSql = 3,
    Prometheus = 4,
    Otlp = 5,
    Grpc = 6,
    Influx = 7,
    Opentsdb = 8,
    Loki = 9,
    Elasticsearch = 10,
    Jaeger = 11,
    Log = 12,
    Promql = 13,
    Splunk = 14,
}

impl From<u32> for Channel {
    fn from(value: u32) -> Self {
        u8::try_from(value)
            .ok()
            .and_then(Self::from_repr)
            .unwrap_or_default()
    }
}

impl Display for Channel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl AsRef<str> for Channel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Unknown => "unknown",
            Self::Mysql => "mysql",
            Self::Postgres => "postgres",
            Self::HttpSql => "httpsql",
            Self::Prometheus => "prometheus",
            Self::Otlp => "otlp",
            Self::Grpc => "grpc",
            Self::Influx => "influx",
            Self::Opentsdb => "opentsdb",
            Self::Loki => "loki",
            Self::Elasticsearch => "elasticsearch",
            Self::Jaeger => "jaeger",
            Self::Log => "log",
            Self::Promql => "promql",
            Self::Splunk => "splunk",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Channel;

    #[test]
    fn test_channel_name() {
        let expected = [
            (1, "mysql"),
            (2, "postgres"),
            (3, "httpsql"),
            (4, "prometheus"),
            (5, "otlp"),
            (6, "grpc"),
            (7, "influx"),
            (8, "opentsdb"),
            (9, "loki"),
            (10, "elasticsearch"),
            (11, "jaeger"),
            (12, "log"),
            (13, "promql"),
            (14, "splunk"),
        ];

        for (value, name) in expected {
            assert_eq!(name, Channel::from(value).as_ref());
        }
        assert_eq!("unknown", Channel::from(0).as_ref());
        assert_eq!("unknown", Channel::from(15).as_ref());
    }
}
