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

use common_telemetry::warn;

/// Resolves server address for meta registration.
/// If `server_addr` is present, prefer to use it, `bind_addr` otherwise.
pub fn resolve_addr(bind_addr: &str, server_addr: Option<&str>) -> String {
    match server_addr {
        Some(server_addr) => {
            // it has port configured
            if server_addr.contains(':') {
                server_addr.to_string()
            } else {
                // otherwise, resolve port from bind_addr
                // should be safe to unwrap here because bind_addr is already validated
                let port = bind_addr.split(':').nth(1).unwrap();
                format!("{server_addr}:{port}")
            }
        }
        None => {
            warn!("hostname not set, using bind_addr: {bind_addr} instead.");
            bind_addr.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_resolve_addr() {
        assert_eq!(
            "tomcat:3001",
            super::resolve_addr("127.0.0.1:3001", Some("tomcat"))
        );

        assert_eq!(
            "tomcat:3002",
            super::resolve_addr("127.0.0.1:3001", Some("tomcat:3002"))
        );

        assert_eq!(
            "127.0.0.1:3001",
            super::resolve_addr("127.0.0.1:3001", None)
        );
    }
}
