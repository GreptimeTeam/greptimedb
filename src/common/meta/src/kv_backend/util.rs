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

/// Removes sensitive information like passwords from connection strings.
///
/// This function sanitizes connection strings by removing credentials:
/// - For URL format (mysql://user:password@host:port/db): Removes everything before '@'
/// - For parameter format (host=localhost password=secret): Removes the password parameter
/// - For URL format without credentials (mysql://host:port/db): Removes the protocol prefix
///
/// # Arguments
///
/// * `conn_str` - The connection string to sanitize
///
/// # Returns
///
/// A sanitized version of the connection string with sensitive information removed
pub fn sanitize_connection_string(conn_str: &str) -> String {
    // Case 1: URL format with credentials (mysql://user:password@host:port/db)
    // Extract everything after the '@' symbol
    if let Some(at_pos) = conn_str.find('@') {
        return conn_str[at_pos + 1..].to_string();
    }

    // Case 2: Parameter format with password (host=localhost password=secret dbname=mydb)
    // Filter out any parameter that starts with "password="
    if conn_str.contains("password=") {
        return conn_str
            .split_whitespace()
            .filter(|param| !param.starts_with("password="))
            .collect::<Vec<_>>()
            .join(" ");
    }

    // Case 3: URL format without credentials (mysql://host:port/db)
    // Extract everything after the protocol prefix
    if let Some(host_part) = conn_str.split("://").nth(1) {
        return host_part.to_string();
    }

    // Case 4: Already sanitized or unknown format
    // Return as is
    conn_str.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_connection_string() {
        // Test URL format with username/password
        let conn_str = "mysql://user:password123@localhost:3306/db";
        assert_eq!(sanitize_connection_string(conn_str), "localhost:3306/db");

        // Test URL format without credentials
        let conn_str = "mysql://localhost:3306/db";
        assert_eq!(sanitize_connection_string(conn_str), "localhost:3306/db");

        // Test parameter format with password
        let conn_str = "host=localhost port=5432 user=postgres password=secret dbname=mydb";
        assert_eq!(
            sanitize_connection_string(conn_str),
            "host=localhost port=5432 user=postgres dbname=mydb"
        );

        // Test parameter format without password
        let conn_str = "host=localhost port=5432 user=postgres dbname=mydb";
        assert_eq!(
            sanitize_connection_string(conn_str),
            "host=localhost port=5432 user=postgres dbname=mydb"
        );
    }
}
