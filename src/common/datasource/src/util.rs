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

pub fn find_dir_and_filename(path: &str) -> (String, Option<String>) {
    if path.is_empty() {
        ("/".to_string(), None)
    } else if path.ends_with('/') {
        (path.to_string(), None)
    } else if let Some(idx) = path.rfind('/') {
        (
            path[..idx + 1].to_string(),
            Some(path[idx + 1..].to_string()),
        )
    } else {
        ("/".to_string(), Some(path.to_string()))
    }
}

#[cfg(test)]
mod tests {

    use url::Url;

    use super::*;

    #[test]
    fn test_parse_uri() {
        struct Test<'a> {
            uri: &'a str,
            expected_path: &'a str,
            expected_schema: &'a str,
        }

        let tests = [
            Test {
                uri: "s3://bucket/to/path/",
                expected_path: "/to/path/",
                expected_schema: "s3",
            },
            Test {
                uri: "fs:///to/path/",
                expected_path: "/to/path/",
                expected_schema: "fs",
            },
            Test {
                uri: "fs:///to/path/file",
                expected_path: "/to/path/file",
                expected_schema: "fs",
            },
        ];
        for test in tests {
            let parsed_uri = Url::parse(test.uri).unwrap();
            assert_eq!(parsed_uri.path(), test.expected_path);
            assert_eq!(parsed_uri.scheme(), test.expected_schema);
        }
    }

    #[cfg(not(windows))]
    #[test]
    fn test_parse_path_and_dir() {
        let parsed = Url::from_file_path("/to/path/file").unwrap();
        assert_eq!(parsed.path(), "/to/path/file");

        let parsed = Url::from_directory_path("/to/path/").unwrap();
        assert_eq!(parsed.path(), "/to/path/");
    }

    #[cfg(windows)]
    #[test]
    fn test_parse_path_and_dir() {
        let parsed = Url::from_file_path("C:\\to\\path\\file").unwrap();
        assert_eq!(parsed.path(), "/C:/to/path/file");

        let parsed = Url::from_directory_path("C:\\to\\path\\").unwrap();
        assert_eq!(parsed.path(), "/C:/to/path/");
    }

    #[test]
    fn test_find_dir_and_filename() {
        struct Test<'a> {
            path: &'a str,
            expected_dir: &'a str,
            expected_filename: Option<String>,
        }

        let tests = [
            Test {
                path: "to/path/",
                expected_dir: "to/path/",
                expected_filename: None,
            },
            Test {
                path: "to/path/filename",
                expected_dir: "to/path/",
                expected_filename: Some("filename".into()),
            },
            Test {
                path: "/to/path/filename",
                expected_dir: "/to/path/",
                expected_filename: Some("filename".into()),
            },
            Test {
                path: "/",
                expected_dir: "/",
                expected_filename: None,
            },
            Test {
                path: "filename",
                expected_dir: "/",
                expected_filename: Some("filename".into()),
            },
            Test {
                path: "",
                expected_dir: "/",
                expected_filename: None,
            },
        ];

        for test in tests {
            let (path, filename) = find_dir_and_filename(test.path);
            assert_eq!(test.expected_dir, path);
            assert_eq!(test.expected_filename, filename)
        }
    }
}
