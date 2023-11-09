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

use common_grpc::channel_manager::{ChannelConfig, ChannelManager, ClientTlsOption};

#[tokio::test]
async fn test_mtls_config() {
    // test no config
    let config = ChannelConfig::new();
    let re = ChannelManager::with_tls_config(config);
    assert!(re.is_err());

    // test wrong file
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        server_ca_cert_path: "tests/tls/wrong_server.cert.pem".to_string(),
        client_cert_path: "tests/tls/wrong_client.cert.pem".to_string(),
        client_key_path: "tests/tls/wrong_client.key.pem".to_string(),
    });

    let re = ChannelManager::with_tls_config(config);
    assert!(re.is_err());

    // test corrupted file content
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        server_ca_cert_path: "tests/tls/server.cert.pem".to_string(),
        client_cert_path: "tests/tls/client.cert.pem".to_string(),
        client_key_path: "tests/tls/corrupted".to_string(),
    });

    let re = ChannelManager::with_tls_config(config).unwrap();
    let re = re.get("127.0.0.1:0");
    assert!(re.is_err());

    // success
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        server_ca_cert_path: "tests/tls/server.cert.pem".to_string(),
        client_cert_path: "tests/tls/client.cert.pem".to_string(),
        client_key_path: "tests/tls/client.key.pem".to_string(),
    });

    let re = ChannelManager::with_tls_config(config).unwrap();
    let re = re.get("127.0.0.1:0");
    let _ = re.unwrap();
}
