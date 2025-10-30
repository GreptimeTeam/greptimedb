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

use common_grpc::channel_manager::{
    ChannelConfig, ChannelManager, ClientTlsOption, load_tls_config,
};

#[tokio::test]
async fn test_mtls_config() {
    // test no config
    let config = ChannelConfig::new();
    let re = load_tls_config(config.client_tls.as_ref());
    assert!(re.is_ok());
    assert!(re.unwrap().is_none());

    // test wrong file
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        enabled: true,
        server_ca_cert_path: Some("tests/tls/wrong_ca.pem".to_string()),
        client_cert_path: Some("tests/tls/wrong_client.pem".to_string()),
        client_key_path: Some("tests/tls/wrong_client.key".to_string()),
    });

    let re = load_tls_config(config.client_tls.as_ref());
    assert!(re.is_err());

    // test corrupted file content
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        enabled: true,
        server_ca_cert_path: Some("tests/tls/ca.pem".to_string()),
        client_cert_path: Some("tests/tls/client.pem".to_string()),
        client_key_path: Some("tests/tls/corrupted".to_string()),
    });

    let tls_config = load_tls_config(config.client_tls.as_ref()).unwrap();
    let re = ChannelManager::with_config(config, tls_config);

    let re = re.get("127.0.0.1:0");
    assert!(re.is_err());

    // success
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        enabled: true,
        server_ca_cert_path: Some("tests/tls/ca.pem".to_string()),
        client_cert_path: Some("tests/tls/client.pem".to_string()),
        client_key_path: Some("tests/tls/client.key".to_string()),
    });

    let tls_config = load_tls_config(config.client_tls.as_ref()).unwrap();
    let re = ChannelManager::with_config(config, tls_config);
    let re = re.get("127.0.0.1:0");
    let _ = re.unwrap();
}
