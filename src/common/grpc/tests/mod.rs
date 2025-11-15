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
    ChannelConfig, ChannelManager, ClientTlsOption, load_client_tls_config,
    maybe_watch_client_tls_config,
};

#[tokio::test]
async fn test_mtls_config() {
    // test no config
    let config = ChannelConfig::new();
    let re = load_client_tls_config(config.client_tls.clone());
    assert!(re.is_ok());
    assert!(re.unwrap().is_none());

    // test wrong file
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        enabled: true,
        server_ca_cert_path: Some("tests/tls/wrong_ca.pem".to_string()),
        client_cert_path: Some("tests/tls/wrong_client.pem".to_string()),
        client_key_path: Some("tests/tls/wrong_client.key".to_string()),
        watch: false,
    });

    let re = load_client_tls_config(config.client_tls.clone());
    assert!(re.is_err());

    // test corrupted file content
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        enabled: true,
        server_ca_cert_path: Some("tests/tls/ca.pem".to_string()),
        client_cert_path: Some("tests/tls/client.pem".to_string()),
        client_key_path: Some("tests/tls/corrupted".to_string()),
        watch: false,
    });

    let tls_config = load_client_tls_config(config.client_tls.clone()).unwrap();
    let re = ChannelManager::with_config(config, tls_config);

    let re = re.get("127.0.0.1:0");
    assert!(re.is_err());

    // success
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        enabled: true,
        server_ca_cert_path: Some("tests/tls/ca.pem".to_string()),
        client_cert_path: Some("tests/tls/client.pem".to_string()),
        client_key_path: Some("tests/tls/client.key".to_string()),
        watch: false,
    });

    let tls_config = load_client_tls_config(config.client_tls.clone()).unwrap();
    let re = ChannelManager::with_config(config, tls_config);
    let re = re.get("127.0.0.1:0");
    let _ = re.unwrap();
}

#[tokio::test]
async fn test_reloadable_client_tls_config() {
    common_telemetry::init_default_ut_logging();

    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("client.pem");
    let key_path = dir.path().join("client.key");

    std::fs::copy("tests/tls/client.pem", &cert_path).expect("failed to copy cert to tmpdir");
    std::fs::copy("tests/tls/client.key", &key_path).expect("failed to copy key to tmpdir");

    assert!(std::fs::exists(&cert_path).unwrap());
    assert!(std::fs::exists(&key_path).unwrap());

    let client_tls_option = ClientTlsOption {
        enabled: true,
        server_ca_cert_path: Some("tests/tls/ca.pem".to_string()),
        client_cert_path: Some(
            cert_path
                .clone()
                .into_os_string()
                .into_string()
                .expect("failed to convert path to string"),
        ),
        client_key_path: Some(
            key_path
                .clone()
                .into_os_string()
                .into_string()
                .expect("failed to convert path to string"),
        ),
        watch: true,
    };

    let reloadable_config = load_client_tls_config(Some(client_tls_option))
        .expect("failed to load tls config")
        .expect("tls config should be present");

    let config = ChannelConfig::new();
    let manager = ChannelManager::with_config(config, Some(reloadable_config.clone()));

    maybe_watch_client_tls_config(reloadable_config.clone(), &manager)
        .expect("failed to watch client config");

    assert_eq!(0, reloadable_config.get_version());
    assert!(reloadable_config.get_client_config().is_some());

    // Create a channel to verify it gets cleared on reload
    let _ = manager.get("127.0.0.1:0").expect("failed to get channel");

    // Simulate file change by copying a different key file
    let tmp_file = key_path.with_extension("tmp");
    std::fs::copy("tests/tls/server.key", &tmp_file).expect("Failed to copy temp key file");
    std::fs::rename(&tmp_file, &key_path).expect("Failed to rename temp key file");

    const MAX_RETRIES: usize = 30;
    let mut retries = 0;
    let mut version_updated = false;

    while retries < MAX_RETRIES {
        if reloadable_config.get_version() > 0 {
            version_updated = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        retries += 1;
    }

    assert!(version_updated, "TLS config did not reload in time");
    assert!(reloadable_config.get_version() > 0);
    assert!(reloadable_config.get_client_config().is_some());
}

#[tokio::test]
async fn test_channel_manager_with_reloadable_tls() {
    common_telemetry::init_default_ut_logging();

    let client_tls_option = ClientTlsOption {
        enabled: true,
        server_ca_cert_path: Some("tests/tls/ca.pem".to_string()),
        client_cert_path: Some("tests/tls/client.pem".to_string()),
        client_key_path: Some("tests/tls/client.key".to_string()),
        watch: false,
    };

    let reloadable_config = load_client_tls_config(Some(client_tls_option))
        .expect("failed to load tls config")
        .expect("tls config should be present");

    let config = ChannelConfig::new();
    let manager = ChannelManager::with_config(config, Some(reloadable_config.clone()));

    // Test that we can get a channel
    let channel = manager.get("127.0.0.1:0");
    assert!(channel.is_ok());

    // Test that config is properly set
    assert_eq!(0, reloadable_config.get_version());
    assert!(reloadable_config.get_client_config().is_some());
}
