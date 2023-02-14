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

    let re = ChannelManager::with_tls_config(config);
    assert!(re.is_ok());
    let re = re.unwrap().get("127.0.0.1:0");
    assert!(re.is_err());

    // success
    let config = ChannelConfig::new().client_tls_config(ClientTlsOption {
        server_ca_cert_path: "tests/tls/server.cert.pem".to_string(),
        client_cert_path: "tests/tls/client.cert.pem".to_string(),
        client_key_path: "tests/tls/client.key.pem".to_string(),
    });

    let re = ChannelManager::with_tls_config(config);
    assert!(re.is_ok());
    let re = re.unwrap().get("127.0.0.1:0");
    assert!(re.is_ok());
}
