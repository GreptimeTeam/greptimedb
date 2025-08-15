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

// To make Example succeed, set up TLS certificates:
//
// 1. Generate certificates:
//    mkdir -p /tmp/etcd-certs
//
//    # CA certificate
//    openssl genrsa -out /tmp/etcd-certs/ca-key.pem 2048
//    openssl req -new -x509 -key /tmp/etcd-certs/ca-key.pem -out /tmp/etcd-certs/ca.crt -days 365 \
//      -subj "/C=US/ST=CA/L=SF/O=Greptime/CN=etcd-ca"
//
//    # Server certificate with SAN
//    cat > /tmp/etcd-certs/server.conf << 'EOF'
//    [req]
//    distinguished_name = req
//    [v3_req]
//    basicConstraints = CA:FALSE
//    keyUsage = keyEncipherment, dataEncipherment
//    subjectAltName = @alt_names
//    [alt_names]
//    DNS.1 = localhost
//    IP.1 = 127.0.0.1
//    EOF
//
//    openssl genrsa -out /tmp/etcd-certs/server-key.pem 2048
//    openssl req -new -key /tmp/etcd-certs/server-key.pem -out /tmp/etcd-certs/server.csr \
//      -subj "/CN=127.0.0.1"
//    openssl x509 -req -in /tmp/etcd-certs/server.csr -CA /tmp/etcd-certs/ca.crt \
//      -CAkey /tmp/etcd-certs/ca-key.pem -CAcreateserial -out /tmp/etcd-certs/server.crt \
//      -days 365 -extensions v3_req -extfile /tmp/etcd-certs/server.conf
//
// 2. Start TLS-enabled etcd:
//    docker run -d --name etcd-tls -p 2379:2379 -v /tmp/etcd-certs:/certs \
//      quay.io/coreos/etcd:v3.5.9 etcd --name etcd-tls \
//      --listen-client-urls https://0.0.0.0:2379 \
//      --advertise-client-urls https://127.0.0.1:2379 \
//      --cert-file /certs/server.crt --key-file /certs/server-key.pem
//
//
// 3. Run example: cargo run --package meta-srv --example etcd

use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::rpc::store::{PutRequest, RangeRequest};
use meta_srv::bootstrap::create_etcd_client_with_tls;
use servers::tls::{TlsMode, TlsOption};
use tracing::{info, subscriber};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    subscriber::set_global_default(FmtSubscriber::builder().finish()).unwrap();
    // TLS required with certificates
    let tls_required = TlsOption {
        mode: TlsMode::Require,
        cert_path: "".to_string(),
        key_path: "".to_string(),
        ca_cert_path: "/tmp/etcd-certs/ca.crt".to_string(),
        watch: false,
    };
    let result =
        create_etcd_client_with_tls(&["https://127.0.0.1:2379".to_string()], Some(&tls_required))
            .await;
    match result {
        Ok(client) => {
            info!("Connected to etcd with TLS enabled");
            demo_operations(&client).await;
        }
        Err(e) => {
            info!("Failed to connect with TLS required: {}", e);
        }
    }
}

async fn demo_operations(client: &etcd_client::Client) {
    // Test direct etcd client operations first
    let mut kv_client = client.kv_client();

    // Direct etcd PUT operation
    match kv_client.put("demo_key", "demo_value", None).await {
        Ok(response) => {
            info!(
                "Direct etcd PUT successful: {} ops",
                response.header().unwrap().revision()
            );
        }
        Err(e) => {
            info!("Direct etcd PUT failed: {}", e);
            return; // Exit early if basic etcd operations fail
        }
    }

    // Direct etcd GET operation
    match kv_client.get("demo_key", None).await {
        Ok(response) => {
            let kvs = response.kvs();
            if kvs.is_empty() {
                info!("GET returned no values");
            } else {
                let value = String::from_utf8_lossy(kvs[0].value());
                info!("Direct etcd GET successful: key=demo_key, value={}", value);
            }
        }
        Err(e) => {
            info!("Direct etcd GET failed: {}", e);
            return;
        }
    }

    // If direct operations work, test EtcdStore wrapper
    let kv_store = EtcdStore::with_etcd_client(client.clone(), 128);

    let put_req = PutRequest {
        key: b"store_key".to_vec(),
        value: b"store_value".to_vec(),
        prev_kv: true,
    };

    match kv_store.put(put_req).await {
        Ok(response) => {
            info!("EtcdStore PUT successful: {:?}", response);
        }
        Err(e) => {
            info!("EtcdStore PUT failed: {}", e);
        }
    }

    let get_req = RangeRequest {
        key: b"store_key".to_vec(),
        ..Default::default()
    };

    match kv_store.range(get_req).await {
        Ok(response) => {
            info!("EtcdStore GET successful: {} keys", response.kvs.len());
        }
        Err(e) => {
            info!("EtcdStore GET failed: {}", e);
        }
    }
}
