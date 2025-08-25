#!/bin/bash

# Generate TLS certificates for etcd testing
# This script creates certificates for TLS-enabled etcd in testing environments

set -euo pipefail

CERT_DIR="${1:-$(dirname "$0")/../tests-integration/fixtures/etcd-tls-certs}"
DAYS="${2:-365}"

echo "Generating TLS certificates for etcd in ${CERT_DIR}..."

mkdir -p "${CERT_DIR}"
cd "${CERT_DIR}"

echo "Generating CA private key..."
openssl genrsa -out ca-key.pem 2048

echo "Generating CA certificate..."
openssl req -new -x509 -key ca-key.pem -out ca.crt -days "${DAYS}" \
  -subj "/C=US/ST=CA/L=SF/O=Greptime/CN=etcd-ca"

# Create server certificate config with Subject Alternative Names
echo "Creating server certificate configuration..."
cat > server.conf << 'EOF'
[req]
distinguished_name = req
[v3_req]
basicConstraints = CA:FALSE
keyUsage = keyEncipherment, dataEncipherment
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
DNS.2 = etcd-tls
DNS.3 = 127.0.0.1
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

echo "Generating server private key..."
openssl genrsa -out server-key.pem 2048

echo "Generating server certificate signing request..."
openssl req -new -key server-key.pem -out server.csr \
  -subj "/CN=etcd-tls"

echo "Generating server certificate..."
openssl x509 -req -in server.csr -CA ca.crt \
  -CAkey ca-key.pem -CAcreateserial -out server.crt \
  -days "${DAYS}" -extensions v3_req -extfile server.conf

echo "Generating client private key..."
openssl genrsa -out client-key.pem 2048

echo "Generating client certificate signing request..."
openssl req -new -key client-key.pem -out client.csr \
  -subj "/CN=etcd-client"

echo "Generating client certificate..."
openssl x509 -req -in client.csr -CA ca.crt \
  -CAkey ca-key.pem -CAcreateserial -out client.crt \
  -days "${DAYS}"

echo "Setting proper file permissions..."
chmod 644 ca.crt server.crt client.crt
chmod 600 ca-key.pem server-key.pem client-key.pem

# Clean up intermediate files
rm -f server.csr client.csr server.conf

echo "TLS certificates generated successfully in ${CERT_DIR}"
