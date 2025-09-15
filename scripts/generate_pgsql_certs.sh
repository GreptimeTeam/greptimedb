#!/usr/bin/env bash
set -euo pipefail

CERT_DIR="${1:-$(dirname "$0")/../tests-integration/fixtures/pgsql-certs}"
DAYS="${2:-365}"

rm -rf "${CERT_DIR}"
mkdir -p "${CERT_DIR}"
cd "${CERT_DIR}"

echo "Generating CA certificate..."
openssl req -new -x509 -days "${DAYS}" -nodes -text \
  -out root.crt -keyout root.key \
  -subj "/CN=PostgresRootCA"


echo "Generating server certificate..."
openssl req -new -nodes -text \
  -out server.csr -keyout server.key \
  -subj "/CN=postgres"

openssl x509 -req -in server.csr -text -days "${DAYS}" \
  -CA root.crt -CAkey root.key -CAcreateserial \
  -out server.crt \
  -extensions v3_req -extfile <(printf "[v3_req]\nsubjectAltName=DNS:localhost,IP:127.0.0.1")

echo "Generating client certificate..."
# Make sure the client certificate is for the greptimedb user
openssl req -new -nodes -text \
  -out client.csr -keyout client.key \
  -subj "/CN=greptimedb"

openssl x509 -req -in client.csr -CA root.crt -CAkey root.key -CAcreateserial \
-out client.crt -days 365 -extensions v3_req -extfile <(printf "[v3_req]\nsubjectAltName=DNS:localhost")

rm -f *.csr

echo "TLS certificates generated successfully in ${CERT_DIR}"

chmod 600 root.key
chmod 600 client.key
chmod 600 server.key
