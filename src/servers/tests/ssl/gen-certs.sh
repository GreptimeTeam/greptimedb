#!/usr/bin/env bash

# Create the self-signed CA certificate.
openssl req -x509 \
            -sha256 -days 356 \
            -nodes \
            -newkey rsa:2048 \
            -subj "/CN=greptime-ca" \
            -keyout root-ca.key -out root-ca.crt

# Create the server private key.
openssl genrsa -out server-rsa.key 2048

# Create the server certificate signing request.
openssl req -new -key server-rsa.key -out server.csr -config csr.conf

# Create the server certificate.
openssl x509 -req \
    -in server.csr \
    -CA root-ca.crt -CAkey root-ca.key \
    -CAcreateserial -out server.crt \
    -days 365 \
    -sha256 -extfile cert.conf

# Create private key of pkcs8 format from rsa key.
openssl pkcs8 -topk8 -inform PEM -in ./server-rsa.key -outform pem -nocrypt -out server-pkcs8.key
