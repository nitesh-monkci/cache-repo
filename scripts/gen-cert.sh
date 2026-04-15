#!/usr/bin/env bash
# Generates a self-signed TLS certificate for local / VM testing.
# For production, replace with a real certificate from your CA.

set -euo pipefail

OUT_DIR="${1:-/etc/cache-server}"
DAYS="${2:-365}"

mkdir -p "$OUT_DIR"

openssl req -x509 \
  -newkey rsa:4096 \
  -keyout "$OUT_DIR/server.key" \
  -out    "$OUT_DIR/server.crt" \
  -days   "$DAYS" \
  -nodes \
  -subj   "/CN=cache-server"

chmod 600 "$OUT_DIR/server.key"
chmod 644 "$OUT_DIR/server.crt"

echo "Certificate written to $OUT_DIR/server.crt"
echo "Private key  written to $OUT_DIR/server.key"
