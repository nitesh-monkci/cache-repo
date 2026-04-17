#!/bin/bash
# Run once at VM boot to resolve the fake Azure hostname to localhost.
# Required when CACHE_AZURE_BLOB_HOST=monkcicache.blob.core.windows.net.
# The GitHub Actions toolkit checks if the download URL hostname ends with
# ".blob.core.windows.net" and activates its Azure SDK parallel-download
# path (8 workers, Range headers) when it does.

AZURE_HOST="${CACHE_AZURE_BLOB_HOST:-monkcicache.blob.core.windows.net}"

if ! grep -q "$AZURE_HOST" /etc/hosts; then
    echo "127.0.0.1  $AZURE_HOST" >> /etc/hosts
    echo "Added $AZURE_HOST -> 127.0.0.1 to /etc/hosts"
else
    echo "$AZURE_HOST already present in /etc/hosts"
fi
