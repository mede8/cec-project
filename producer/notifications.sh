#!/bin/bash
# for running the notifications-service locally
set -e
cd "$(dirname, "$0")"
source ./producer.env

docker run --rm -d --name notifications-service \
    -p 3000:3000 \
    -v "$PWD/sqlx.env:/notifications-service/.env:ro" \
    dclandau/cec-notifications-service \
    --secret-key "$SECRET_KEY"

echo "notifications-service on :3000"
