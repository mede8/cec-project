#!bin/bash
set -e
cd "$(dirname "$0")"
source ./producer.env

docker run --rm -d --name experiment-producer \
    -v "$PWD/../auth:/experiment-producer/auth:ro" \
    -v "$PWD/sqlx.env:/experiment-producer/.env" \
    -v "$PWD/config.json:/config.json" \
    dclandau/cec-experiment-producer \
    --secret-key "$SECRET_KEY" --brokers "$BROKERS" --topic "$TOPIC" --config-file config.json

echo "producer started"