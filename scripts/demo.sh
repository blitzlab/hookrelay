#!/usr/bin/env bash
# Registers the demo receiver, publishes events, and prints the delivery summary.
# Run after `docker compose up --build`.
set -euo pipefail

API=${API:-http://localhost:8000}
EVENTS=${EVENTS:-25}

echo "Registering endpoint -> http://receiver:9000/webhooks"
curl -sf -X POST "$API/api/endpoints/" \
  -H 'Content-Type: application/json' \
  -d '{"url": "http://receiver:9000/webhooks", "event_types": ["order.created"]}' >/dev/null

echo "Publishing $EVENTS events (each with an Idempotency-Key)"
for i in $(seq 1 "$EVENTS"); do
  curl -sf -o /dev/null -X POST "$API/api/events/" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: demo-order-$i" \
    -d "{\"event_type\": \"order.created\", \"payload\": {\"order_id\": $i}}"
done

echo "Waiting for retries to settle..."
sleep 20

for status in succeeded pending in_flight dead; do
  count=$(curl -sf "$API/api/deliveries/?status=$status&limit=1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])')
  printf '%-10s %s\n' "$status" "$count"
done
echo "Receiver stats: $(curl -sf http://localhost:9000/)"
