#!/usr/bin/env bash
# Parties 4-5 : producteur temps reel (20 evenements/s). Ctrl+C pour arreter.
# Option : un evenement 15 min en retard (et un 5 min) toutes les 60 s pour illustrer le watermark.
set -euo pipefail
cd "$(dirname "$0")/.."
RATE="${RATE:-20}"
docker compose exec jupyter python3 producer/producer.py --mode kafka --rate "$RATE" \
  --late-demo-every "${LATE_DEMO_EVERY:-60}" --log logs/producer.log "$@"
