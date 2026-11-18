#!/usr/bin/env bash
# Partie 2 : generation de l'historique (50 000 evenements) et copie dans lake/bronze/.
set -euo pipefail
cd "$(dirname "$0")/.."
N="${1:-50000}"
docker compose exec jupyter python3 producer/producer.py --mode file --n "$N"
docker compose run --rm mc cp /data/transactions_history.jsonl local/lake/bronze/history/transactions_history.jsonl
docker compose run --rm mc cp /data/merchants.json local/lake/bronze/referentiel/merchants.json
docker compose run --rm mc ls --recursive local/lake/bronze/
