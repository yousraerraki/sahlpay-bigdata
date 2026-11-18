#!/usr/bin/env bash
# Partie 4 : Structured Streaming depuis Kafka.  Usage : ./scripts/03_part4_streaming.sh [raw|counts|watermark|all]
set -euo pipefail
cd "$(dirname "$0")/.."
Q="${1:-all}"
docker compose exec jupyter spark-submit jobs/part4_streaming.py --query "$Q" 2>&1 | tee "logs/part4_${Q}_console.log"
