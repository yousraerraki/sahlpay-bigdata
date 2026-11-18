#!/usr/bin/env bash
# Partie 5 : pipeline Silver/Gold. Ctrl+C pour un arret propre (ou RUN_SECONDS=300).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec jupyter spark-submit jobs/part5_pipeline.py --run-seconds "${RUN_SECONDS:-0}" 2>&1 \
  | tee -a logs/part5_pipeline_console.log
