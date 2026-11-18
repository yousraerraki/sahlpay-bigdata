#!/usr/bin/env bash
# Partie 6 : lance seulement le traitement GraphX (le notebook 06 l'appelle aussi automatiquement).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec jupyter bash graphx/run_graphx.sh
