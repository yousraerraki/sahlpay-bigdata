#!/usr/bin/env bash
# Preuves pour le rapport : arborescence du lac (Bronze / Silver / Gold) et historique Delta.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p rapport/captures
docker compose run --rm -T mc ls --recursive local/lake/ | tee rapport/captures/mc_ls_recursive_lake.txt | \
  awk '{print $NF}' | cut -d/ -f1-2 | sort | uniq -c
echo "Liste complete : rapport/captures/mc_ls_recursive_lake.txt"
