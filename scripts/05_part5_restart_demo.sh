#!/usr/bin/env bash
# Partie 5.6 : demonstration de reprise apres panne (le producteur doit tourner dans un autre terminal).
#   1) pipeline 3 min -> arret propre       2) controle "avant_arret" + offsets + consumer groups
#   3) 2 minutes d'attente (le producteur continue, le lag augmente)
#   4) redemarrage 3 min (rattrapage depuis le checkpoint) -> arret   5) controle "apres_redemarrage"
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=logs/part5_restart_demo.log
: > "$OUT"
KG="docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092"
step() { echo -e "\n######## $(date -u +%H:%M:%S) $*" | tee -a "$OUT"; }

show_offsets() {
  step "Checkpoint p5_silver : dernier fichier offsets/ et commits/"
  LAST=$(docker compose run --rm -T mc ls local/lake/checkpoints/p5_silver/offsets/ | awk '{print $NF}' | grep -E '^[0-9]+$' | sort -n | tail -1)
  echo "dernier batch : $LAST" | tee -a "$OUT"
  docker compose run --rm -T mc cat "local/lake/checkpoints/p5_silver/offsets/$LAST" | tee -a "$OUT"
  echo | tee -a "$OUT"
  step "kafka-consumer-groups.sh --list"
  $KG --list | tee -a "$OUT"
  step "kafka-consumer-groups.sh --describe --group sahlpay-pipeline-monitor"
  $KG --describe --group sahlpay-pipeline-monitor 2>&1 | tee -a "$OUT" || true
}

step "1) Demarrage du pipeline pour ${FIRST_RUN:-180} s"
docker compose exec -T jupyter spark-submit jobs/part5_pipeline.py --run-seconds "${FIRST_RUN:-180}" >> logs/part5_pipeline_console.log 2>&1
step "2) Controle avant arret"
docker compose exec -T jupyter spark-submit jobs/part5_verify.py --label avant_arret 2>&1 | grep -v WARN | tee -a "$OUT"
show_offsets
step "3) Pipeline arrete, le producteur continue pendant 120 s"
sleep 60; $KG --describe --group sahlpay-pipeline-monitor 2>&1 | tee -a "$OUT" || true; sleep 60
step "4) Redemarrage : reprise au bon offset grace au checkpoint (${SECOND_RUN:-180} s)"
docker compose exec -T jupyter spark-submit jobs/part5_pipeline.py --run-seconds "${SECOND_RUN:-180}" >> logs/part5_pipeline_console.log 2>&1
step "5) Controle apres redemarrage"
docker compose exec -T jupyter spark-submit jobs/part5_verify.py --label apres_redemarrage 2>&1 | grep -v WARN | tee -a "$OUT"
show_offsets
step "Comptages avant / apres (logs/part5_counts.csv)"
column -s, -t < logs/part5_counts.csv | tee -a "$OUT"
