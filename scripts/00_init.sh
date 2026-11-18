#!/usr/bin/env bash
# Partie 1 : construction de l'image, demarrage du cluster et verifications.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose build
docker compose up -d
echo "Attente de la creation du topic et du bucket..."
docker compose logs -f kafka-init minio-init &
LOGS_PID=$!
docker compose wait kafka-init minio-init >/dev/null 2>&1 || sleep 20
kill $LOGS_PID 2>/dev/null || true
echo; echo "=== Topic Kafka ==="
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic transactions
echo; echo "=== Bucket MinIO ==="
docker compose run --rm mc ls local/
echo; echo "=== Spark Master ==="
sleep 5
docker compose exec jupyter curl -s http://spark-master:8080/json/ | python3 -c \
  "import json,sys; s=json.load(sys.stdin); print('workers ALIVE:', sum(w['state']=='ALIVE' for w in s['workers']), '| cores:', s['cores'])"
echo
echo "Spark Master : http://localhost:8080   MinIO : http://localhost:9001 (minioadmin / minioadmin123)"
echo "Jupyter      : http://localhost:8888"
