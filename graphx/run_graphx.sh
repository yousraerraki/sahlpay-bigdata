#!/usr/bin/env bash
# Lance le traitement GraphX (Scala) avec spark-shell sur le cluster standalone.
# A executer dans le conteneur jupyter :
#   docker compose exec jupyter bash graphx/run_graphx.sh
# Variables : SPARK_MASTER (defaut spark://spark-master:7077), LAKE (defaut s3a://lake)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
SCRIPT=/tmp/sahlpay_graphx_run.scala
cat "$HERE/src/main/scala/SahlPayGraphX.scala" > "$SCRIPT"
printf '\nSahlPayGraphX.main(Array())\nSystem.exit(0)\n' >> "$SCRIPT"

EXTRA=()
if [[ "$MASTER" == spark://* ]]; then
  EXTRA+=(--conf "spark.driver.host=${SPARK_LOCAL_HOSTNAME:-jupyter}"
          --conf spark.driver.bindAddress=0.0.0.0
          --conf spark.cores.max="${SPARK_CORES_MAX:-2}"
          --conf spark.executor.cores=1
          --conf spark.executor.memory=1g)
fi
echo "[run_graphx] master=$MASTER LAKE=${LAKE:-s3a://lake}"
time spark-shell --master "$MASTER" --name SahlPay-P6-GraphX "${EXTRA[@]}" -i "$SCRIPT" 2>&1 \
  | grep -v -E "^(scala>|     \|)" | tee "${LOG_DIR:-$HERE/../logs}/graphx_run.log"
