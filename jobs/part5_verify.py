# -*- coding: utf-8 -*-
"""
Partie 5 - Verification : reprise apres arret sans perte ni doublon + lecture de la table Delta Gold.

  docker compose exec jupyter spark-submit jobs/part5_verify.py --label avant_arret
  ... arret du pipeline, 2 minutes d'attente (le producteur continue), redemarrage, rattrapage ...
  docker compose exec jupyter spark-submit jobs/part5_verify.py --label apres_redemarrage

Ce que fait le script :
 1. lit le dernier batch COMMITE du checkpoint p5_silver (dossiers commits/ et offsets/) ;
 2. relit en batch le topic Kafka exactement jusqu'a ces offsets (endingOffsets) et calcule le
    nombre attendu d'evenements uniques (tx_id, status) ;
 3. compare avec la couche Silver : nombre de lignes, cles distinctes, doublons, manquants ;
    les manquants sont expliques (evenements arrives apres le watermark de 10 minutes) ;
 4. affiche la table Delta Gold et son historique (DESCRIBE HISTORY).
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import functions as F

from sahlpay_common import KAFKA_BOOTSTRAP, LOG_DIR, TOPIC, Paths, get_spark
from streaming_utils import parse_transactions


def last_committed_offsets(spark, checkpoint):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    commits = jvm.org.apache.hadoop.fs.Path(f"{checkpoint}/commits")
    fs = commits.getFileSystem(conf)
    ids = [int(s.getPath().getName()) for s in fs.listStatus(commits) if s.getPath().getName().isdigit()]
    batch_id = max(ids)
    lines = spark.sparkContext.textFile(f"{checkpoint}/offsets/{batch_id}").collect()
    offsets = json.loads(lines[-1])
    return batch_id, lines, offsets


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="controle")
    args = ap.parse_args()
    spark = get_spark("SahlPay-P5-Verify")

    ck = f"{Paths.checkpoints}/p5_silver"
    batch_id, lines, end_offsets = last_committed_offsets(spark, ck)
    print(f"\n=== Checkpoint {ck}/offsets/{batch_id} (dernier batch commite) ===")
    for l in lines:
        print("   ", l)
    total_offsets = sum(int(v) for p in end_offsets.values() for v in p.values())

    # Relecture batch de Kafka jusqu'aux offsets du checkpoint
    kafka = (spark.read.format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", TOPIC)
             .option("startingOffsets", "earliest")
             .option("endingOffsets", json.dumps(end_offsets))
             .load())
    k = parse_transactions(kafka).filter(F.col("tx_id").isNotNull()).cache()
    n_kafka = k.count()
    expected = k.dropDuplicates(["tx_id", "status"])
    n_expected = expected.count()

    silver = spark.read.parquet(Paths.silver_clean).cache()
    n_silver = silver.count()
    n_silver_keys = silver.select("tx_id", "status").distinct().count()
    dup_keys = (silver.groupBy("tx_id", "status").count().filter("count > 1").count())

    missing = expected.join(silver.select("tx_id", "status"), ["tx_id", "status"], "left_anti") \
                      .withColumn("retard_min", (F.col("kafka_ts").cast("long") - F.col("event_time").cast("long")) / 60)
    n_missing = missing.count()
    n_missing_late = missing.filter("retard_min > 10").count()
    extra = silver.join(expected.select("tx_id", "status"), ["tx_id", "status"], "left_anti").count()

    print("\n=== Controle exactly-once (Kafka -> Silver) ===")
    print(f"messages Kafka lus jusqu'aux offsets du checkpoint : {n_kafka} (somme des offsets = {total_offsets})")
    print(f"evenements uniques attendus (tx_id, status)         : {n_expected}")
    print(f"lignes Silver                                        : {n_silver}")
    print(f"cles (tx_id, status) distinctes dans Silver          : {n_silver_keys}")
    print(f"cles en double dans Silver                           : {dup_keys}   <- doit etre 0")
    print(f"lignes Silver absentes de Kafka (en trop)            : {extra}   <- doit etre 0")
    print(f"evenements attendus absents de Silver                : {n_missing}")
    print(f"   dont arrives avec > 10 min de retard (watermark)  : {n_missing_late}")
    if n_missing:
        missing.select("tx_id", "status", "event_time", "kafka_ts", F.round("retard_min", 1).alias("retard_min")) \
               .orderBy("kafka_ts").show(10, truncate=False)

    os.makedirs(LOG_DIR, exist_ok=True)
    csv_path = os.path.join(LOG_DIR, "part5_counts.csv")
    new = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["horodatage", "label", "batch_commite", "somme_offsets", "kafka_messages",
                        "attendus_uniques", "silver_lignes", "silver_cles_distinctes", "doublons",
                        "manquants", "manquants_retard_watermark"])
        w.writerow([datetime.now(timezone.utc).isoformat(timespec="seconds"), args.label, batch_id, total_offsets,
                    n_kafka, n_expected, n_silver, n_silver_keys, dup_keys, n_missing, n_missing_late])
    print(f"\nComptage ajoute a {csv_path}")

    # Table Delta Gold
    print("\n=== Gold : lake/gold/kpi_city_10min (Delta) ===")
    gold = spark.read.format("delta").load(Paths.gold_kpi)
    gold.orderBy(F.desc("window_start"), F.desc("montant_total_mad")).show(15, truncate=False)
    print("lignes Gold :", gold.count())
    print("\n=== DESCRIBE HISTORY ===")
    spark.sql(f"DESCRIBE HISTORY delta.`{Paths.gold_kpi}`") \
         .select("version", "timestamp", "operation", "operationParameters", "operationMetrics") \
         .show(15, truncate=60)

    for name, path in (("reject_rate_city_sliding", Paths.gold_reject), ("device_sessions", Paths.gold_sessions)):
        try:
            df = spark.read.format("delta").load(path)
            print(f"\n=== Gold : {name} ({df.count()} lignes) ===")
            order = "taux_rejet" if "taux_rejet" in df.columns else "nb_tx"
            df.orderBy(F.desc(order)).show(8, truncate=False)
        except Exception as e:
            print(f"{name} : pas encore de donnees ({type(e).__name__})")
    spark.stop()


if __name__ == "__main__":
    main()
