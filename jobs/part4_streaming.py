# -*- coding: utf-8 -*-
"""
Partie 4 - Structured Streaming depuis Kafka.

Lancement (producteur dans un autre terminal) :
  docker compose exec jupyter python3 producer/producer.py --mode kafka --rate 20 --late-demo-every 60 --log logs/producer.log
  docker compose exec jupyter spark-submit jobs/part4_streaming.py --query all

--query raw       : 4.3 flux brut type -> console (append)  + 4.4 Bronze JSON en parallele
--query counts    : 4.5 nombre de transactions par ville et statut -> console (complete)
--query watermark : 4.6 agregation fenetree avec watermark de 10 min + sonde des evenements en retard
--query all       : toutes les requetes ci-dessus dans la meme application
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import functions as F

from sahlpay_common import Paths, get_spark
from streaming_utils import ProgressLogger, await_all, parse_transactions, read_kafka


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", choices=["raw", "counts", "watermark", "all"], default="all")
    ap.add_argument("--trigger", default="10 seconds", help="intervalle des micro-batches")
    ap.add_argument("--starting-offsets", default="latest")
    ap.add_argument("--run-seconds", type=int, default=0)
    args = ap.parse_args()

    spark = get_spark("SahlPay-P4-StructuredStreaming")
    spark.streams.addListener(ProgressLogger("part4"))

    # 4.1 Source Kafka : chaque nouvel enregistrement du topic est une nouvelle ligne d'une table infinie
    kafka_df = read_kafka(spark, starting_offsets=args.starting_offsets, group_prefix="sahlpay-p4")
    # 4.2 value (binaire) -> colonnes typees avec from_json + schema explicite de la Partie 3
    tx = parse_transactions(kafka_df)
    queries = []

    if args.query in ("raw", "all"):
        # 4.3 Flux brut type -> sink console (append : seules les nouvelles lignes sont emises)
        queries.append(tx.select("tx_id", "type", "city", "status", "amount_mad", "event_time",
                                 "kafka_partition", "kafka_offset")
                       .writeStream.queryName("p4_console_raw")
                       .format("console").outputMode("append")
                       .option("truncate", "false").option("numRows", 15)
                       .trigger(processingTime=args.trigger)
                       .start())
        # 4.4 En parallele : flux brut JSON tel que lu dans Kafka -> couche Bronze (checkpoint dedie)
        bronze = kafka_df.select(F.col("key").cast("string").alias("key"),
                                 F.col("value").cast("string").alias("value"),
                                 "topic", "partition", "offset", "timestamp",
                                 F.to_date("timestamp").alias("ingest_date"))
        queries.append(bronze.writeStream.queryName("p4_bronze_raw")
                       .format("json").outputMode("append")
                       .option("path", Paths.bronze_raw)
                       .option("checkpointLocation", f"{Paths.checkpoints}/p4_bronze_raw")
                       .partitionBy("ingest_date")
                       .trigger(processingTime=args.trigger)
                       .start())

    if args.query in ("counts", "all"):
        # 4.5 Agregation sans fenetre : la table resultat complete est reemise a chaque micro-batch
        counts = tx.groupBy("city", "status").count().orderBy("city", "status")
        queries.append(counts.writeStream.queryName("p4_counts_city_status")
                       .format("console").outputMode("complete")
                       .option("truncate", "false").option("numRows", 60)
                       .trigger(processingTime=args.trigger)
                       .start())

    if args.query in ("watermark", "all"):
        # 4.6 Watermark de 10 minutes : watermark = max(event_time vu) - 10 min.
        # Un evenement dont event_time < watermark est ignore par l'agregation fenetree
        # (compte dans numRowsDroppedByWatermark du listener).
        windowed = (tx.withWatermark("event_time", "10 minutes")
                    .groupBy(F.window("event_time", "5 minutes"), "city")
                    .agg(F.count("*").alias("nb_evenements"),
                         F.sum(F.when(F.col("tx_id").startswith("TX-LATE"), 1).otherwise(0)).alias("nb_demo_retard"))
                    .select(F.col("window.start").alias("fenetre_debut"), F.col("window.end").alias("fenetre_fin"),
                            "city", "nb_evenements", "nb_demo_retard"))
        queries.append(windowed.writeStream.queryName("p4_watermark_window")
                       .format("console").outputMode("update")
                       .option("truncate", "false").option("numRows", 40)
                       .trigger(processingTime=args.trigger)
                       .start())
        # Sonde sans etat : montre chaque evenement arrive avec plus de 60 s de retard
        # (heure d'arrivee Kafka - event_time), pour tracer son sort dans les logs.
        late_probe = (tx.withColumn("retard_s", F.col("kafka_ts").cast("long") - F.col("event_time").cast("long"))
                      .filter((F.col("retard_s") > 60) | F.col("tx_id").startswith("TX-LATE"))
                      .select("tx_id", "city", "status", "event_time", "kafka_ts",
                              F.round(F.col("retard_s") / 60, 1).alias("retard_min")))
        queries.append(late_probe.writeStream.queryName("p4_late_probe")
                       .format("console").outputMode("append")
                       .option("truncate", "false").option("numRows", 20)
                       .trigger(processingTime=args.trigger)
                       .start())

    print("[main] requetes actives :", [q.name for q in queries], flush=True)
    await_all(spark, queries, args.run_seconds)
    spark.stop()


if __name__ == "__main__":
    main()
