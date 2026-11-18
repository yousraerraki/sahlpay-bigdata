# -*- coding: utf-8 -*-
"""
Partie 5 - Pipeline de production Spark-Kafka (Bronze -> Silver -> Gold).

  docker compose exec jupyter python3 producer/producer.py --mode kafka --rate 20 --log logs/producer.log
  docker compose exec jupyter spark-submit jobs/part5_pipeline.py

Requetes demarrees (une application, un checkpoint par requete) :
  p5_silver        : nettoyage + dedoublonnage (tx_id, status) + watermark + jointure stream-static
                     merchants + amount_band  -> Parquet lake/silver/transactions_clean (city, date)
  p5_gold_kpi      : fenetre tumbling 10 min par ville (nb tx, montant)    -> Delta lake/gold/kpi_city_10min
  p5_reject_rate   : fenetre glissante 10 min / 5 min, taux de rejet       -> Delta lake/gold/reject_rate_city_sliding
  p5_device_session: session_window 5 min d'inactivite par device_id       -> Delta lake/gold/device_sessions

Exactly-once :
  * source rejouable : Kafka conserve les messages, Spark relit exactement la plage d'offsets d'un batch ;
  * checkpoint (WAL offsets/ + commits/ + state/) : la plage d'offsets de chaque batch est ecrite AVANT
    traitement et le batch n'est marque "commit" qu'APRES ecriture du sink ;
  * sinks idempotents / transactionnels : le sink fichier Parquet ecrit un journal _spark_metadata
    (un batch rejoue ne produit pas de doublon visible) ; les sinks Delta utilisent MERGE sur une cle
    (window_start, city) ou une ecriture transactionnelle (txnAppId, txnVersion=batchId).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import functions as F

from sahlpay_common import MERCHANT_SCHEMA, Paths, get_spark, merchants_path
from streaming_utils import ProgressLogger, amount_band_col, await_all, parse_transactions, read_kafka

CHECKPOINT_GROUP = "sahlpay-pipeline-monitor"   # groupe Kafka ou l'on recopie les offsets du checkpoint


def merge_into_delta(spark, path, keys, query_name):
    """foreachBatch : MERGE (upsert) idempotent dans une table Delta, cle = `keys`."""
    from delta.tables import DeltaTable

    def _fn(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df = batch_df.withColumn("updated_batch_id", F.lit(batch_id)) \
                           .withColumn("updated_at", F.current_timestamp())
        if not DeltaTable.isDeltaTable(spark, path):
            batch_df.write.format("delta").mode("overwrite").save(path)
            return
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (DeltaTable.forPath(spark, path).alias("t")
         .merge(batch_df.alias("s"), cond)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    return _fn


def append_into_delta(path, app_id):
    """foreachBatch : append transactionnel idempotent (txnAppId + txnVersion = batch_id)."""
    def _fn(batch_df, batch_id):
        (batch_df.withColumn("batch_id", F.lit(batch_id))
         .write.format("delta").mode("append")
         .option("txnAppId", app_id).option("txnVersion", batch_id)
         .save(path))
        batch_df.orderBy(F.desc("nb_tx")).show(10, truncate=False)
    return _fn


def build_pipeline(spark, tx):
    """Transformations du pipeline (independantes des sinks)."""
    # 1. Nettoyage + dedoublonnage (tx_id, status) combine au watermark de 10 minutes
    clean = (tx.filter(F.col("tx_id").isNotNull() & F.col("event_time").isNotNull()
                       & F.col("status").isin("INITIATED", "AUTHORIZED", "COMPLETED", "REJECTED", "REVERSED")
                       & (F.col("amount_mad") > 0))
             .withWatermark("event_time", "10 minutes")
             .dropDuplicates(["tx_id", "status"]))

    # 2. Enrichissement : jointure stream-static avec le referentiel merchants + amount_band
    merchants = (spark.read.schema(MERCHANT_SCHEMA).json(merchants_path())
                 .select(F.col("merchant_id").alias("payee_id"),
                         F.col("name").alias("merchant_name"),
                         F.col("category").alias("merchant_category"),
                         F.col("latitude").alias("merchant_lat"),
                         F.col("longitude").alias("merchant_lon")))
    enriched = (clean.join(F.broadcast(merchants), on="payee_id", how="left")
                .withColumn("amount_band", amount_band_col(F.col("amount_mad")))
                .withColumn("date", F.to_date("event_time"))
                .withColumn("processed_at", F.current_timestamp()))

    # 4a. Fenetre tumbling 10 min par ville : nombre de transactions et montant total -> Gold Delta (MERGE)
    is_completed = F.col("status") == "COMPLETED"
    kpi = (enriched.groupBy(F.window("event_time", "10 minutes"), "city")
           .agg(F.sum(F.when(F.col("status") == "INITIATED", 1).otherwise(0)).alias("nb_transactions"),
                F.sum(F.when(is_completed, 1).otherwise(0)).alias("nb_completed"),
                F.sum(F.when(F.col("status") == "REJECTED", 1).otherwise(0)).alias("nb_rejected"),
                F.round(F.sum(F.when(is_completed, F.col("amount_mad")).otherwise(0.0)), 2).alias("montant_total_mad"),
                F.count("*").alias("nb_evenements"))
           .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                   "city", "nb_transactions", "nb_completed", "nb_rejected", "montant_total_mad", "nb_evenements"))
    # 4b. Fenetre glissante 10 min, pas de 5 min : taux de rejet = REJECTED / (COMPLETED + REJECTED)
    reject = (enriched.filter(F.col("status").isin("COMPLETED", "REJECTED"))
              .groupBy(F.window("event_time", "10 minutes", "5 minutes"), "city")
              .agg(F.sum(F.when(F.col("status") == "REJECTED", 1).otherwise(0)).alias("nb_rejected"),
                   F.count("*").alias("nb_decisions"))
              .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                      "city", "nb_rejected", "nb_decisions",
                      F.round(F.col("nb_rejected") / F.col("nb_decisions"), 4).alias("taux_rejet")))
    # 4c. Fenetre de session par appareil (inactivite 5 min) : duree et nombre de transactions.
    #     Les fenetres de session n'acceptent pas le mode update -> append (emise a la fermeture).
    sessions = (enriched.groupBy(F.session_window("event_time", "5 minutes"), "device_id")
                .agg(F.sum(F.when(F.col("status") == "INITIATED", 1).otherwise(0)).alias("nb_tx"),
                     F.count("*").alias("nb_evenements"),
                     F.min("event_time").alias("premier_evenement"),
                     F.max("event_time").alias("dernier_evenement"),
                     F.collect_set("payer_wallet").alias("wallets"))
                .select(F.col("session_window.start").alias("session_start"),
                        F.col("session_window.end").alias("session_end"),
                        "device_id", "nb_tx", "nb_evenements",
                        (F.col("dernier_evenement").cast("long") - F.col("premier_evenement").cast("long"))
                        .alias("duree_activite_s"),
                        (F.col("session_window.end").cast("long") - F.col("session_window.start").cast("long"))
                        .alias("duree_session_s"),
                        F.size("wallets").alias("nb_wallets")))
    return enriched, kpi, reject, sessions


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", default="15 seconds")
    ap.add_argument("--run-seconds", type=int, default=0, help="arret automatique apres N secondes (0 = Ctrl+C)")
    ap.add_argument("--starting-offsets", default="earliest",
                    help="utilise uniquement au tout premier lancement ; ensuite le checkpoint fait foi")
    ap.add_argument("--max-offsets", type=int, default=5000, help="maxOffsetsPerTrigger (rattrapage apres arret)")
    args = ap.parse_args()

    spark = get_spark("SahlPay-P5-Pipeline")
    spark.streams.addListener(ProgressLogger("part5", commit_group=CHECKPOINT_GROUP, commit_query="p5_silver"))

    kafka_df = read_kafka(spark, starting_offsets=args.starting_offsets, group_prefix="sahlpay-pipeline",
                          max_offsets=args.max_offsets)
    tx = parse_transactions(kafka_df)

    enriched, kpi, reject, sessions = build_pipeline(spark, tx)

    queries = []
    # 3. Sink Silver : Parquet partitionne par city et date
    queries.append(enriched.writeStream.queryName("p5_silver")
                   .format("parquet").outputMode("append")
                   .option("path", Paths.silver_clean)
                   .option("checkpointLocation", f"{Paths.checkpoints}/p5_silver")
                   .partitionBy("city", "date")
                   .trigger(processingTime=args.trigger)
                   .start())

    # 4a. Fenetre tumbling 10 min par ville -> Gold Delta (MERGE sur window_start, city)
    queries.append(kpi.writeStream.queryName("p5_gold_kpi")
                   .outputMode("update")
                   .foreachBatch(merge_into_delta(spark, Paths.gold_kpi, ["window_start", "city"], "p5_gold_kpi"))
                   .option("checkpointLocation", f"{Paths.checkpoints}/p5_gold_kpi")
                   .trigger(processingTime=args.trigger)
                   .start())

    # 4b. Fenetre glissante 10 min / 5 min : taux de rejet -> Gold Delta (MERGE)
    queries.append(reject.writeStream.queryName("p5_reject_rate")
                   .outputMode("update")
                   .foreachBatch(merge_into_delta(spark, Paths.gold_reject, ["window_start", "city"], "p5_reject_rate"))
                   .option("checkpointLocation", f"{Paths.checkpoints}/p5_reject_rate")
                   .trigger(processingTime=args.trigger)
                   .start())

    # 4c. Sessions par appareil (append, ecriture Delta transactionnelle)
    queries.append(sessions.writeStream.queryName("p5_device_session")
                   .outputMode("append")
                   .foreachBatch(append_into_delta(Paths.gold_sessions, "p5_device_session"))
                   .option("checkpointLocation", f"{Paths.checkpoints}/p5_device_session")
                   .trigger(processingTime=args.trigger)
                   .start())

    print("[main] requetes actives :", [q.name for q in queries], flush=True)
    await_all(spark, queries, args.run_seconds)
    spark.stop()


if __name__ == "__main__":
    main()
